"""
🔥 Bulletproof Message Processor (v3.2)
--------------------------------------
Responsible for:
 - Sending outbound SMS via TextGrid
 - Logging to Conversations table (guaranteed)
 - Updating Leads table activity
 - Handling retries and delivery status

Upgrades in v3.2:
 - Automatic failsafe logging to Airtable
 - Schema-safe + fallback via datastore
 - KPI + Run telemetry (best-effort)
 - Uniform return envelope
"""

from __future__ import annotations

import json
import os
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional

# Airtable (optional at runtime)
try:
    from pyairtable import Table
except Exception:
    Table = None  # guarded below

# Transport + retry
from sms.textgrid_sender import send_message
from sms.retry_handler import handle_retry
from sms.datastore import safe_log_message

# Schema maps
from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS, LEAD_FIELDS
from sms.airtable_schema import ConversationDirection, ConversationDeliveryStatus

# Central logger
from sms.runtime import get_logger

# ✅ Add failsafe datastore imports
from sms.datastore import safe_create_conversation, safe_log_message

logger = get_logger("message_processor")

# Best-effort telemetry imports (won't crash if missing)
try:
    from sms.kpi_logger import log_kpi
except Exception:
    def log_kpi(*_a, **_k):  # type: ignore
        return {"ok": False, "action": "skipped", "error": "kpi_logger unavailable"}

try:
    from sms.logger import log_run
except Exception:
    def log_run(*_a, **_k):  # type: ignore
        pass


# ---------------------------
# Field mappings (canonical)
# ---------------------------
FROM_FIELD = CONV_FIELDS["FROM"]
TO_FIELD = CONV_FIELDS["TO"]
MSG_FIELD = CONV_FIELDS["BODY"]
STATUS_FIELD = CONV_FIELDS["STATUS"]
DIR_FIELD = CONV_FIELDS["DIRECTION"]
SENT_AT_FIELD = CONV_FIELDS["SENT_AT"]
TEXTGRID_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]

CAMPAIGN_LINK_FIELD = CONVERSATIONS_FIELDS.get("CAMPAIGN_LINK", "Campaign")
TEMPLATE_LINK_FIELD = CONVERSATIONS_FIELDS.get("TEMPLATE_LINK", "Template")
DRIP_QUEUE_LINK_FIELD = CONVERSATIONS_FIELDS.get("DRIP_QUEUE_LINK", "Drip Queue")

LEAD_STATUS_FIELD = LEAD_FIELDS["STATUS"]
LEAD_LAST_ACTIVITY_FIELD = LEAD_FIELDS["LAST_ACTIVITY"]
LEAD_LAST_MESSAGE_FIELD = LEAD_FIELDS["LAST_MESSAGE"]
LEAD_LAST_OUTBOUND_FIELD = LEAD_FIELDS["LAST_OUTBOUND"]
LEAD_LAST_INBOUND_FIELD = LEAD_FIELDS["LAST_INBOUND"]
LEAD_LAST_DIRECTION_FIELD = LEAD_FIELDS["LAST_DIRECTION"]
LEAD_LAST_DELIVERY_STATUS_FIELD = LEAD_FIELDS["LAST_DELIVERY_STATUS"]
LEAD_DELIVERED_COUNT_FIELD = LEAD_FIELDS["DELIVERED_COUNT"]
LEAD_FAILED_COUNT_FIELD = LEAD_FIELDS["FAILED_COUNT"]
LEAD_SENT_COUNT_FIELD = LEAD_FIELDS["SENT_COUNT"]
LEAD_PROPERTY_ID_FIELD = LEAD_FIELDS["PROPERTY_ID"]


# ---------------------------
# Env config
# ---------------------------
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")


# ---------------------------
# Utility helpers
# ---------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower()) if s else ""


def _auto_field_map(tbl: Any) -> Dict[str, str]:
    # Get comprehensive field mapping from schema instead of just sampling existing records
    from sms.config import CONV_FIELDS
    
    # Start with schema-based field mappings  
    schema_fields = dict(CONV_FIELDS)
    
    # Add common linking fields that might be present
    common_fields = {
        "Campaign": "Campaign",
        "Template": "Template", 
        "Prospect": "Prospect",
        "Lead": "Lead",
        "Lead Record ID": "Lead Record ID",
        "Prospect Record ID": "Prospect Record ID",
        "County": "County",
        # Remove Drip Queue as it doesn't exist in conversations table
    }
    
    # Try to get existing fields from sample record as fallback
    try:
        probe = tbl.all(max_records=1)
        existing_keys = list(probe[0].get("fields", {}).keys()) if probe else []
        for key in existing_keys:
            common_fields[key] = key
    except Exception:
        pass
    
    # Combine schema fields with common/existing fields
    all_fields = {**schema_fields, **common_fields}
    
    # Create normalized mapping
    return {_norm(k): k for k in all_fields.values()}


def _remap_existing_only(tbl: Any, payload: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            out[mk] = v
    return out


def _compact(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (d or {}).items() if v not in (None, "", [], {}, ())}


# ---------------------------
# Lazy Airtable connections
# ---------------------------
@lru_cache(maxsize=None)
def _tbl(table_name: str) -> Optional[Any]:
    if not (AIRTABLE_KEY and LEADS_CONVOS_BASE and Table):
        return None
    try:
        return Table(AIRTABLE_KEY, LEADS_CONVOS_BASE, table_name)
    except Exception as e:
        logger.warning(f"⚠️ Airtable init failed for {table_name}: {e}")
        return None


def get_convos():
    return _tbl(CONVERSATIONS_TABLE)


def get_leads():
    return _tbl(LEADS_TABLE)


# ---------------------------
# Core Processor
# ---------------------------
class MessageProcessor:
    @staticmethod
    def send(
        phone: str,
        body: str,
        *,
        from_number: str | None = None,
        campaign_id: str | None = None,
        template_id: str | None = None,
        drip_queue_id: str | None = None,
        lead_id: str | None = None,
        property_id: str | None = None,
        direction: str = ConversationDirection.OUTBOUND.value,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Sends SMS → logs Conversations → updates Leads."""
        if not phone or not body:
            logger.warning("Skipping send: missing phone or body")
            return {
                "ok": False, "status": "skipped", "sid": None,
                "phone": phone, "body": body, "convo_id": None,
                "provider_status": None, "error": "missing phone or body",
                "timestamp": utcnow_iso(), "property_id": property_id,
            }

        if not from_number:
            err = "missing_from_number"
            logger.error(f"Transport error sending to {phone}: {err}")
            return {
                "ok": False, "status": "failed", "sid": None,
                "phone": phone, "body": body, "convo_id": None,
                "provider_status": None, "error": err,
                "timestamp": utcnow_iso(), "property_id": property_id,
            }

        convos = get_convos()
        leads = get_leads()
        meta = dict(metadata or {})

        # --- 1) Send via TextGrid
        try:
            send_result = send_message(from_number=from_number, to=phone, message=body)
        except Exception as e:
            err = str(e)
            logger.error(f"Transport error sending to {phone}: {err}", exc_info=True)
            failure_meta = MessageProcessor._conversation_metadata(
                {**meta, "error": err},
                status=ConversationDeliveryStatus.FAILED.value,
                lead_id=lead_id,
                prospect_id=meta.get("prospect_id"),
            )
            convo_id = MessageProcessor._log_conversation(
                status="FAILED", phone=phone, body=body, from_number=from_number,
                direction=direction, sid=None, campaign_id=campaign_id,
                template_id=template_id, drip_queue_id=drip_queue_id,
                metadata={"error": err, **meta},
            )
            MessageProcessor._safe_retry(convo_id, err)
            log_run("OUTBOUND_ERROR", processed=0, breakdown={"phone": phone, "error": err})
            log_kpi("OUTBOUND_FAILED", 1)
            return {"ok": False, "status": "failed", "sid": None, "phone": phone,
                    "body": body, "convo_id": convo_id, "provider_status": None,
                    "error": err, "timestamp": utcnow_iso(), "property_id": property_id}

        # --- 2) Normalize provider response
        sid = send_result.get("sid") or send_result.get("message_sid") or send_result.get("id")
        provider_status = (send_result.get("status") or "sent").lower()
        ok = provider_status in {"sent", "queued", "accepted", "submitted", "enroute", "delivered"}

        # --- 3) Log Conversations
        convo_status = ConversationDeliveryStatus.SENT.value if ok else ConversationDeliveryStatus.FAILED.value
        convo_meta = MessageProcessor._conversation_metadata(
            {**meta, "provider_status": provider_status},
            status=convo_status,
            lead_id=lead_id,
            prospect_id=meta.get("prospect_id"),
        )
        convo_id = MessageProcessor._log_conversation(
            status=convo_status, phone=phone, body=body,
            from_number=from_number, direction=direction,
            sid=sid, campaign_id=campaign_id,
            template_id=template_id, drip_queue_id=drip_queue_id,
            metadata={"provider_status": provider_status, **meta},
        )

        # --- 4) Lead activity update
        if lead_id and leads:
            MessageProcessor._update_lead_activity(leads, lead_id, body, direction, property_id=property_id)

        # --- 5) Retry on failure
        if not ok:
            MessageProcessor._safe_retry(convo_id, f"provider_status={provider_status}")

        log_run("OUTBOUND_MESSAGE", processed=1, breakdown={"phone": phone, "sid": sid, "provider_status": provider_status, "ok": ok})
        log_kpi("OUTBOUND_SENT" if ok else "OUTBOUND_FAILED", 1)

        result = {
            "ok": ok, "status": "sent" if ok else "failed", "sid": sid,
            "phone": phone, "body": body, "convo_id": convo_id,
            "provider_status": provider_status,
            "error": None if ok else provider_status,
            "timestamp": utcnow_iso(), "property_id": property_id,
        }
        logger.info(f"📤 Outbound → {phone} | {result['status'].upper()} | sid={sid} | provider={provider_status}")
        return result

    @staticmethod
    def _conversation_metadata(
        meta: Dict[str, Any],
        *,
        status: Optional[str] = None,
        lead_id: Optional[str] = None,
        prospect_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        alias_map = {
            "status": CONV_FIELDS.get("STATUS", "status"),
            "stage": CONV_FIELDS.get("STAGE", "stage"),
            "aiintent": CONV_FIELDS.get("AI_INTENT", "ai_intent"),
            "ai_intent": CONV_FIELDS.get("AI_INTENT", "ai_intent"),
            "leadid": CONV_FIELDS.get("LEAD_RECORD_ID", "lead_id"),
            "leadrecordid": CONV_FIELDS.get("LEAD_RECORD_ID", "lead_id"),
            "lead": CONV_FIELDS.get("LEAD_RECORD_ID", "lead_id"),
            "prospectid": CONV_FIELDS.get("PROSPECT_RECORD_ID", "prospect_id"),
            "prospectrecordid": CONV_FIELDS.get("PROSPECT_RECORD_ID", "prospect_id"),
            "prospect": CONV_FIELDS.get("PROSPECT_RECORD_ID", "prospect_id"),
        }

        passthrough: Dict[str, Any] = {}
        normalized: Dict[str, Any] = {}
        for key, value in (meta or {}).items():
            norm_key = _norm(key)
            target = alias_map.get(norm_key)
            if target:
                normalized[target] = value
            else:
                passthrough[key] = value

        status_field = CONV_FIELDS.get("STATUS", "status")
        if status:
            normalized[status_field] = status
        if lead_id:
            normalized[CONV_FIELDS.get("LEAD_RECORD_ID", "lead_id")] = lead_id
        if prospect_id:
            normalized[CONV_FIELDS.get("PROSPECT_RECORD_ID", "prospect_id")] = prospect_id

        return _compact({**passthrough, **normalized})

    @staticmethod
    def _stringify_provider_error(error: Any) -> Optional[str]:
        if error in (None, "", [], {}, ()):  # type: ignore[arg-type]
            return None
        if isinstance(error, str):
            return error
        try:
            return json.dumps(error)
        except Exception:
            return str(error)

    # -----------------------------------------------------------
    @staticmethod
    def _log_conversation(
        *, status: str, phone: str, body: str, from_number: Optional[str],
        direction: str, sid: Optional[str], campaign_id: Optional[str],
        template_id: Optional[str], drip_queue_id: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Logs outbound message to Conversations (with failsafe)."""
        convos = get_convos()
        canonical_dir = (
            ConversationDirection.OUTBOUND.value
            if direction.upper().startswith("OUT")
            else ConversationDirection.INBOUND.value
            if direction.upper().startswith("IN")
            else direction
        )
        status_map = {
            "SENT": ConversationDeliveryStatus.SENT.value,
            "FAILED": ConversationDeliveryStatus.FAILED.value,
            "DELIVERED": ConversationDeliveryStatus.DELIVERED.value,
            "QUEUED": ConversationDeliveryStatus.QUEUED.value,
            "UNDELIVERED": ConversationDeliveryStatus.UNDELIVERED.value,
            "OPT OUT": ConversationDeliveryStatus.OPT_OUT.value,
        }
        canonical_status = status_map.get(status.upper(), status)
        
        # Get current timestamp for multiple time fields
        now_iso = utcnow_iso()

        payload = _compact({
            FROM_FIELD: phone,
            TO_FIELD: from_number,
            MSG_FIELD: body,
            DIR_FIELD: canonical_dir,
            STATUS_FIELD: canonical_status,
            SENT_AT_FIELD: now_iso,
            TEXTGRID_ID_FIELD: sid,
            CAMPAIGN_LINK_FIELD: [campaign_id] if campaign_id else None,
            TEMPLATE_LINK_FIELD: [template_id] if template_id else None,
            DRIP_QUEUE_LINK_FIELD: [drip_queue_id] if drip_queue_id else None,
            
            # Enhanced field mapping - delivery status only for outbound
            CONV_FIELDS.get("DELIVERY_STATUS", "Delivery Status"): canonical_status if canonical_dir == "OUTBOUND" else None,
            
            # Processing fields with proper logic
            CONV_FIELDS.get("RECEIVED_AT", "Received Time"): now_iso,
            CONV_FIELDS.get("PROCESSED_AT", "Processed Time"): now_iso,
            CONV_FIELDS.get("PROCESSED_BY", "Processed By"): "Campaign Runner" if canonical_dir == "OUTBOUND" else "Autoresponder",
            
            # Get total message counts for this prospect
            **MessageProcessor._get_enhanced_counts(phone, canonical_dir),
            
            # Stage management based on message content and direction
            CONV_FIELDS.get("STAGE", "Stage"): MessageProcessor._determine_stage(phone, canonical_dir, metadata),
            
            # Add metadata fields with proper field mapping
            **(metadata or {}),
        })
        
        # Add prospect linking if available in metadata
        meta = metadata or {}
        if meta.get("prospect_id"):
            payload["Prospect Record ID"] = meta["prospect_id"]
            payload["Prospect"] = [meta["prospect_id"]]
            
        # Add AI analysis fields if available in metadata using proper field mappings
        if meta.get("ai_intent"):
            payload[CONV_FIELDS.get("AI_INTENT", "AI Intent")] = meta["ai_intent"]
        if meta.get("intent_detected"):
            payload[CONV_FIELDS.get("INTENT", "Intent Detected")] = meta["intent_detected"]
        if meta.get("stage"):
            payload[CONV_FIELDS.get("STAGE", "Stage")] = meta["stage"]

        # Link to Lead record
        try:
            from sms.airtable_client import get_leads
            leads = get_leads()
            if leads:
                lead_matches = leads.all(
                    formula=f"{{Seller Phone Number}} = '{phone}'",
                    max_records=1,
                    fields=["Record ID"]
                )
                if lead_matches:
                    payload[CONV_FIELDS.get("LEAD", "Lead")] = [lead_matches[0]["id"]]
                    logger.info(f"Linked conversation to lead: {lead_matches[0]['id']}")
        except Exception as e:
            logger.warning(f"Error linking to lead: {e}")

        # Link to Drip Queue record if applicable
        if drip_queue_id:
            payload[CONV_FIELDS.get("DRIP_QUEUE", "Drip Queue")] = [drip_queue_id]

        # ✅ Conversation logging handled by main convos.create() below - no need for duplicate safe_log_message
        # safe_log_message("OUTBOUND", phone, from_number or "", body, status=canonical_status, sid=sid)

        if not convos:
            logger.info(f"[MOCK] Conversations ← {payload}")
            return "mock_convo"

        try:
            record = convos.create(_remap_existing_only(convos, payload))
            rid = record.get("id")
            logger.info(f"🗒️ Conversations[{rid}] {canonical_dir} → {phone} | {canonical_status}")
            return rid
        except Exception as e:
            logger.error(f"Failed to create Conversations row: {e}")
            # 🔥 Failsafe: Continue without conversation logging to avoid blocking SMS sends
            logger.warning(f"⚠️ SMS sent successfully but conversation logging failed - continuing")
            return None

    @staticmethod
    def _get_enhanced_counts(phone: str, direction: str) -> Dict[str, int]:
        """Get enhanced message counts for this prospect."""
        try:
            sent_count, reply_count = get_prospect_total_counts(phone)
            
            # Include the current message in counts
            if direction == "OUTBOUND":
                sent_count += 1
            else:
                reply_count += 1
                
            return {
                CONV_FIELDS.get("SENT_COUNT", "Sent Count"): 1 if direction == "OUTBOUND" else 0,
                CONV_FIELDS.get("REPLY_COUNT", "Reply Count"): 1 if direction == "INBOUND" else 0,
            }
        except Exception as e:
            logger.warning(f"Error getting enhanced counts for {phone}: {e}")
            return {
                CONV_FIELDS.get("SENT_COUNT", "Sent Count"): 1 if direction == "OUTBOUND" else 0,
                CONV_FIELDS.get("REPLY_COUNT", "Reply Count"): 1 if direction == "INBOUND" else 0,
            }

    @staticmethod
    def _determine_stage(phone: str, direction: str, metadata: Optional[Dict[str, Any]]) -> str:
        """Determine conversation stage based on message history and content."""
        try:
            # Check metadata first for explicit stage
            meta = metadata or {}
            if meta.get("stage"):
                return meta["stage"]
            
            # Get total counts to determine stage
            sent_count, reply_count = get_prospect_total_counts(phone)
            
            # Include current message in counts
            if direction == "OUTBOUND":
                sent_count += 1
            else:
                reply_count += 1
            
            # Stage logic based on conversation flow
            if reply_count == 0 and sent_count == 1:
                return "Stage 1 - Ownership Confirmation"
            elif reply_count > 0:
                # Check AI intent for more specific staging
                ai_intent = meta.get("ai_intent", "").lower()
                if "interested" in ai_intent or "qualified" in ai_intent:
                    return "Stage 3 - Price Qualification"
                elif "not interested" in ai_intent or "unqualified" in ai_intent:
                    return "Opt-Out"
                else:
                    return "Stage 2 - Interest Filter"
            elif sent_count > 3:
                return "Stage 2 - Interest Filter"
            else:
                return "Stage 1 - Ownership Confirmation"
                
        except Exception as e:
            logger.warning(f"Error determining stage for {phone}: {e}")
            return "Stage 1 - Ownership Confirmation"

    # -----------------------------------------------------------
    @staticmethod
    def _update_lead_activity(leads_tbl: Any, lead_id: str, body: str, direction: str, *, property_id: Optional[str] = None):
        now = utcnow_iso()
        canonical_dir = (
            ConversationDirection.OUTBOUND.value
            if direction.upper().startswith("OUT")
            else ConversationDirection.INBOUND.value
            if direction.upper().startswith("IN")
            else direction
        )
        patch = _compact({
            LEAD_LAST_ACTIVITY_FIELD: now,
            LEAD_LAST_MESSAGE_FIELD: body[:500] if body else "",
            LEAD_LAST_DIRECTION_FIELD: canonical_dir,
            LEAD_LAST_OUTBOUND_FIELD: now if canonical_dir == ConversationDirection.OUTBOUND.value else None,
            LEAD_LAST_INBOUND_FIELD: now if canonical_dir == ConversationDirection.INBOUND.value else None,
            LEAD_PROPERTY_ID_FIELD: property_id,
        })
        try:
            leads_tbl.update(lead_id, _remap_existing_only(leads_tbl, patch))
        except Exception as e:
            logger.warning(f"Lead update failed for {lead_id}: {e}", exc_info=True)

    # -----------------------------------------------------------
    @staticmethod
    def _safe_retry(convo_id: Optional[str], error: str):
        if not convo_id:
            return
        try:
            handle_retry(convo_id, error)
        except Exception as e:
            logger.warning(f"handle_retry failed for {convo_id}: {e}", exc_info=True)


def get_prospect_total_counts(phone: str) -> tuple[int, int]:
    """
    Get total sent and reply counts for a prospect across all conversations.
    
    Args:
        phone: The prospect's phone number
        
    Returns:
        tuple: (sent_count, reply_count)
    """
    try:
        from sms.airtable_client import get_convos
        convos = get_convos()
        if not convos:
            return (0, 0)
        
        # Get all messages for this prospect
        all_messages = convos.all(
            formula=f"{{Seller Phone Number}} = '{phone}'",
            fields=["Direction"]
        )
        
        sent_count = sum(1 for msg in all_messages 
                        if msg["fields"].get("Direction") == "OUTBOUND")
        reply_count = sum(1 for msg in all_messages 
                         if msg["fields"].get("Direction") == "INBOUND")
        
        return (sent_count, reply_count)
        
    except Exception as e:
        logger.warning(f"Error getting message counts for {phone}: {e}")
        return (0, 0)
