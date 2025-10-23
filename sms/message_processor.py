"""
🔥 Bulletproof Message Processor
--------------------------------
Responsible for:
 - Sending outbound SMS via TextGrid
 - Logging to Conversations table
 - Updating Leads table activity
 - Handling retries and delivery status

Now features:
 - Robust error trapping
 - Provider-agnostic response parsing
 - Schema-safe Airtable updates
 - Intelligent retry & self-healing
"""

from __future__ import annotations

import os
import re
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional

try:
    from pyairtable import Table
except ImportError:
    Table = None

from sms.retry_handler import handle_retry
from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS, LEAD_FIELDS
from sms.airtable_schema import ConversationDirection, ConversationDeliveryStatus

# ---------------------------
# Field mappings
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
# Airtable env config
# ---------------------------
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

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
    try:
        probe = tbl.all(max_records=1)
        keys = list(probe[0].get("fields", {}).keys()) if probe else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


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
        print(f"⚠️ Airtable init failed for {table_name}: {e}")
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
        """
        Sends SMS via provider → logs Conversations → updates Lead.

        Returns dict(status='sent'|'failed'|'skipped', sid, error, ...)
        """

        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        convos = get_convos()
        leads = get_leads()

        # --- 1️⃣ Send message via provider
        try:
            from sms.textgrid_sender import send_message

            send_result = send_message(phone, body, from_number=from_number)
        except Exception as e:
            err = str(e)
            print(f"❌ Transport error sending to {phone}: {err}")
            convo_id = MessageProcessor._log_conversation(
                status="FAILED",
                phone=phone,
                body=body,
                from_number=from_number,
                direction=direction,
                sid=None,
                campaign_id=campaign_id,
                template_id=template_id,
                drip_queue_id=drip_queue_id,
                metadata={"error": err, **(metadata or {})},
            )
            MessageProcessor._safe_retry(convo_id, err)
            return {"status": "failed", "phone": phone, "error": err, "convo_id": convo_id}

        # --- 2️⃣ Normalize provider response
        sid = send_result.get("sid") or send_result.get("message_sid") or send_result.get("id")
        provider_status = (send_result.get("status") or "sent").lower()
        ok = provider_status in {"sent", "queued", "accepted", "submitted", "enroute", "delivered"}

        # --- 3️⃣ Log conversation
        convo_status = ConversationDeliveryStatus.SENT.value if ok else ConversationDeliveryStatus.FAILED.value
        convo_id = MessageProcessor._log_conversation(
            status=convo_status,
            phone=phone,
            body=body,
            from_number=from_number,
            direction=direction,
            sid=sid,
            campaign_id=campaign_id,
            template_id=template_id,
            drip_queue_id=drip_queue_id,
            metadata={"provider_status": provider_status, **(metadata or {})},
        )

        # --- 4️⃣ Update lead activity
        if lead_id and leads:
            MessageProcessor._update_lead_activity(
                leads, lead_id, body, direction, property_id=property_id
            )

        # --- 5️⃣ Retry hook for failures
        if not ok:
            MessageProcessor._safe_retry(convo_id, f"provider_status={provider_status}")

        return {
            "status": "sent" if ok else "failed",
            "sid": sid,
            "phone": phone,
            "body": body,
            "convo_id": convo_id,
            "provider_status": provider_status,
            "property_id": property_id,
        }

    # -----------------------------------------------------------
    @staticmethod
    def _log_conversation(
        *,
        status: str,
        phone: str,
        body: str,
        from_number: Optional[str],
        direction: str,
        sid: Optional[str],
        campaign_id: Optional[str],
        template_id: Optional[str],
        drip_queue_id: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Logs the message to Conversations table (or mock if missing)."""

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

        payload = {
            FROM_FIELD: phone,
            TO_FIELD: from_number,
            MSG_FIELD: body,
            DIR_FIELD: canonical_dir,
            STATUS_FIELD: canonical_status,
            SENT_AT_FIELD: utcnow_iso(),
            TEXTGRID_ID_FIELD: sid,
            CAMPAIGN_LINK_FIELD: [campaign_id] if campaign_id else None,
            TEMPLATE_LINK_FIELD: [template_id] if template_id else None,
            DRIP_QUEUE_LINK_FIELD: [drip_queue_id] if drip_queue_id else None,
        }
        if metadata:
            payload.update(metadata)

        if not convos:
            print(f"[MOCK] Conversations ← {payload}")
            return "mock_convo"

        try:
            record = convos.create(_remap_existing_only(convos, {k: v for k, v in payload.items() if v is not None}))
            print(f"📤 LOG → Conversations[{record.get('id')}] {canonical_dir} → {phone} | {canonical_status}")
            return record.get("id")
        except Exception as e:
            print(f"⚠️ Failed to create Conversations row: {e}")
            traceback.print_exc()
            return None

    # -----------------------------------------------------------
    @staticmethod
    def _update_lead_activity(leads_tbl: Any, lead_id: str, body: str, direction: str, *, property_id: Optional[str] = None):
        """Safely update lead activity metrics."""
        now = utcnow_iso()
        canonical_dir = (
            ConversationDirection.OUTBOUND.value
            if direction.upper().startswith("OUT")
            else ConversationDirection.INBOUND.value
            if direction.upper().startswith("IN")
            else direction
        )
        patch = {
            LEAD_LAST_ACTIVITY_FIELD: now,
            LEAD_LAST_MESSAGE_FIELD: body[:500],
            LEAD_LAST_DIRECTION_FIELD: canonical_dir,
        }
        if property_id:
            patch[LEAD_PROPERTY_ID_FIELD] = property_id
        if canonical_dir == ConversationDirection.OUTBOUND.value:
            patch[LEAD_LAST_OUTBOUND_FIELD] = now
        else:
            patch[LEAD_LAST_INBOUND_FIELD] = now

        try:
            leads_tbl.update(lead_id, _remap_existing_only(leads_tbl, patch))
        except Exception as e:
            print(f"⚠️ Lead update failed for {lead_id}: {e}")

    # -----------------------------------------------------------
    @staticmethod
    def _safe_retry(convo_id: Optional[str], error: str):
        """Retry handler wrapper."""
        if not convo_id:
            return
        try:
            handle_retry(convo_id, error)
        except Exception as e:
            print(f"⚠️ handle_retry failed for {convo_id}: {e}")
