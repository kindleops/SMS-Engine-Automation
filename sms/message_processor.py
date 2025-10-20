# sms/message_processor.py
from __future__ import annotations

import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional

import os

try:
    from pyairtable import Table
except ImportError:
    Table = None

from sms.textgrid_sender import send_message
from sms.retry_handler import handle_retry
from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS, LEAD_FIELDS
from sms.airtable_schema import ConversationDirection, ConversationDeliveryStatus


# ---------- Field mappings (env-overridable) ----------
FROM_FIELD = CONV_FIELDS["FROM"]  # other party (seller) phone
TO_FIELD = CONV_FIELDS["TO"]  # our DID used to send
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

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")


# ---------- Small helpers ----------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else str(s)


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


# ---------- Lazy Airtable clients ----------
@lru_cache(maxsize=1)
def _tbl(table_name: str) -> Any:
    if not (AIRTABLE_KEY and LEADS_CONVOS_BASE and Table):
        return None
    try:
        return Table(AIRTABLE_KEY, LEADS_CONVOS_BASE, table_name)
    except Exception as e:
        print(f"⚠️ Airtable init failed for {table_name}: {e}")
        return None


@lru_cache(maxsize=1)  # keep same signature as earlier helpers
def get_convos():
    return _tbl(CONVERSATIONS_TABLE)


@lru_cache(maxsize=1)
def get_leads():
    return _tbl(LEADS_TABLE)


@lru_cache(maxsize=1)
def get_prospects():
    return _tbl(PROSPECTS_TABLE)


# ---------- Core Processor ----------
class MessageProcessor:
    @staticmethod
    def send(
        phone: str,
        body: str,
        *,
        from_number: str | None = None,  # DID you’re sending from
        campaign_id: str | None = None,  # linked Campaign record id
        template_id: str | None = None,  # linked Template record id
        drip_queue_id: str | None = None,  # (optional) if you want to back-link
        lead_id: str | None = None,
        property_id: str | None = None,
        direction: str = ConversationDirection.OUTBOUND.value,
        metadata: Dict[str, Any] | None = None,  # any extra fields to stash on Conversations
    ) -> dict:
        """
        Sends an SMS and logs it to Conversations safely.

        Returns:
          {
            "status": "sent"|"failed"|"skipped",
            "sid": "<carrier/telco sid>" | None,
            "phone": "<to_phone>",
            "body": "<message>",
            "convo_id": "<airtable id or mock>",
            "error": "<error if any>",
            ...
          }
        """
        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        convos = get_convos()
        leads = get_leads()

        # ---- 1) Send via provider
        try:
            send_result = send_message(phone, body, from_number=from_number)
        except Exception as e:
            print(f"❌ Transport error sending to {phone}: {e}")
            # We may still attempt to log a failed Conversations row for traceability
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
                metadata=metadata,
            )
            if convo_id:
                try:
                    handle_retry(convo_id, str(e))
                except Exception as ee:
                    print(f"⚠️ handle_retry failed: {ee}")
            return {"status": "failed", "phone": phone, "error": str(e), "convo_id": convo_id}

        # Normalize provider response
        sid = send_result.get("sid") or send_result.get("message_sid") or send_result.get("MessageSid") or send_result.get("id")
        provider_status = (send_result.get("status") or "sent").lower()
        ok = provider_status in {"sent", "queued", "accepted", "submitted", "enroute", "delivered"}

        # ---- 2) Log Conversations (safe field filter)
        convo_status = (
            ConversationDeliveryStatus.SENT.value
            if ok
            else ConversationDeliveryStatus.FAILED.value
        )
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
            metadata=metadata,
        )

        # ---- 3) Update Lead activity (best effort)
        if lead_id and leads:
            now_iso = utcnow_iso()
            dir_key = (direction or "").replace("_", " ").strip().upper()
            if dir_key in ("OUT", ConversationDirection.OUTBOUND.value):
                canonical_direction = ConversationDirection.OUTBOUND.value
            elif dir_key in ("IN", ConversationDirection.INBOUND.value):
                canonical_direction = ConversationDirection.INBOUND.value
            else:
                canonical_direction = direction or ConversationDirection.OUTBOUND.value
            patch = {
                LEAD_LAST_ACTIVITY_FIELD: now_iso,
                LEAD_LAST_MESSAGE_FIELD: body[:500],
                LEAD_LAST_DIRECTION_FIELD: canonical_direction,
            }
            if property_id:
                patch[LEAD_PROPERTY_ID_FIELD] = property_id
            if canonical_direction == ConversationDirection.OUTBOUND.value:
                patch[LEAD_LAST_OUTBOUND_FIELD] = now_iso
            elif canonical_direction == ConversationDirection.INBOUND.value:
                patch[LEAD_LAST_INBOUND_FIELD] = now_iso
            try:
                leads.update(lead_id, _remap_existing_only(leads, patch))
            except Exception as e:
                print(f"⚠️ Lead update failed for {lead_id}: {e}")

        # ---- 4) Retry hook on failure
        if not ok and convo_id:
            try:
                handle_retry(convo_id, str(send_result))
            except Exception as e:
                print(f"⚠️ handle_retry failed: {e}")

        # Done
        return {
            "status": "sent" if ok else "failed",
            "sid": sid,
            "phone": phone,
            "body": body,
            "convo_id": convo_id,
            "property_id": property_id,
            "provider_status": provider_status,
        }

    # ---------- internal: conversations logger ----------
    @staticmethod
    def _log_conversation(
        *,
        status: str,
        phone: str,
        body: str,
        from_number: str | None,
        direction: str,
        sid: str | None,
        campaign_id: str | None,
        template_id: str | None,
        drip_queue_id: str | None,
        metadata: Dict[str, Any] | None,
    ) -> Optional[str]:
        convos = get_convos()
        direction_key = (direction or "").replace("_", " ").strip().upper()
        if direction_key in ("OUT", ConversationDirection.OUTBOUND.value):
            canonical_direction = ConversationDirection.OUTBOUND.value
        elif direction_key in ("IN", ConversationDirection.INBOUND.value):
            canonical_direction = ConversationDirection.INBOUND.value
        else:
            canonical_direction = direction or ConversationDirection.OUTBOUND.value

        status_lookup = {
            "SENT": ConversationDeliveryStatus.SENT.value,
            "FAILED": ConversationDeliveryStatus.FAILED.value,
            "DELIVERED": ConversationDeliveryStatus.DELIVERED.value,
            "QUEUED": ConversationDeliveryStatus.QUEUED.value,
            "UNDELIVERED": ConversationDeliveryStatus.UNDELIVERED.value,
            "OPT OUT": ConversationDeliveryStatus.OPT_OUT.value,
        }
        status_key = (status or "").replace("_", " ").strip().upper()
        canonical_status = status_lookup.get(status_key, status or ConversationDeliveryStatus.SENT.value)

        payload = {
            # core mapped fields
            FROM_FIELD: phone,
            TO_FIELD: from_number,
            MSG_FIELD: body,
            DIR_FIELD: canonical_direction,
            STATUS_FIELD: canonical_status,
            SENT_AT_FIELD: utcnow_iso(),
            TEXTGRID_ID_FIELD: sid,
            # helpful links/trace
            CAMPAIGN_LINK_FIELD: [campaign_id] if campaign_id and CAMPAIGN_LINK_FIELD else None,
            TEMPLATE_LINK_FIELD: [template_id] if template_id and TEMPLATE_LINK_FIELD else None,
            DRIP_QUEUE_LINK_FIELD: [drip_queue_id] if drip_queue_id and DRIP_QUEUE_LINK_FIELD else None,
        }
        if metadata:
            payload.update(metadata)

        if not convos:
            # mock (no Airtable)
            print(f"[MOCK] Conversations ← {payload}")
            return "mock_convo"

        try:
            rec = convos.create(_remap_existing_only(convos, {k: v for k, v in payload.items() if v is not None}))
            print(f"📤 LOG → Conversations[{rec.get('id')}] {direction} to {phone} via {from_number} | {status}")
            return rec.get("id")
        except Exception as e:
            print(f"⚠️ Failed to create Conversations row: {e}")
            return None
