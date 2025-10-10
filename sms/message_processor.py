# sms/message_processor.py
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional

try:
    from pyairtable import Table
except ImportError:
    Table = None

from sms.textgrid_sender import send_message
from sms.retry_handler import handle_retry


# ---------- Field mappings (env-overridable) ----------
FROM_FIELD              = os.getenv("CONV_FROM_FIELD", "phone")          # other party (seller) phone
TO_FIELD                = os.getenv("CONV_TO_FIELD", "to_number")        # our DID used to send
MSG_FIELD               = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD            = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD               = os.getenv("CONV_DIRECTION_FIELD", "direction")
SENT_AT_FIELD           = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
TEXTGRID_ID_FIELD       = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")

CONVERSATIONS_TABLE     = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE             = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE         = os.getenv("PROSPECTS_TABLE", "Prospects")

AIRTABLE_KEY            = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE       = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")


# ---------- Small helpers ----------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+","", s.strip().lower()) if isinstance(s, str) else str(s)

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
def get_convos():    return _tbl(CONVERSATIONS_TABLE)

@lru_cache(maxsize=1)
def get_leads():     return _tbl(LEADS_TABLE)

@lru_cache(maxsize=1)
def get_prospects(): return _tbl(PROSPECTS_TABLE)


# ---------- Core Processor ----------
class MessageProcessor:
    @staticmethod
    def send(
        phone: str,
        body: str,
        *,
        from_number: str | None = None,        # DID you’re sending from
        campaign_id: str | None = None,        # linked Campaign record id
        template_id: str | None = None,        # linked Template record id
        drip_queue_id: str | None = None,      # (optional) if you want to back-link
        lead_id: str | None = None,
        property_id: str | None = None,
        direction: str = "OUT",
        metadata: Dict[str, Any] | None = None # any extra fields to stash on Conversations
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
        leads  = get_leads()

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
        sid = (
            send_result.get("sid")
            or send_result.get("message_sid")
            or send_result.get("MessageSid")
            or send_result.get("id")
        )
        provider_status = (send_result.get("status") or "sent").lower()
        ok = provider_status in {"sent", "queued", "accepted", "submitted", "enroute", "delivered"}

        # ---- 2) Log Conversations (safe field filter)
        convo_status = "SENT" if ok else "FAILED"
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
            patch = {
                "Last Activity": now_iso,
                "Last Message": body[:500],
                "Property ID": property_id,
            }
            if direction == "OUT":
                patch["Last Outbound"] = now_iso
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
        payload = {
            # core mapped fields
            FROM_FIELD: phone,
            TO_FIELD: from_number,
            MSG_FIELD: body,
            DIR_FIELD: direction,
            STATUS_FIELD: status,
            SENT_AT_FIELD: utcnow_iso(),
            TEXTGRID_ID_FIELD: sid,
            # helpful links/trace
            "Campaign": [campaign_id] if campaign_id else None,
            "Template": [template_id] if template_id else None,
            "Drip Queue": [drip_queue_id] if drip_queue_id else None,  # will be ignored if field not present
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