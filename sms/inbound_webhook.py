# sms/inbound_webhook.py
"""
Inbound SMS + Opt-Out Webhook
-----------------------------
Unified inbound handler (TextGrid/Twilio compatible)
Schema-aware via datastore.
"""

from __future__ import annotations
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Request
from typing import Any, Dict
from sms.datastore import CONNECTOR, create_conversation, ensure_prospect_or_lead, update_conversation, update_record, touch_lead
from sms.airtable_schema import ConversationDirection
from sms.number_pools import increment_delivered, increment_opt_out
from sms.runtime import get_logger, iso_now

router = APIRouter(prefix="/inbound", tags=["Inbound"])
logger = get_logger("inbound")

STOP_TERMS = {"stop", "unsubscribe", "remove", "opt out", "opt-out", "optout", "quit", "cancel"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _parse_payload(request: Request) -> Dict[str, Any]:
    """Parse inbound request to dict (JSON or Form)."""
    if "application/json" in request.headers.get("content-type", ""):
        return await request.json()
    return dict(await request.form())


def _digits(v: str | None) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())


def _e164(v: str | None, field="Phone") -> str:
    d = _digits(v)
    if len(d) == 10:
        return "+1" + d
    if len(d) == 11 and d.startswith("1"):
        return "+" + d
    if v and v.startswith("+"):
        return "+" + d
    raise HTTPException(status_code=422, detail=f"Invalid {field}")


def _sanitize(body: Any) -> str:
    txt = str(body or "").strip()
    if not txt:
        raise HTTPException(status_code=422, detail="Empty Body")
    return txt


def _is_stop(msg: str) -> bool:
    return any(t in " ".join(msg.lower().split()) for t in STOP_TERMS)


# ---------------------------------------------------------------------------
# Core workflows
# ---------------------------------------------------------------------------


def _log_conv(data: dict, lead_id: str | None, prospect_id: str | None, status="DELIVERED", processed_by="Inbound Webhook"):
    fields = {
        "Seller Phone Number": data["from"],
        "TextGrid Phone Number": data.get("to"),
        "Message": data["body"],
        "Direction": ConversationDirection.INBOUND.value,
        "Status": status,
        "Received At": iso_now(),
        "Processed By": processed_by,
    }
    if data.get("sid"):
        fields["TextGrid ID"] = data["sid"]
    if lead_id:
        fields["Lead"] = [lead_id]
    if prospect_id:
        fields["Prospect"] = [prospect_id]
    return create_conversation(data.get("sid"), fields)


def _update_lead(lead_id: str | None, body: str):
    if not lead_id:
        return
    try:
        touch_lead(lead_id, body=body, direction=ConversationDirection.INBOUND.value)
    except Exception:
        logger.warning("Lead touch failed", exc_info=True)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _process_inbound(data: dict) -> dict:
    from_n = _e164(data.get("From"), "From")
    to_n = _e164(data.get("To"), "To") if data.get("To") else None
    body = _sanitize(data.get("Body"))
    sid = data.get("MessageSid") or data.get("TextGridId")

    # STOP → redirect
    if _is_stop(body):
        return _process_optout(data)

    lead, prospect = ensure_prospect_or_lead(from_n)
    lead_id = (lead or {}).get("id")
    prospect_id = (prospect or {}).get("id")

    record = _log_conv({"from": from_n, "to": to_n, "body": body, "sid": sid}, lead_id, prospect_id)
    _update_lead(lead_id, body)
    if to_n:
        try:
            increment_delivered(to_n)
        except Exception:
            logger.warning("Increment delivered failed", exc_info=True)
    return {"status": "ok", "conversation_id": (record or {}).get("id"), "message_sid": sid, "linked": "lead" if lead_id else "prospect"}


def _process_optout(data: dict) -> dict:
    from_n = _e164(data.get("From"), "From")
    to_n = data.get("To")
    body = _sanitize(data.get("Body"))
    sid = data.get("MessageSid") or data.get("TextGridId")

    if to_n:
        try:
            increment_opt_out(to_n)
        except Exception:
            logger.warning("Increment opt-out failed", exc_info=True)

    lead, prospect = ensure_prospect_or_lead(from_n)
    lead_id = (lead or {}).get("id")
    prospect_id = (prospect or {}).get("id")

    record = _log_conv(
        {"from": from_n, "to": to_n, "body": body, "sid": sid}, lead_id, prospect_id, status="DNC", processed_by="Opt-Out Webhook"
    )
    if lead_id:
        try:
            update_record(CONNECTOR.leads(), lead_id, {"Status": "DNC"})
            _update_lead(lead_id, body)
        except Exception:
            logger.warning("Lead DNC update failed", exc_info=True)

    if record:
        try:
            update_conversation(record["id"], {"Status": "DNC", "Processed By": "Opt-Out Webhook"})
        except Exception:
            logger.warning("Conversation DNC flag failed", exc_info=True)

    return {
        "status": "optout",
        "conversation_id": (record or {}).get("id"),
        "message_sid": sid,
        "linked": "lead" if lead_id else "prospect",
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("")
async def inbound_entry(request: Request):
    """Unified webhook for inbound + opt-out detection."""
    data = await _parse_payload(request)
    try:
        result = _process_inbound(data)
        logger.info("Inbound → %s", result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Inbound failure")
        raise HTTPException(status_code=500, detail=str(e))
