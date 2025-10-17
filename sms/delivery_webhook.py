"""Delivery receipt webhook aligned with README2.md."""

from __future__ import annotations

from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request

from sms.airtable_schema import CONVERSATIONS
from sms.conversation_store import update_conversation_links, update_lead_activity
from sms.tables import get_convos, get_leads


router = APIRouter(prefix="/delivery", tags=["Delivery"])

STATUS_NORMALISATION = {
    "queued": "QUEUED",
    "accepted": "QUEUED",
    "sending": "QUEUED",
    "sent": "SENT",
    "delivered": "DELIVERED",
    "failed": "FAILED",
    "undelivered": "UNDELIVERED",
    "optout": "OPT OUT",
}


def _normalize_status(raw_status: Optional[str]) -> str:
    value = (raw_status or "").lower().strip()
    return STATUS_NORMALISATION.get(value, "SENT")


def _extract_payload(data: Dict[str, str]) -> Dict[str, Optional[str]]:
    return {
        "sid": data.get("MessageSid") or data.get("sid") or data.get("MessageID"),
        "status": data.get("MessageStatus") or data.get("status"),
        "to": data.get("To") or data.get("to"),
        "from": data.get("From") or data.get("from"),
        "error": data.get("ErrorMessage") or data.get("error"),
    }


def _find_conversation_by_sid(sid: Optional[str]) -> Optional[Dict[str, object]]:
    tbl = get_convos()
    if not tbl or not sid:
        return None
    try:
        for record in tbl.all():
            fields = record.get("fields", {}) or {}
            if str(fields.get(CONVERSATIONS.textgrid_id) or "") == sid:
                return record
    except Exception as exc:  # pragma: no cover - network error path
        print(f"⚠️ Failed to locate conversation {sid}: {exc}")
    return None


def _find_lead_from_conversation(record: Dict[str, object]) -> Optional[Dict[str, object]]:
    tbl = get_leads()
    if not tbl:
        return None
    fields = record.get("fields", {}) or {}
    lead_ids = fields.get(CONVERSATIONS.link_lead) or []
    if not lead_ids:
        return None
    lead_id = lead_ids[0]
    try:
        return tbl.get(lead_id)
    except Exception:  # pragma: no cover - network error path
        return None


def _update_conversation_status(conversation_id: str, status: str, error: Optional[str]) -> None:
    tbl = get_convos()
    if not tbl or not conversation_id:
        return
    payload = {
        CONVERSATIONS.delivery_status: status,
    }
    if status == "DELIVERED":
        payload[CONVERSATIONS.processed_time] = None
    if error:
        payload["Last Error"] = error[:500]
    try:
        tbl.update(conversation_id, payload)
    except Exception as exc:  # pragma: no cover - network error path
        print(f"⚠️ Failed to update conversation {conversation_id}: {exc}")


@router.post("")
async def delivery_handler(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
) -> Dict[str, object]:
    token = x_webhook_token or request.headers.get("Authorization", "").replace("Bearer", "").strip()
    expected = request.app.state.__dict__.get("delivery_token") if hasattr(request.app, "state") else None
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="invalid token")

    try:
        body = await request.json()
    except Exception:
        form = await request.form()
        body = dict(form)

    payload = _extract_payload(body)
    sid = payload["sid"]
    status = _normalize_status(payload["status"])

    conversation_record = _find_conversation_by_sid(sid)
    if conversation_record:
        fields = conversation_record.get("fields", {}) or {}
        previous_status = fields.get(CONVERSATIONS.delivery_status)
        status_changed = previous_status != status
        conversation_id = conversation_record["id"]
        _update_conversation_status(conversation_id, status, payload["error"])
        lead_record = _find_lead_from_conversation(conversation_record)
        update_lead_activity(
            lead_record,
            body=conversation_record.get("fields", {}).get(CONVERSATIONS.message_long, ""),
            direction="OUTBOUND",
            delivery_status=status,
            reply_increment=False,
            send_increment=False,
            status_changed=status_changed,
        )
        update_conversation_links(conversation_id, lead=lead_record, textgrid_id=sid)

    return {"status": "ok", "sid": sid, "normalized": status}

