"""Delivery webhook normalising provider statuses and updating counters."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from . import spec
from .auth import require_webhook_token
from .datastore import (
    REPOSITORY,
    conversation_by_sid,
    create_conversation,
    touch_lead,
    update_conversation,
    update_lead_totals,
)


router = APIRouter()


async def _payload(request: Request) -> dict:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return dict(form)


def _validate(payload: dict) -> tuple[str, str, str, str | None]:
    message_sid = payload.get("MessageSid") or payload.get("MessageSID")
    status = payload.get("MessageStatus") or payload.get("Status")
    to_number = payload.get("To")
    from_number = payload.get("From")
    if not message_sid or not status:
        raise HTTPException(status_code=422, detail="Missing MessageSid or MessageStatus")
    if not to_number or not from_number:
        raise HTTPException(status_code=422, detail="Missing To/From for delivery update")
    normalized = spec.normalize_delivery_status(status)
    return message_sid, normalized, to_number, from_number


@router.post("/delivery")
async def delivery_handler(request: Request):
    await require_webhook_token(request)
    payload = await _payload(request)
    message_sid, status, to_number, from_number = _validate(payload)

    record = conversation_by_sid(message_sid)
    if not record:
        fields = {
            spec.CONVERSATION_FIELDS.textgrid_id: message_sid,
            spec.CONVERSATION_FIELDS.delivery_status: status,
            spec.CONVERSATION_FIELDS.seller_phone: to_number,
            spec.CONVERSATION_FIELDS.textgrid_phone: from_number,
        }
        record = create_conversation(message_sid, fields)

    record_id = record.get("id")
    if not record_id:
        raise HTTPException(status_code=500, detail="Failed to persist delivery event")

    update_conversation(
        record_id,
        {
            spec.CONVERSATION_FIELDS.delivery_status: status,
            spec.CONVERSATION_FIELDS.processed_by: "Delivery Webhook",
        },
    )

    lead_links = record.get("fields", {}).get(spec.CONVERSATION_FIELDS.lead_link) or []
    lead_id = lead_links[0] if lead_links else None

    if status == "DELIVERED":
        update_lead_totals(lead_id, delivered=1, status=status)
        REPOSITORY.increment_number_counters(from_number, delivered_total=1, delivered_today=1)
    elif status in {"FAILED", "UNDELIVERED"}:
        update_lead_totals(lead_id, failed=1, status=status)
        REPOSITORY.increment_number_counters(from_number, failed_total=1, failed_today=1)
    elif status == "OPT OUT" and lead_id:
        touch_lead(lead_id, body=None, direction="OUTBOUND", status=status)
        REPOSITORY.increment_number_counters(from_number, optout_total=1, optout_today=1)

    return {"status": "ok", "normalized": status}

