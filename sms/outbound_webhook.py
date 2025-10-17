"""Outbound webhook logging for sent messages."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from . import spec
from .auth import require_webhook_token
from .datastore import (
    create_conversation,
    ensure_prospect_or_lead,
    touch_lead,
)


router = APIRouter()


async def _payload(request: Request) -> dict:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    data = await request.form()
    return dict(data)


def _validate(payload: dict) -> tuple[str, str, str]:
    to_number = payload.get("To")
    from_number = payload.get("From")
    body = payload.get("Body")
    if not to_number or not from_number or not body:
        raise HTTPException(status_code=422, detail="Missing To, From or Body")
    norm_to = spec.normalize_phone(to_number) or to_number
    norm_from = spec.normalize_phone(from_number) or from_number
    return norm_to, norm_from, str(body)


@router.post("/outbound")
async def outbound_handler(request: Request):
    await require_webhook_token(request)
    payload = await _payload(request)
    to_number, from_number, body = _validate(payload)

    message_sid = payload.get("MessageSid") or payload.get("TextGridId")

    fields = {
        spec.CONVERSATION_FIELDS.seller_phone: to_number,
        spec.CONVERSATION_FIELDS.textgrid_phone: from_number,
        spec.CONVERSATION_FIELDS.direction: "OUTBOUND",
        spec.CONVERSATION_FIELDS.delivery_status: "SENT",
        spec.CONVERSATION_FIELDS.message_body: body,
        spec.CONVERSATION_FIELDS.last_sent_time: spec.iso_now(),
        spec.CONVERSATION_FIELDS.processed_by: payload.get("ProcessedBy", "Outbound Webhook"),
    }
    if message_sid:
        fields[spec.CONVERSATION_FIELDS.textgrid_id] = message_sid

    template_id = payload.get("Template Record ID") or payload.get("TemplateId")
    if template_id:
        fields[spec.CONVERSATION_FIELDS.template_record_id] = template_id

    lead_record, prospect_record = ensure_prospect_or_lead(to_number)
    lead_id = lead_record["id"] if lead_record else None
    prospect_id = prospect_record["id"] if prospect_record else None

    if lead_id:
        fields[spec.CONVERSATION_FIELDS.lead_link] = [lead_id]
    elif prospect_id:
        fields[spec.CONVERSATION_FIELDS.prospect_link] = [prospect_id]

    record = create_conversation(message_sid, fields)

    if lead_id:
        touch_lead(lead_id, body=body, direction="OUTBOUND", status="SENT")

    return {"status": "ok", "conversation_id": record.get("id")}

