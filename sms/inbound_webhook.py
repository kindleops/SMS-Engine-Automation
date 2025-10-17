"""Inbound webhook implementation aligned with README2.md specification."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from . import autoresponder, spec
from .auth import require_webhook_token
from .datastore import (
    create_conversation,
    ensure_prospect_or_lead,
    promote_if_needed,
    touch_lead,
)


router = APIRouter()


async def _payload_from_request(request: Request) -> dict:
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    form = await request.form()
    return dict(form)


def _validate_payload(payload: dict) -> tuple[str, str, str]:
    from_number = payload.get("From")
    body = payload.get("Body")
    if not from_number or not body:
        raise HTTPException(status_code=422, detail="Missing From or Body")
    to_number = payload.get("To") or ""
    norm_from = spec.normalize_phone(from_number)
    norm_to = spec.normalize_phone(to_number) if to_number else to_number
    return norm_from or from_number, norm_to or to_number, str(body)


def _base_conversation_fields(payload: dict, from_number: str, to_number: str, body: str) -> dict:
    fields = {
        spec.CONVERSATION_FIELDS.seller_phone: from_number,
        spec.CONVERSATION_FIELDS.textgrid_phone: to_number,
        spec.CONVERSATION_FIELDS.message_body: body,
        spec.CONVERSATION_FIELDS.direction: "INBOUND",
        spec.CONVERSATION_FIELDS.received_time: spec.iso_now(),
        spec.CONVERSATION_FIELDS.delivery_status: "SENT",
    }
    message_sid = payload.get("MessageSid") or payload.get("MessageSID") or payload.get("SmsSid")
    if message_sid:
        fields[spec.CONVERSATION_FIELDS.textgrid_id] = message_sid
    return fields


def _stop_response(fields: dict, from_number: str) -> dict:
    fields.update(
        {
            spec.CONVERSATION_FIELDS.delivery_status: "OPT OUT",
            spec.CONVERSATION_FIELDS.stage: "OPT OUT",
            spec.CONVERSATION_FIELDS.intent_detected: "DNC",
            spec.CONVERSATION_FIELDS.ai_intent: "not_interested",
        }
    )
    create_conversation(fields.get(spec.CONVERSATION_FIELDS.textgrid_id), fields)
    promote_if_needed(from_number, fields, "OPT OUT")
    return {"status": "optout"}


def _log_inbound(payload: dict, fields: dict, classification: autoresponder.IntentClassification, lead_id: str | None, prospect_id: str | None) -> dict:
    fields.update(
        {
            spec.CONVERSATION_FIELDS.intent_detected: classification.intent_detected,
            spec.CONVERSATION_FIELDS.ai_intent: classification.ai_intent,
            spec.CONVERSATION_FIELDS.processed_by: "Inbound Webhook",
            spec.CONVERSATION_FIELDS.message_summary: classification.summary,
        }
    )
    if classification.stage:
        fields[spec.CONVERSATION_FIELDS.stage] = classification.stage

    message_sid = fields.get(spec.CONVERSATION_FIELDS.textgrid_id)
    record = create_conversation(message_sid, fields)

    if lead_id:
        record.setdefault("fields", {}).setdefault(spec.CONVERSATION_FIELDS.lead_link, [lead_id])
    elif prospect_id:
        record.setdefault("fields", {}).setdefault(spec.CONVERSATION_FIELDS.prospect_link, [prospect_id])

    return record


@router.post("/inbound")
async def inbound_handler(request: Request):
    await require_webhook_token(request)
    payload = await _payload_from_request(request)
    from_number, to_number, body = _validate_payload(payload)

    fields = _base_conversation_fields(payload, from_number, to_number, body)

    if spec.valid_stop_payload(body):
        return _stop_response(fields, from_number)

    classification = autoresponder.classify_intent(body)

    lead_record, prospect_record = ensure_prospect_or_lead(from_number)
    lead_id = lead_record["id"] if lead_record else None
    prospect_id = prospect_record["id"] if prospect_record else None

    record = _log_inbound(payload, fields, classification, lead_id, prospect_id)

    if classification.should_promote:
        promote_if_needed(from_number, fields, classification.stage)

    if lead_id:
        touch_lead(lead_id, body=body, direction="INBOUND", status="SENT")

    autoresponder.maybe_send_reply(
        from_number=from_number,
        to_number=to_number,
        classification=classification,
        conversation_id=record.get("id"),
    )

    response = {"status": "ok", "conversation_id": record.get("id")}
    if lead_id:
        response["linked_to"] = "lead"
    elif prospect_id:
        response["linked_to"] = "prospect"
    return response

