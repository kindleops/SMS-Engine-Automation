"""Outbound echo webhook honouring the README2.md schema."""

from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, HTTPException, Request

from sms.airtable_schema import CONVERSATIONS
from sms.conversation_store import (
    base_conversation_payload,
    normalize_phone,
    resolve_contact_links,
    update_conversation_links,
    update_lead_activity,
    upsert_conversation,
)


router = APIRouter(prefix="", tags=["Outbound"])


def _actor_from_payload(data: Dict[str, str]) -> str:
    actor = (data.get("Processed By") or data.get("processed_by") or data.get("actor") or "").strip()
    return actor or "Campaign Runner"


def handle_outbound(payload: Dict[str, str]) -> Dict[str, object]:
    to_number = normalize_phone(payload.get("To"))
    from_number = normalize_phone(payload.get("From"))
    body = (payload.get("Body") or "").strip()
    textgrid_id = payload.get("MessageSid") or payload.get("TextGridId")
    campaign_id = payload.get("Campaign Record ID") or payload.get("Campaign ID") or payload.get("campaign_id")
    template_id = payload.get("Template Record ID") or payload.get("Template ID") or payload.get("template_id")
    stage = payload.get("Stage") or payload.get("stage")

    if not to_number or not body:
        raise HTTPException(status_code=422, detail="Missing To or Body")

    lead_record, prospect_record = resolve_contact_links(to_number)

    conversation_payload = base_conversation_payload(
        seller_phone=to_number,
        textgrid_phone=from_number,
        body=body,
        direction="OUTBOUND",
        delivery_status="SENT",
        processed_by=_actor_from_payload(payload),
        stage=stage or (lead_record and lead_record.get("fields", {}).get(CONVERSATIONS.stage)) or "STAGE 2 - INTEREST FEELER",
        intent_detected="Neutral",
        ai_intent="other",
        textgrid_id=textgrid_id,
        campaign_id=campaign_id,
        template_id=template_id,
    )

    if lead_record:
        conversation_payload[CONVERSATIONS.lead_record_id] = lead_record["id"]
        conversation_payload[CONVERSATIONS.link_lead] = [lead_record["id"]]
    elif prospect_record:
        conversation_payload[CONVERSATIONS.prospect_record_id] = prospect_record["id"]
        conversation_payload[CONVERSATIONS.link_prospect] = [prospect_record["id"]]

    conversation_id = upsert_conversation(conversation_payload, textgrid_id)

    update_lead_activity(
        lead_record,
        body=body,
        direction="OUTBOUND",
        delivery_status="SENT",
        reply_increment=False,
        send_increment=True,
    )

    update_conversation_links(conversation_id, lead=lead_record, prospect=prospect_record, textgrid_id=textgrid_id)

    return {
        "status": "ok",
        "conversation_id": conversation_id,
        "linked_to": "lead" if lead_record else "prospect",
    }


@router.post("/outbound")
async def outbound_handler(request: Request) -> Dict[str, object]:
    form = await request.form()
    return handle_outbound(dict(form))

