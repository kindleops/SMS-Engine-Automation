"""Inbound webhook that enforces the README2.md contracts."""

from __future__ import annotations

import re
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Request

from sms.airtable_schema import CONVERSATIONS, PROMOTION_STAGE
from sms.conversation_store import (
    base_conversation_payload,
    normalize_phone,
    promote_to_lead,
    resolve_contact_links,
    update_conversation_links,
    update_lead_activity,
    upsert_conversation,
)
from sms.number_pools import increment_opt_out


router = APIRouter(prefix="", tags=["Inbound"])


STOP_RE = re.compile(r"\b(stop|unsubscribe|remove|opt\s*out)\b", re.I)

POSITIVE_WORDS = {
    "yes",
    "yeah",
    "interested",
    "offer",
    "talk",
    "price",
    "sure",
    "okay",
    "ok",
    "let's",
    "lets",
}

PRICE_WORDS = {"price", "number", "cost", "offer", "dollar", "$"}
CONDITION_WORDS = {"condition", "repairs", "fix", "renovate", "update"}
MOTIVATION_WORDS = {"timeline", "when", "ready", "soon", "move", "motivation", "close"}
DELAY_WORDS = {"later", "busy", "follow up", "next week", "tomorrow", "call me", "text me"}
REJECT_WORDS = {"no", "not interested", "stop", "wrong", "dnc", "remove"}


def classify_intent(body: str) -> Dict[str, Optional[str]]:
    text = (body or "").strip().lower()
    if not text:
        return {
            "intent_detected": "Neutral",
            "ai_intent": "neutral",
            "stage": CONVERSATIONS.allowed_stages[0],
            "promote": False,
        }

    if STOP_RE.search(text):
        return {
            "intent_detected": "DNC",
            "ai_intent": "not_interested",
            "stage": "OPT OUT",
            "promote": False,
        }

    if any(word in text for word in PRICE_WORDS):
        return {
            "intent_detected": "Positive",
            "ai_intent": "ask_price",
            "stage": PROMOTION_STAGE,
            "promote": True,
        }

    if any(word in text for word in CONDITION_WORDS):
        return {
            "intent_detected": "Positive",
            "ai_intent": "condition_question",
            "stage": "STAGE 4 - PROPERTY CONDITION",
            "promote": True,
        }

    if any(word in text for word in MOTIVATION_WORDS):
        return {
            "intent_detected": "Positive",
            "ai_intent": "motivation_detected",
            "stage": "STAGE 5 - MOTIVATION / TIMELINE",
            "promote": True,
        }

    if any(word in text for word in POSITIVE_WORDS):
        return {
            "intent_detected": "Positive",
            "ai_intent": "interest_detected",
            "stage": PROMOTION_STAGE,
            "promote": True,
        }

    if any(word in text for word in DELAY_WORDS):
        return {
            "intent_detected": "Delay",
            "ai_intent": "delay",
            "stage": "STAGE 5 - MOTIVATION / TIMELINE",
            "promote": False,
        }

    if any(word in text for word in REJECT_WORDS):
        return {
            "intent_detected": "Reject",
            "ai_intent": "not_interested",
            "stage": "STAGE 2 - INTEREST FEELER",
            "promote": False,
        }

    return {
        "intent_detected": "Neutral",
        "ai_intent": "neutral",
        "stage": "STAGE 1 - OWNERSHIP CONFIRMATION",
        "promote": False,
    }


def _validate_payload(payload: Dict[str, str]) -> None:
    if not payload.get("From") or not payload.get("Body"):
        raise HTTPException(status_code=422, detail="Missing From or Body")


def handle_inbound(payload: Dict[str, str]) -> Dict[str, object]:
    _validate_payload(payload)

    seller_phone = normalize_phone(payload.get("From"))
    to_number = normalize_phone(payload.get("To"))
    body = (payload.get("Body") or "").strip()
    textgrid_id = payload.get("MessageSid") or payload.get("TextGridId")

    intent = classify_intent(body)

    lead_record, prospect_record = resolve_contact_links(seller_phone)

    delivery_status = "DELIVERED" if intent["stage"] != "OPT OUT" else "OPT OUT"

    conversation_payload = base_conversation_payload(
        seller_phone=seller_phone,
        textgrid_phone=to_number,
        body=body,
        direction="INBOUND",
        delivery_status=delivery_status,
        processed_by="Manual / Human",
        stage=intent["stage"],
        intent_detected=intent["intent_detected"],
        ai_intent=intent["ai_intent"],
        textgrid_id=textgrid_id,
    )

    if lead_record:
        conversation_payload[CONVERSATIONS.lead_record_id] = lead_record["id"]
        conversation_payload[CONVERSATIONS.link_lead] = [lead_record["id"]]
    elif prospect_record:
        conversation_payload[CONVERSATIONS.prospect_record_id] = prospect_record["id"]
        conversation_payload[CONVERSATIONS.link_prospect] = [prospect_record["id"]]

    conversation_id = upsert_conversation(conversation_payload, textgrid_id)

    if not lead_record and intent["promote"]:
        lead_record = promote_to_lead(seller_phone or "", source="Inbound")
        if lead_record:
            update_conversation_links(
                conversation_id,
                lead=lead_record,
                textgrid_id=textgrid_id,
            )
            prospect_record = None

    if intent["stage"] == "OPT OUT":
        increment_opt_out(seller_phone or "")

    update_lead_activity(
        lead_record,
        body=body,
        direction="INBOUND",
        delivery_status=delivery_status,
        reply_increment=True,
    )

    status = "optout" if intent["stage"] == "OPT OUT" else "ok"

    return {
        "status": status,
        "conversation_id": conversation_id,
        "linked_to": "lead" if lead_record else "prospect",
        "stage": intent["stage"],
        "intent": intent["intent_detected"],
    }


def process_optout(payload: Dict[str, str]) -> Dict[str, object]:
    payload = dict(payload)
    payload["Body"] = payload.get("Body") or "STOP"
    return handle_inbound(payload)


def process_status(payload: Dict[str, str]) -> Dict[str, object]:
    status = (payload.get("MessageStatus") or payload.get("status") or "").lower()
    normalized = "delivered" if status == "delivered" else "failed" if status in {"failed", "undelivered"} else "sent"
    return {"ok": True, "status": normalized}


@router.post("/inbound")
async def inbound_handler(request: Request) -> Dict[str, object]:
    form = await request.form()
    return handle_inbound(dict(form))


@router.post("/optout")
async def optout_handler(request: Request) -> Dict[str, object]:
    form = await request.form()
    return process_optout(dict(form))


@router.post("/status")
async def status_handler(request: Request) -> Dict[str, object]:
    form = await request.form()
    return process_status(dict(form))

