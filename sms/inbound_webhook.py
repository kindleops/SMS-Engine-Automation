"""Inbound SMS webhook integrated with the schema-aware datastore (TextGrid)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request

from sms.airtable_schema import (
    ConversationDeliveryStatus,
    ConversationDirection,
    conversations_field_map,
    leads_field_map,
    prospects_field_map,
)
from sms.datastore import (
    CONNECTOR,
    create_conversation,
    ensure_prospect_or_lead,
    update_conversation,
    update_record,
    promote_to_lead as _promote_to_lead,
    touch_lead,
)
from sms.number_pools import increment_delivered, increment_opt_out
from sms.runtime import get_logger, iso_now

# ---------------------------------------------------------------------------
# Router + logger
# ---------------------------------------------------------------------------

router = APIRouter()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Field maps
# ---------------------------------------------------------------------------

CONV_FIELDS = conversations_field_map()
LEAD_FIELDS = leads_field_map()
PROSPECT_FIELDS = prospects_field_map()

CONV_FROM_FIELD = CONV_FIELDS["FROM"]          # Seller Phone Number
CONV_TO_FIELD = CONV_FIELDS["TO"]              # TextGrid Phone Number
CONV_BODY_FIELD = CONV_FIELDS["BODY"]
CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_STATUS_FIELD = CONV_FIELDS["STATUS"]
CONV_TEXTGRID_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
CONV_RECEIVED_AT_FIELD = CONV_FIELDS["RECEIVED_AT"]
CONV_PROCESSED_BY_FIELD = CONV_FIELDS["PROCESSED_BY"]
CONV_LEAD_LINK_FIELD = CONV_FIELDS.get("LEAD_LINK", "Lead")
CONV_PROSPECT_LINK_FIELD = CONV_FIELDS.get("PROSPECT_LINK", "Prospect")

STOP_TERMS = {"stop", "unsubscribe", "remove", "opt out", "opt-out", "optout", "quit", "cancel"}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

async def _payload(request: Request) -> Dict[str, Any]:
    """Normalize inbound FastAPI request (JSON or Form)."""
    if request.headers.get("content-type", "").startswith("application/json"):
        data = await request.json()
        return dict(data)
    form = await request.form()
    return dict(form)


def _digits(value: str | None) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def normalize_e164(value: str, *, field: str) -> str:
    """Normalize phone numbers to +E.164 format."""
    digits = _digits(value)
    if not digits:
        raise HTTPException(status_code=422, detail=f"Missing required field: {field}")
    if len(digits) == 10:
        digits = "1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if value.startswith("+"):
        return "+" + digits
    raise HTTPException(status_code=422, detail=f"Invalid phone number for {field}")


def sanitize_body(value: str | None, *, field: str = "Body") -> str:
    if value is None:
        raise HTTPException(status_code=422, detail=f"Missing required field: {field}")
    text = str(value).strip()
    if not text:
        raise HTTPException(status_code=422, detail=f"Missing required field: {field}")
    return text


def is_stop_message(body: str) -> bool:
    """Detect opt-out messages like 'STOP' or 'UNSUBSCRIBE'."""
    folded = " ".join(body.lower().split())
    return any(term in folded for term in STOP_TERMS)


# ---------------------------------------------------------------------------
# Airtable helpers
# ---------------------------------------------------------------------------

def promote_prospect_to_lead(phone_number: str, source: str = "Inbound") -> tuple[Optional[str], Optional[str]]:
    return _promote_to_lead(phone_number, source=source, conversation_fields=None)


def log_conversation(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Log inbound SMS to Conversations table with schema alignment."""
    message_sid = payload.get("message_sid")
    fields: Dict[str, Any] = {
        CONV_FROM_FIELD: payload.get("from_number"),  # Seller
        CONV_TO_FIELD: payload.get("to_number"),      # Our TextGrid DID
        CONV_BODY_FIELD: payload.get("body"),
        CONV_DIRECTION_FIELD: ConversationDirection.INBOUND.value,
        CONV_STATUS_FIELD: payload.get("status") or ConversationDeliveryStatus.DELIVERED.value,
        CONV_RECEIVED_AT_FIELD: iso_now(),
        CONV_PROCESSED_BY_FIELD: payload.get("processed_by", "Inbound Webhook"),
    }

    if message_sid:
        fields[CONV_TEXTGRID_ID_FIELD] = message_sid
    if payload.get("lead_id"):
        fields[CONV_LEAD_LINK_FIELD] = [payload["lead_id"]]
    if payload.get("prospect_id"):
        fields[CONV_PROSPECT_LINK_FIELD] = [payload["prospect_id"]]

    try:
        record = create_conversation(message_sid, fields)
        return record
    except Exception as exc:
        logger.exception("Airtable conversation log failed: %s", exc)
        return None


def update_lead_activity(
    lead_id: Optional[str],
    body: str,
    direction: str,
) -> None:
    """Update last message activity on lead."""
    if not lead_id:
        return
    try:
        touch_lead(lead_id, body=body, direction=direction)
    except Exception:
        logger.warning("Failed to touch lead activity for %s", lead_id, exc_info=True)


# ---------------------------------------------------------------------------
# Core workflows
# ---------------------------------------------------------------------------

def handle_inbound(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Process inbound (non-opt-out) message."""
    from_number = normalize_e164(payload.get("From"), field="From")     # Seller number
    to_number_raw = payload.get("To")
    to_number = normalize_e164(to_number_raw, field="To") if to_number_raw else None  # Our DID
    body = sanitize_body(payload.get("Body"))
    message_sid = payload.get("MessageSid") or payload.get("TextGridId")

    # STOP detection
    if is_stop_message(body):
        return process_optout(payload)

    # Find or create lead/prospect
    lead_record, prospect_record = ensure_prospect_or_lead(from_number)
    lead_hint = (lead_record or {}).get("id")
    prospect_id = (prospect_record or {}).get("id")

    lead_id, property_id = promote_prospect_to_lead(from_number, source="Inbound")
    if not lead_id:
        lead_id = lead_hint

    # Log conversation to Airtable
    record = log_conversation(
        {
            "from_number": from_number,
            "to_number": to_number,
            "body": body,
            "message_sid": message_sid,
            "lead_id": lead_id,
            "prospect_id": prospect_id,
        }
    )

    update_lead_activity(lead_id, body, ConversationDirection.INBOUND.value)

    if to_number:
        try:
            increment_delivered(to_number)
        except Exception:
            logger.warning("Failed to increment delivered counter for %s", to_number, exc_info=True)

    return {
        "status": "ok",
        "conversation_id": (record or {}).get("id"),
        "message_sid": message_sid,
        "linked_to": "lead" if lead_id else ("prospect" if prospect_id else None),
        "property_id": property_id,
    }


def process_optout(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Handle STOP/DNC inbound messages and suppress future outreach."""
    from_number = normalize_e164(payload.get("From"), field="From")
    to_number = payload.get("To")
    body = sanitize_body(payload.get("Body"))
    message_sid = payload.get("MessageSid") or payload.get("TextGridId")

    if to_number:
        try:
            increment_opt_out(to_number)
        except Exception:
            logger.warning("Failed to increment opt-out counter for %s", to_number, exc_info=True)

    lead_record, prospect_record = ensure_prospect_or_lead(from_number)
    lead_hint = (lead_record or {}).get("id")
    prospect_id = (prospect_record or {}).get("id")

    lead_id, _ = promote_prospect_to_lead(from_number, source="Opt-Out")
    if not lead_id:
        lead_id = lead_hint

    record = log_conversation(
        {
            "from_number": from_number,
            "to_number": to_number,
            "body": body,
            "message_sid": message_sid,
            "lead_id": lead_id,
            "prospect_id": prospect_id,
            "status": "DNC",
            "processed_by": "Opt-Out Webhook",
        }
    )

    if lead_id:
        try:
            update_record(CONNECTOR.leads(), lead_id, {LEAD_FIELDS["STATUS"]: "DNC"})
            update_lead_activity(lead_id, body, ConversationDirection.INBOUND.value)
        except Exception:
            logger.warning("Failed to flag lead DNC for %s", lead_id, exc_info=True)

    if record and record.get("id"):
        try:
            update_conversation(
                record["id"],
                {
                    CONV_STATUS_FIELD: "DNC",
                    CONV_PROCESSED_BY_FIELD: "Opt-Out Webhook",
                },
            )
        except Exception:
            logger.warning("Failed to update conversation DNC for %s", record.get("id"), exc_info=True)

    return {
        "status": "optout",
        "conversation_id": (record or {}).get("id"),
        "message_sid": message_sid,
        "linked_to": "lead" if lead_id else ("prospect" if prospect_id else None),
    }


# ---------------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------------

@router.post("/inbound")
async def inbound_handler(request: Request):
    """Webhook entrypoint for inbound SMS messages."""
    payload = await _payload(request)
    try:
        result = handle_inbound(payload)
        logger.info("✅ Inbound processed → %s", result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Inbound webhook failure")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/optout")
async def optout_handler(request: Request):
    """Webhook entrypoint for STOP / DNC messages."""
    payload = await _payload(request)
    try:
        result = process_optout(payload)
        logger.info("⚠️ Opt-out processed → %s", result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Opt-out webhook failure")
        raise HTTPException(status_code=500, detail=str(exc))
