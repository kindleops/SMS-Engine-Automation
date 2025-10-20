<<<<<<< HEAD
import os
import re
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table

from sms.inbound_webhook import normalize_e164, sanitize_body

router = APIRouter()

# === ENV CONFIG ===
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE         = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE     = os.getenv("PROSPECTS_TABLE", "Prospects")
CAMPAIGNS_TABLE     = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE     = os.getenv("TEMPLATES_TABLE", "Templates")

if not AIRTABLE_API_KEY or not BASE_ID:
    raise RuntimeError("Missing AIRTABLE_API_KEY or Base ID envs")

# === Airtable Clients ===
convos     = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
leads      = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
prospects  = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE)
campaigns  = Table(AIRTABLE_API_KEY, BASE_ID, CAMPAIGNS_TABLE)
templates  = Table(AIRTABLE_API_KEY, BASE_ID, TEMPLATES_TABLE)

# === FIELD MAPPINGS ===
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# === HELPERS ===
def iso_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def _digits(s): 
    return "".join(re.findall(r"\d+", s or "")) if isinstance(s, str) else ""

def _last10(s):
    d = _digits(s)
    return d[-10:] if len(d) >= 10 else None

def _safe_update(tbl, rec_id: str, payload: dict):
    body = {k: v for k, v in (payload or {}).items() if v not in (None, "", [], {})}
    if not body:
        return
    try:
        tbl.update(rec_id, body)
    except Exception as e:
        print(f"⚠️ Update failed for {rec_id}: {e}")

def _find_one_by_field(tbl: Table, field: str, value: str):
    try:
        if not value:
            return None
        rows = tbl.all(formula=f"{{{field}}}='{value}'", max_records=1) or []
        return rows[0] if rows else None
    except Exception:
        return None

def _find_by_phone_last10(tbl, phone):
    """Find a record by matching last 10 digits of any phone-like field."""
    if not tbl or not phone:
        return None
    want = _last10(phone)
    if not want:
        return None
    try:
        for r in tbl.all():
            f = r.get("fields", {})
            for key in ("phone","Phone","Mobile","Cell","Primary Phone","Owner Phone"):
                if _last10(f.get(key)) == want:
                    return r
    except Exception:
        traceback.print_exc()
    return None

def _upsert_conversation_by_msgid(msg_id: str, payload: dict):
    """Create/update Conversation idempotently using MessageSid or unique TextGrid ID."""
    if not msg_id:
        return convos.create(payload)
    existing = _find_one_by_field(convos, TG_ID_FIELD, msg_id)
    if existing:
        _safe_update(convos, existing["id"], payload)
        return existing
    return convos.create(payload)


# === MAIN OUTBOUND HANDLER ===
@router.post("/outbound")
async def _extract_payload(request: Request) -> Dict[str, Any]:
    try:
        body = await request.json()
        if isinstance(body, dict):
            return body
    except Exception:
        pass
    form = await request.form()
    return dict(form)


async def outbound_handler(request: Request):
    """
    Logs outbound SMS sends → Conversations.
    Expected payload fields:
      {
        "To": "+19043334444",
        "From": "+19045556666",
        "Body": "Hey! Here’s your offer link.",
        "MessageSid": "SMxxxxxxxx",
        "Campaign ID": "CAMP-123",
        "Template ID": "TEMP-456"
      }
    """
    try:
        data = await _extract_payload(request)
        to_number = normalize_e164(data.get("To"), field="To")
        from_number = normalize_e164(data.get("From"), field="From")
        body = sanitize_body(data.get("Body"))
        msg_id = (data.get("MessageSid") or data.get("TextGridId") or "").strip()
        campaign_id = data.get("Campaign ID") or data.get("campaign_id")
        template_id = data.get("Template ID") or data.get("template_id")
        processed_by = data.get("ProcessedBy") or data.get("processed_by") or "Outbound Webhook"

        if not msg_id:
            raise HTTPException(status_code=422, detail="Missing required field: MessageSid")

        print(f"📤 Outbound SMS to {to_number}: {body[:60]}...")

        # --- link by phone ---
        lead = _find_by_phone_last10(leads, to_number)
        prospect = _find_by_phone_last10(prospects, to_number)
        lead_id = lead["id"] if lead else None
        prospect_id = prospect["id"] if prospect else None

        # --- link Campaign & Template if IDs provided ---
        camp_rec, tmpl_rec = None, None
        if campaign_id:
            camp_rec = _find_one_by_field(campaigns, "Campaign ID", campaign_id)
        if not camp_rec and campaign_id:
            camp_rec = _find_one_by_field(campaigns, "Campaign Name", campaign_id)
        if template_id:
            tmpl_rec = _find_one_by_field(templates, "Template ID", template_id)
        if not tmpl_rec and template_id:
            tmpl_rec = _find_one_by_field(templates, "Message", template_id)

        # --- build payload ---
        payload = {
            FROM_FIELD: from_number,
            TO_FIELD: to_number,
            MSG_FIELD: body[:10000],
            STATUS_FIELD: "SENT",
            DIR_FIELD: "OUTBOUND",
            TG_ID_FIELD: msg_id,
            SENT_AT: iso_timestamp(),
            PROCESSED_BY: processed_by,
        }

        if lead_id:
            payload["Lead"] = [lead_id]
            payload["Lead Record ID"] = lead_id
        if prospect_id:
            payload["Prospect"] = [prospect_id]
            payload["Prospect Record ID"] = prospect_id
        if camp_rec:
            payload["Campaign"] = [camp_rec["id"]]
            payload["Campaign Record ID"] = camp_rec["id"]
        if tmpl_rec:
            payload["Template"] = [tmpl_rec["id"]]
            payload["Template Record ID"] = tmpl_rec["id"]

        # === UPSERT ===
        record = _upsert_conversation_by_msgid(msg_id, payload)

        # === Lead Activity Update ===
        if lead_id:
            try:
                lf = lead["fields"]
                reply_count = lf.get("Reply Count", 0)
                updates = {
                    "Last Activity": iso_timestamp(),
                    "Last Outbound": iso_timestamp(),
                    "Last Direction": "OUT",
                    "Last Message": (body or "")[:500],
                    "Reply Count": reply_count,  # no increment
                }
                _safe_update(leads, lead_id, updates)
            except Exception as e:
                print(f"⚠️ Failed to update Lead {lead_id}: {e}")

        return {
            "status": "ok",
            "conversation_id": record.get("id") if record else None,
            "message_sid": msg_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Outbound webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
=======
<<<<<<< HEAD
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

=======
"""Outbound echo webhook honouring the README2.md schema."""
>>>>>>> origin/codex/enforce-idempotency-and-logging-rules

from __future__ import annotations

<<<<<<< HEAD

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

=======
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

>>>>>>> origin/codex/enforce-idempotency-and-logging-rules
>>>>>>> codex-refactor-test
