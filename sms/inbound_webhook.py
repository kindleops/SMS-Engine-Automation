import os
import re
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, HTTPException, Request
from pyairtable import Table

from sms.number_pools import increment_delivered, increment_failed, increment_opt_out

router = APIRouter()

# === ENV CONFIG ===
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

# === FIELD MAPPINGS ===
FROM_FIELD = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
RECEIVED_AT = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
SENT_AT = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")
LEAD_LINK_FIELD = os.getenv("CONV_LEAD_LINK_FIELD", "lead_id")
STAGE_FIELD = os.getenv("CONV_STAGE_FIELD", "Stage")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD", "Intent Detected")
AI_INTENT_FIELD = os.getenv("CONV_AI_INTENT_FIELD", "AI Intent")

# === AIRTABLE CLIENTS ===
convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None

# === HELPERS ===
PHONE_CANDIDATES = [
    "phone",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
    "Phone 1",
    "Phone 2",
    "Phone 3",
    "Owner Phone",
    "Owner Phone 1",
    "Owner Phone 2",
    "Phone 1 (from Linked Owner)",
    "Phone 2 (from Linked Owner)",
    "Phone 3 (from Linked Owner)",
]

STAGE_SEQUENCE = [
    "STAGE 1 - OWNERSHIP CONFIRMATION",
    "STAGE 2 - INTEREST FEELER",
    "STAGE 3 - PRICE QUALIFICATION",
    "STAGE 4 - PROPERTY CONDITION",
    "STAGE 5 - MOTIVATION / TIMELINE",
    "STAGE 6 - OFFER FOLLOW UP",
    "STAGE 7 - CONTRACT READY",
    "STAGE 8 - CONTRACT SENT",
    "STAGE 9 - CONTRACT FOLLOW UP",
]

PROMOTION_INTENTS = {"positive"}
PROMOTION_AI_INTENTS = {"interest_detected", "offer_discussion", "ask_price"}

POSITIVE_KEYWORDS = {
    "yes",
    "interested",
    "offer",
    "ready",
    "let's talk",
    "lets talk",
    "sure",
    "sounds good",
}

PRICE_KEYWORDS = {"price", "ask", "number", "how much", "offer"}
CONTRACT_KEYWORDS = {"contract", "paperwork", "agreement"}
TIMELINE_KEYWORDS = {"timeline", "move", "closing", "close"}

STOP_WORDS = {"stop", "unsubscribe", "remove", "opt out", "quit"}

_SEEN_MESSAGE_IDS: set[str] = set()


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _digits(value: Any) -> str:
    return "".join(re.findall(r"\d+", value or "")) if isinstance(value, str) else ""


def _last10(value: Any) -> Optional[str]:
    digits = _digits(value)
    return digits[-10:] if len(digits) >= 10 else None


def _first_existing_fields(tbl, candidates):
    try:
        probe = tbl.all(max_records=1) or []
        keys = list((probe[0] or {}).get("fields", {}).keys()) if probe else []
    except Exception:
        keys = []
    return [c for c in candidates if c in keys]


def _find_by_phone_last10(tbl, phone):
    """Return first record whose phone-like field matches last10 digits."""
    if not tbl or not phone:
        return None
    want = _last10(phone)
    if not want:
        return None
    fields = _first_existing_fields(tbl, PHONE_CANDIDATES)
    try:
        for r in tbl.all():
            f = r.get("fields", {})
            for col in fields:
                if _last10(f.get(col)) == want:
                    return r
    except Exception:
        traceback.print_exc()
    return None


def _lookup_existing_lead(phone_number: str) -> Tuple[Optional[str], Optional[str]]:
    if not phone_number or not leads:
        return None, None
    try:
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            return existing["id"], existing["fields"].get("Property ID")
    except Exception:
        traceback.print_exc()
    return None, None


def _lookup_prospect_property(phone_number: str) -> Optional[str]:
    if not phone_number or not prospects:
        return None
    try:
        prospect = _find_by_phone_last10(prospects, phone_number)
        if prospect:
            return prospect.get("fields", {}).get("Property ID")
    except Exception:
        traceback.print_exc()
    return None


# === PROMOTE PROSPECT → LEAD ===
def promote_prospect_to_lead(phone_number: str, source: str = "Inbound"):
    if not phone_number or not leads:
        return None, None
    try:
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            return existing["id"], existing["fields"].get("Property ID")

        fields: Dict[str, Any] = {}
        property_id = None
        prospect = _find_by_phone_last10(prospects, phone_number)
        if prospect:
            p_fields = prospect["fields"]
            for p_col, l_col in {
                "phone": "phone",
                "Property ID": "Property ID",
                "Owner Name": "Owner Name",
                "Address": "Address",
                "Market": "Market",
                "Sync Source": "Synced From",
                "List": "Source List",
                "Property Type": "Property Type",
            }.items():
                if p_col in p_fields:
                    fields[l_col] = p_fields[p_col]
            property_id = p_fields.get("Property ID")

        new_lead = leads.create({
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source,
            "Reply Count": 0,
            "Last Inbound": iso_timestamp(),
        })
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id

    except Exception as e:
        print(f"⚠️ Prospect promotion failed for {phone_number}: {e}")
        return None, None


def _normalize_stage(stage: Optional[str]) -> str:
    if not stage:
        return STAGE_SEQUENCE[0]
    stage_upper = str(stage).strip().upper()
    for defined in STAGE_SEQUENCE:
        if stage_upper.startswith(defined):
            return defined
    match = re.search(r"(\d)", stage_upper)
    if match:
        idx = int(match.group(1)) - 1
        if 0 <= idx < len(STAGE_SEQUENCE):
            return STAGE_SEQUENCE[idx]
    return STAGE_SEQUENCE[0]


def _stage_rank(stage: Optional[str]) -> int:
    normalized = _normalize_stage(stage)
    try:
        return STAGE_SEQUENCE.index(normalized) + 1
    except ValueError:
        return 1


def _classify_message(body: str, overrides: Optional[Dict[str, str]] = None) -> Tuple[str, str, str]:
    """Return (stage, intent_detected, ai_intent)."""
    overrides = overrides or {}
    intent_override = overrides.get("intent")
    ai_intent_override = overrides.get("ai_intent")
    stage_override = overrides.get("stage")

    if intent_override or ai_intent_override or stage_override:
        stage = _normalize_stage(stage_override)
        intent = intent_override or ("Positive" if _stage_rank(stage) >= 3 else "Neutral")
        ai_intent = ai_intent_override or ("interest_detected" if intent.lower() == "positive" else "neutral")
        return stage, intent, ai_intent

    text = (body or "").strip().lower()

    stage = STAGE_SEQUENCE[0]
    intent = "Neutral"
    ai_intent = "neutral"

    if any(token in text for token in POSITIVE_KEYWORDS):
        intent = "Positive"
        ai_intent = "interest_detected"
        stage = STAGE_SEQUENCE[2]
    elif any(token in text for token in PRICE_KEYWORDS):
        intent = "Positive"
        ai_intent = "ask_price"
        stage = STAGE_SEQUENCE[2]
    elif any(token in text for token in CONTRACT_KEYWORDS):
        intent = "Positive"
        ai_intent = "offer_discussion"
        stage = STAGE_SEQUENCE[6]
    elif any(token in text for token in TIMELINE_KEYWORDS):
        intent = "Delay"
        ai_intent = "timeline_question"
        stage = STAGE_SEQUENCE[4]

    return stage, intent, ai_intent


def _should_promote(intent: str, ai_intent: str, stage: str) -> bool:
    if intent.lower() in PROMOTION_INTENTS:
        return True
    if ai_intent in PROMOTION_AI_INTENTS:
        return True
    return _stage_rank(stage) >= 3


def _is_opt_out(body: str) -> bool:
    body_lower = (body or "").lower()
    return any(token in body_lower for token in STOP_WORDS)


# === ACTIVITY UPDATES ===
def update_lead_activity(lead_id: str, body: str, direction: str, reply_increment: bool = False):
    if not leads or not lead_id:
        return
    try:
        lead = leads.get(lead_id)
        reply_count = lead["fields"].get("Reply Count", 0)
        updates = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        if reply_increment:
            updates["Reply Count"] = reply_count + 1
            updates["Last Inbound"] = iso_timestamp()
        if direction == "OUT":
            updates["Last Outbound"] = iso_timestamp()
        leads.update(lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


def log_conversation(payload: dict):
    if not convos:
        return None
    try:
        return convos.create(payload)
    except Exception as e:
        print(f"⚠️ Failed to log to Conversations: {e}")


# === TESTABLE HANDLER (used by CI) ===
def handle_inbound(payload: dict):
    """Non-async inbound handler used by tests."""
    from_number = payload.get("From")
    to_number = payload.get("To")
    body = payload.get("Body")
    msg_id = payload.get("MessageSid") or payload.get("TextGridId")

    if not from_number or not body:
        raise HTTPException(status_code=422, detail="Missing From or Body")

    if _is_opt_out(body):
        return process_optout(payload)

    if msg_id and msg_id in _SEEN_MESSAGE_IDS:
        return {"status": "duplicate"}

    overrides: Dict[str, str] = {}
    for key in ("Intent", "Intent Detected", "intent"):
        if payload.get(key):
            overrides["intent"] = str(payload[key])
            break
    for key in ("AI Intent", "AiIntent", "ai_intent"):
        if payload.get(key):
            overrides["ai_intent"] = str(payload[key])
            break
    for key in ("Stage", "stage"):
        if payload.get(key):
            overrides["stage"] = str(payload[key])
            break

    stage, intent, ai_intent = _classify_message(body, overrides)

    lead_id, property_id = _lookup_existing_lead(from_number)
    promoted = False
    if not lead_id and _should_promote(intent, ai_intent, stage):
        lead_id, property_id = promote_prospect_to_lead(from_number)
        promoted = bool(lead_id)
    elif lead_id:
        promoted = _should_promote(intent, ai_intent, stage)

    if msg_id:
        _SEEN_MESSAGE_IDS.add(msg_id)

    record = {
        FROM_FIELD: from_number,
        MSG_FIELD: body,
        DIR_FIELD: "INBOUND",
        TG_ID_FIELD: msg_id,
        RECEIVED_AT: iso_timestamp(),
    }
    record[STAGE_FIELD] = stage
    record[INTENT_FIELD] = intent
    record[AI_INTENT_FIELD] = ai_intent

    if lead_id and LEAD_LINK_FIELD:
        record[LEAD_LINK_FIELD] = [lead_id]
    else:
        property_id = property_id or _lookup_prospect_property(from_number)

    if property_id:
        record["Property ID"] = property_id

    logged = log_conversation(record) or {}
    if lead_id:
        update_lead_activity(lead_id, body, "INBOUND", reply_increment=True)

    return {"status": "ok", "stage": stage, "intent": intent, "promoted": promoted}


# === TESTABLE OPTOUT HANDLER ===
def process_optout(payload: dict):
    """Handles STOP/unsubscribe messages for tests + webhook."""
    from_number = payload.get("From")
    raw_body = payload.get("Body")
    msg_id = payload.get("MessageSid") or payload.get("TextGridId")
    body = "" if raw_body is None else str(raw_body)

    if not from_number or not body:
        raise HTTPException(status_code=422, detail="Missing From or Body")

    if not _is_opt_out(body):
        return {"status": "ignored"}

    if msg_id and msg_id in _SEEN_MESSAGE_IDS:
        return {"status": "duplicate"}
    if msg_id:
        _SEEN_MESSAGE_IDS.add(msg_id)

    print(f"🚫 [TEST] Opt-out from {from_number}")
    increment_opt_out(from_number)

    lead_id, property_id = _lookup_existing_lead(from_number)
    if not lead_id:
        property_id = property_id or _lookup_prospect_property(from_number)

    record = {
        FROM_FIELD: from_number,
        MSG_FIELD: body,
        STATUS_FIELD: "OPT OUT",
        DIR_FIELD: "IN",
        TG_ID_FIELD: msg_id,
        RECEIVED_AT: iso_timestamp(),
        PROCESSED_BY: "OptOut Handler",
        STAGE_FIELD: "OPT OUT",
        INTENT_FIELD: "DNC",
        AI_INTENT_FIELD: "not_interested",
    }

    if lead_id and LEAD_LINK_FIELD:
        record[LEAD_LINK_FIELD] = [lead_id]
    if property_id:
        record["Property ID"] = property_id

    log_conversation(record)
    if lead_id:
        update_lead_activity(lead_id, body, "IN")

    return {"status": "optout"}


# === TESTABLE STATUS HANDLER ===
def process_status(payload: dict):
    """Testable delivery status handler used by CI and webhook."""
    msg_id = payload.get("MessageSid")
    status = (payload.get("MessageStatus") or "").lower()
    to = payload.get("To")
    from_num = payload.get("From")

    if not to or not from_num:
        raise HTTPException(status_code=400, detail="Missing To or From")

    print(f"📡 [TEST] Delivery receipt for {to} [{status}]")

    if not to or not from_num:
        raise HTTPException(status_code=422, detail="Missing To or From")

    if status == "delivered":
        increment_delivered(from_num)
    elif status in ("failed", "undelivered"):
        increment_failed(from_num)

    return {"ok": True, "status": status or "unknown"}


# === FASTAPI ROUTES ===
@router.post("/inbound")
async def inbound_handler(request: Request):
    try:
        data = await request.form()
        return handle_inbound(dict(data))
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Inbound webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optout")
async def optout_handler(request: Request):
    try:
        data = await request.form()
        return process_optout(dict(data))
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Opt-out webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/status")
async def status_handler(request: Request):
    try:
        data = await request.form()
        return process_status(dict(data))
    except HTTPException:
        raise
    except Exception as e:
        print("❌ Status webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
