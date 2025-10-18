import os, re, traceback
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table

from sms.config import (
    settings,
    CONVERSATION_FIELDS as CF,
    LEAD_FIELDS as LF,
    PHONE_FIELDS,
)
from sms.number_pools import increment_delivered, increment_failed, increment_opt_out

router = APIRouter()

CFG = settings()
AIRTABLE_API_KEY = CFG.AIRTABLE_API_KEY
BASE_ID = CFG.LEADS_CONVOS_BASE

CONVERSATIONS_TABLE = CFG.CONVERSATIONS_TABLE
LEADS_TABLE = CFG.LEADS_TABLE
PROSPECTS_TABLE = CFG.PROSPECTS_TABLE

FROM_FIELD = CF.SELLER_PHONE_NUMBER
TO_FIELD = CF.TEXTGRID_PHONE_NUMBER
MSG_FIELD = CF.MESSAGE_LONG_TEXT
DIR_FIELD = CF.DIRECTION
TG_ID_FIELD = CF.TEXTGRID_ID
RECEIVED_AT = CF.RECEIVED_TIME
LEAD_LINK_FIELD = CF.LEAD_RECORD_ID
STATUS_FIELD = CF.DELIVERY_STATUS
PROCESSED_BY_FIELD = CF.PROCESSED_BY

# === AIRTABLE CLIENTS ===
convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None

# === HELPERS ===
PHONE_CANDIDATES = PHONE_FIELDS

STOP_TERMS = {"stop", "unsubscribe", "remove", "opt out", "opt-out", "optout", "quit"}


def normalize_e164(value: str, *, field: str) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        raise HTTPException(status_code=422, detail=f"Missing required field: {field}")
    if len(digits) == 10:
        digits = "1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    raise HTTPException(status_code=422, detail=f"Invalid phone number for {field}")


def sanitize_body(value: str, *, field: str = "Body") -> str:
    if value is None:
        raise HTTPException(status_code=422, detail=f"Missing required field: {field}")
    text = str(value).strip()
    if not text:
        raise HTTPException(status_code=422, detail=f"Missing required field: {field}")
    return text


def is_stop_message(body: str) -> bool:
    folded = " ".join(body.lower().split())
    return any(term in folded for term in STOP_TERMS)

def iso_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def _digits(s):
    return "".join(re.findall(r"\d+", s or "")) if isinstance(s, str) else ""

def _last10(s):
    d = _digits(s)
    return d[-10:] if len(d) >= 10 else None

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

# === PROMOTE PROSPECT → LEAD ===
def promote_prospect_to_lead(phone_number: str, source="Inbound"):
    if not phone_number or not leads:
        return None, None
    try:
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            return existing["id"], existing["fields"].get("Property ID")

        fields, property_id = {}, None
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
            LF.PHONE: phone_number,
            LF.LEAD_STATUS: "ACTIVE COMMUNICATION",
            "Source": source,
            LF.REPLY_COUNT: 0,
            LF.LAST_INBOUND: iso_timestamp(),
        })
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id

    except Exception as e:
        print(f"⚠️ Prospect promotion failed for {phone_number}: {e}")
        return None, None

# === ACTIVITY UPDATES ===
def update_lead_activity(lead_id: str, body: str, direction: str, reply_increment: bool = False):
    if not leads or not lead_id:
        return
    try:
        lead = leads.get(lead_id)
        reply_count = lead["fields"].get(LF.REPLY_COUNT, 0)
        updates = {
            LF.LAST_ACTIVITY: iso_timestamp(),
            LF.LAST_DIRECTION: direction,
            LF.LAST_MESSAGE: (body or "")[:500],
        }
        if reply_increment:
            updates[LF.REPLY_COUNT] = reply_count + 1
            updates[LF.LAST_INBOUND] = iso_timestamp()
        if direction == "OUTBOUND":
            updates[LF.LAST_OUTBOUND] = iso_timestamp()
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
    return None

# === TESTABLE HANDLER (used by CI) ===
def handle_inbound(payload: dict):
    """Non-async inbound handler used by tests."""
    from_number = normalize_e164(payload.get("From"), field="From")
    to_number_raw = payload.get("To")
    to_number = normalize_e164(to_number_raw, field="To") if to_number_raw else None
    body = sanitize_body(payload.get("Body"))
    msg_id = str(payload.get("MessageSid") or payload.get("TextGridId") or "").strip() or None

    print(f"📥 [TEST] Inbound SMS from {from_number}: {body}")

    lead_id, property_id = promote_prospect_to_lead(from_number)
    linked_to = "lead" if lead_id else "prospect"

    record = {
        FROM_FIELD: from_number,
        MSG_FIELD: body,
        DIR_FIELD: "INBOUND",
        TG_ID_FIELD: msg_id,
        RECEIVED_AT: iso_timestamp(),
    }
    if to_number:
        record[TO_FIELD] = to_number
    if property_id:
        record["Property ID"] = property_id
    if lead_id and LEAD_LINK_FIELD:
        record[LEAD_LINK_FIELD] = [lead_id]

    is_stop = is_stop_message(body)
    if is_stop:
        record[STATUS_FIELD] = "OPT OUT"
        increment_opt_out(from_number)

    logged = log_conversation(record) or {}
    if lead_id:
        update_lead_activity(lead_id, body, "INBOUND", reply_increment=True)

    status = "optout" if is_stop else "ok"
    return {
        "status": status,
        "conversation_id": logged.get("id"),
        "linked_to": linked_to,
        "message_sid": msg_id,
    }

# === TESTABLE OPTOUT HANDLER ===
def process_optout(payload: dict):
    """Handles STOP/unsubscribe messages for tests + webhook."""
<<<<<<< HEAD
    from_number = payload.get("From")
    raw_body = payload.get("Body")
    if raw_body is None:
        raw_body = ""
    body = str(raw_body)

    if not from_number or not body:
        raise HTTPException(status_code=400, detail="Missing From or Body")

    body_lower = body.lower()

    if "stop" in body_lower or "unsubscribe" in body_lower or "quit" in body_lower:
        print(f"🚫 [TEST] Opt-out from {from_number}")
        increment_opt_out(from_number)
        lead_id, property_id = promote_prospect_to_lead(from_number, source="Opt-Out")
        if lead_id:
            update_lead_activity(lead_id, body, "INBOUND")

        record = {
            FROM_FIELD: from_number,
            MSG_FIELD: body,
            STATUS_FIELD: "OPT OUT",
            DIR_FIELD: "INBOUND",
            RECEIVED_AT: iso_timestamp(),
            PROCESSED_BY_FIELD: "Opt-Out Processor",
        }
        if lead_id and LEAD_LINK_FIELD:
            record[LEAD_LINK_FIELD] = [lead_id]
        if property_id:
            record["Property ID"] = property_id

        log_conversation(record)
        return {"status": "optout"}

    return {"status": "ignored"}
=======
    result = handle_inbound(payload)
    if result.get("status") != "optout":
        raise HTTPException(status_code=422, detail="Payload is not an opt-out message")
    return result
>>>>>>> origin/main

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

    if status == "delivered":
        increment_delivered(from_num)
    elif status in ("failed", "undelivered"):
        increment_failed(from_num)

    # Match the test's expectation: include ok=True
    return {"ok": True, "status": status or "unknown"}

# === FASTAPI ROUTES ===
@router.post("/inbound")
async def inbound_handler(request: Request):
    try:
        data = await request.form()
        return handle_inbound(data)
    except Exception as e:
        print("❌ Inbound webhook error:")
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

@router.post("/optout")
async def optout_handler(request: Request):
    try:
        data = await request.form()
        return process_optout(data)
    except Exception as e:
        print("❌ Opt-out webhook error:")
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

@router.post("/status")
async def status_handler(request: Request):
    try:
        data = await request.form()
        return process_status(data)
    except Exception as e:
        print("❌ Status webhook error:")
        traceback.print_exc()
        return {"ok": False, "error": str(e)}
