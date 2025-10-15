import os, re, traceback
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
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

# === AIRTABLE CLIENTS ===
convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None

# === HELPERS ===
PHONE_CANDIDATES = [
    "phone", "Phone", "Mobile", "Cell", "Phone Number", "Primary Phone",
    "Phone 1", "Phone 2", "Phone 3",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)", "Phone 3 (from Linked Owner)"
]

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
        return
    try:
        convos.create(payload)
    except Exception as e:
        print(f"⚠️ Failed to log to Conversations: {e}")

# === TESTABLE HANDLER (used by CI) ===
def handle_inbound(payload: dict):
    """Non-async inbound handler used by tests."""
    from_number = payload.get("From")
    to_number = payload.get("To")
    body = payload.get("Body")
    msg_id = payload.get("MessageSid")

    if not from_number or not body:
        raise HTTPException(status_code=400, detail="Missing From or Body")

    print(f"📥 [TEST] Inbound SMS from {from_number}: {body}")

    lead_id, property_id = promote_prospect_to_lead(from_number)
    record = {
        FROM_FIELD: from_number,
        TO_FIELD: to_number,
        MSG_FIELD: body,
        STATUS_FIELD: "UNPROCESSED",
        DIR_FIELD: "IN",
        TG_ID_FIELD: msg_id,
        RECEIVED_AT: iso_timestamp(),
    }
    if lead_id:
        record["lead_id"] = [lead_id]
    if property_id:
        record["Property ID"] = property_id

    log_conversation(record)
    if lead_id:
        update_lead_activity(lead_id, body, "IN", reply_increment=True)

    return {"status": "ok"}

# === TESTABLE OPTOUT HANDLER ===
def process_optout(payload: dict):
    """Handles STOP/unsubscribe messages for tests + webhook."""
    from_number = payload.get("From")
    body = (payload.get("Body") or "").lower()

    if not from_number or not body:
        raise HTTPException(status_code=400, detail="Missing From or Body")

    if "stop" in body or "unsubscribe" in body or "quit" in body:
        print(f"🚫 [TEST] Opt-out from {from_number}")
        increment_opt_out(from_number)
        lead_id, property_id = promote_prospect_to_lead(from_number, source="Opt-Out")
        if lead_id:
            update_lead_activity(lead_id, body, "IN")

        record = {
            FROM_FIELD: from_number,
            MSG_FIELD: body,
            STATUS_FIELD: "OPTOUT",
            DIR_FIELD: "IN",
            RECEIVED_AT: iso_timestamp(),
            PROCESSED_BY: "OptOut Handler",
        }
        if lead_id:
            record["lead_id"] = [lead_id]
        if property_id:
            record["Property ID"] = property_id

        log_conversation(record)
        return {"status": "optout"}

    return {"status": "ignored"}

# === TESTABLE STATUS HANDLER ===
def process_status(payload: dict):
    """Testable delivery status handler used by CI and webhook."""
    msg_id = payload.get("MessageSid")
    status = payload.get("MessageStatus")
    to = payload.get("To")
    from_num = payload.get("From")

    print(f"📡 [TEST] Delivery receipt for {to} [{status}]")

    if status == "delivered":
        increment_delivered(from_num)
    elif status in ("failed", "undelivered"):
        increment_failed(from_num)

    return {"status": status or "unknown"}

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