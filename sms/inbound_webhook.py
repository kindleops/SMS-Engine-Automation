import os, re, traceback
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table

from sms.number_pools import increment_delivered, increment_failed, increment_opt_out
from sms.config import (
    CONV_FIELDS,
    CONVERSATIONS_FIELDS,
    LEAD_FIELDS,
    LEADS_FIELDS,
    PROSPECT_FIELD_MAP as PROSPECT_FIELDS,
)
from sms.airtable_schema import (
    ConversationDirection,
    ConversationDeliveryStatus,
    ConversationProcessor,
    ConversationIntent,
    LeadStatus,
)

router = APIRouter()

# === ENV CONFIG ===
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")

<<<<<<< HEAD
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
=======
# --- Field Mappings ---
FROM_FIELD = CONV_FIELDS["FROM"]
TO_FIELD = CONV_FIELDS["TO"]
MSG_FIELD = CONV_FIELDS["BODY"]
STATUS_FIELD = CONV_FIELDS["STATUS"]
DIR_FIELD = CONV_FIELDS["DIRECTION"]
TG_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
RECEIVED_AT = CONV_FIELDS["RECEIVED_AT"]
SENT_AT = CONV_FIELDS["SENT_AT"]
PROCESSED_BY = CONV_FIELDS["PROCESSED_BY"]
INTENT_FIELD = CONV_FIELDS.get("INTENT", "Intent Detected")
AI_INTENT_FIELD = CONV_FIELDS.get("AI_INTENT", "AI Intent")
STAGE_FIELD = CONV_FIELDS.get("STAGE", "Stage")
LEAD_LINK_FIELD = CONVERSATIONS_FIELDS.get("LEAD_LINK", "Lead")
PROPERTY_ID_FIELD = CONVERSATIONS_FIELDS.get("PROPERTY_ID", "Property Record ID")

LEAD_PHONE_FIELD = LEAD_FIELDS["PHONE"]
LEAD_STATUS_FIELD = LEAD_FIELDS["STATUS"]
LEAD_LAST_DIRECTION_FIELD = LEAD_FIELDS["LAST_DIRECTION"]
LEAD_LAST_DELIVERY_STATUS_FIELD = LEAD_FIELDS["LAST_DELIVERY_STATUS"]
LEAD_REPLY_COUNT_FIELD = LEAD_FIELDS["REPLY_COUNT"]
LEAD_SENT_COUNT_FIELD = LEAD_FIELDS["SENT_COUNT"]
LEAD_FAILED_COUNT_FIELD = LEAD_FIELDS["FAILED_COUNT"]
LEAD_DELIVERED_COUNT_FIELD = LEAD_FIELDS["DELIVERED_COUNT"]
LEAD_LAST_MESSAGE_FIELD = LEAD_FIELDS["LAST_MESSAGE"]
LEAD_LAST_ACTIVITY_FIELD = LEAD_FIELDS["LAST_ACTIVITY"]
LEAD_LAST_OUTBOUND_FIELD = LEAD_FIELDS["LAST_OUTBOUND"]
LEAD_LAST_INBOUND_FIELD = LEAD_FIELDS["LAST_INBOUND"]
LEAD_RECORD_ID_FIELD = LEAD_FIELDS["RECORD_ID"]
LEAD_PROPERTY_ID_FIELD = LEAD_FIELDS["PROPERTY_ID"]

PROSPECT_PHONE_FIELD = PROSPECT_FIELDS["PHONE_PRIMARY"]
PROSPECT_PROPERTY_ID_FIELD = PROSPECT_FIELDS["PROPERTY_ID"]
PROSPECT_OWNER_NAME_FIELD = PROSPECT_FIELDS["OWNER_NAME"]
PROSPECT_OWNER_FIRST_FIELD = PROSPECT_FIELDS["OWNER_FIRST_NAME"]
PROSPECT_OWNER_LAST_FIELD = PROSPECT_FIELDS["OWNER_LAST_NAME"]
PROSPECT_ADDRESS_FIELD = PROSPECT_FIELDS["PROPERTY_ADDRESS"]
PROSPECT_MARKET_FIELD = PROSPECT_FIELDS["MARKET"]
PROSPECT_SYNC_SOURCE_FIELD = PROSPECT_FIELDS["SYNC_SOURCE"]
PROSPECT_SOURCE_LIST_FIELD = PROSPECT_FIELDS["SOURCE_LIST"]
PROSPECT_PROPERTY_TYPE_FIELD = PROSPECT_FIELDS["PROPERTY_TYPE"]
>>>>>>> codex-refactor-test

# === AIRTABLE CLIENTS ===
convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None

<<<<<<< HEAD
# === HELPERS ===
PHONE_CANDIDATES = [
    "phone", "Phone", "Mobile", "Cell", "Phone Number", "Primary Phone",
    "Phone 1", "Phone 2", "Phone 3",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)", "Phone 3 (from Linked Owner)"
]

STOP_TERMS = {"stop", "unsubscribe", "remove", "opt out", "opt-out", "optout", "quit"}
=======
# --- Field mapping (Prospects → Leads) ---
FIELD_MAP = {
    PROSPECT_PHONE_FIELD: LEAD_PHONE_FIELD,
    PROSPECT_PROPERTY_ID_FIELD: LEAD_PROPERTY_ID_FIELD,
    PROSPECT_OWNER_NAME_FIELD: LEADS_FIELDS.get("OWNER_NAME", "Owner Name"),
    PROSPECT_ADDRESS_FIELD: LEADS_FIELDS.get("ADDRESS", "Address"),
    PROSPECT_MARKET_FIELD: LEADS_FIELDS.get("MARKET", "Market"),
    PROSPECT_SYNC_SOURCE_FIELD: LEADS_FIELDS.get("SYNC_SOURCE", "Synced From"),
    PROSPECT_SOURCE_LIST_FIELD: LEADS_FIELDS.get("LIST", "Source List"),
    PROSPECT_PROPERTY_TYPE_FIELD: LEADS_FIELDS.get("PROPERTY_TYPE", "Property Type"),
}
>>>>>>> codex-refactor-test


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
<<<<<<< HEAD
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            return existing["id"], existing["fields"].get("Property ID")
=======
        # Already a Lead?
        existing = leads.all(formula=f"{{{LEAD_PHONE_FIELD}}}='{phone_number}'")
        if existing:
            lead = existing[0]
            return lead["id"], lead["fields"].get(LEAD_PROPERTY_ID_FIELD)
>>>>>>> codex-refactor-test

        fields, property_id = {}, None
<<<<<<< HEAD
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
=======
        prospect = prospects.all(formula=f"{{{PROSPECT_PHONE_FIELD}}}='{phone_number}'") if prospects else []
        if prospect:
            p_fields = prospect[0]["fields"]
            fields = {leads_col: p_fields.get(prospects_col) for prospects_col, leads_col in FIELD_MAP.items()}
            property_id = p_fields.get(PROSPECT_PROPERTY_ID_FIELD)

        # Create new Lead
        new_lead = leads.create(
            {
                **fields,
                LEAD_PHONE_FIELD: phone_number,
                LEAD_STATUS_FIELD: LeadStatus.NEW.value,
                LEAD_FIELDS.get("SOURCE", "Source"): source,
                LEAD_REPLY_COUNT_FIELD: 0,
                LEAD_LAST_INBOUND_FIELD: iso_timestamp(),
            }
        )
>>>>>>> codex-refactor-test
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
        reply_count = lead["fields"].get(LEAD_REPLY_COUNT_FIELD, 0)
        updates = {
            LEAD_LAST_ACTIVITY_FIELD: iso_timestamp(),
            LEAD_LAST_DIRECTION_FIELD: direction,
            LEAD_LAST_MESSAGE_FIELD: (body or "")[:500],
        }
        if reply_increment:
<<<<<<< HEAD
            updates["Reply Count"] = reply_count + 1
            updates["Last Inbound"] = iso_timestamp()
        if direction == "OUT":
            updates["Last Outbound"] = iso_timestamp()
=======
            updates[LEAD_REPLY_COUNT_FIELD] = reply_count + 1
            updates[LEAD_LAST_INBOUND_FIELD] = iso_timestamp()
        if direction == ConversationDirection.OUTBOUND.value:
            updates[LEAD_LAST_OUTBOUND_FIELD] = iso_timestamp()

>>>>>>> codex-refactor-test
        leads.update(lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")

def log_conversation(payload: dict):
    if not convos:
        return None
    try:
<<<<<<< HEAD
        return convos.create(payload)
=======
        convos.create(payload)
    except Exception as log_err:
        print(f"⚠️ Failed to log to Conversations: {log_err}")


def _map_delivery_status(provider_status: str | None) -> str:
    """
    Map provider delivery statuses to the single-select values available in Airtable.
    Falls back to an uppercase string if the provider introduces a new status.
    """
    if not provider_status:
        return ConversationDeliveryStatus.SENT.value

    status_map = {
        "queued": ConversationDeliveryStatus.QUEUED.value,
        "accepted": ConversationDeliveryStatus.QUEUED.value,
        "sending": ConversationDeliveryStatus.SENT.value,
        "sent": ConversationDeliveryStatus.SENT.value,
        "delivered": ConversationDeliveryStatus.DELIVERED.value,
        "failed": ConversationDeliveryStatus.FAILED.value,
        "undelivered": ConversationDeliveryStatus.UNDELIVERED.value,
        "optout": ConversationDeliveryStatus.OPT_OUT.value,
        "opt-out": ConversationDeliveryStatus.OPT_OUT.value,
    }
    return status_map.get(provider_status.lower(), provider_status.upper())


# --- Inbound SMS ---
@router.post("/inbound")
async def inbound_handler(request: Request):
    try:
        data = await request.form()
        from_number = data.get("From")
        to_number = data.get("To")
        body = data.get("Body")
        msg_id = data.get("MessageSid")

        if not from_number or not body:
            raise HTTPException(status_code=400, detail="Missing From or Body")

        print(f"📥 Inbound SMS from {from_number}: {body}")

        lead_id, property_id = promote_prospect_to_lead(from_number)

        payload = {
            FROM_FIELD: from_number,
            TO_FIELD: to_number,
            MSG_FIELD: body,
            STATUS_FIELD: ConversationDeliveryStatus.DELIVERED.value,
            DIR_FIELD: ConversationDirection.INBOUND.value,
            TG_ID_FIELD: msg_id,
            RECEIVED_AT: iso_timestamp(),
            PROCESSED_BY: ConversationProcessor.MANUAL.value,
        }
        if lead_id and LEAD_LINK_FIELD:
            payload[LEAD_LINK_FIELD] = [lead_id]
        if property_id and PROPERTY_ID_FIELD:
            payload[PROPERTY_ID_FIELD] = property_id

        log_conversation(payload)

        if lead_id:
            update_lead_activity(lead_id, body, ConversationDirection.INBOUND.value, reply_increment=True)

        return {"ok": True}

>>>>>>> codex-refactor-test
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

<<<<<<< HEAD
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
=======
            lead_id, property_id = promote_prospect_to_lead(from_number, source="Opt-Out")
            if lead_id:
                update_lead_activity(lead_id, body, ConversationDirection.INBOUND.value)

            payload = {
                FROM_FIELD: from_number,
                MSG_FIELD: body,
                STATUS_FIELD: ConversationDeliveryStatus.OPT_OUT.value,
                DIR_FIELD: ConversationDirection.INBOUND.value,
                RECEIVED_AT: iso_timestamp(),
                PROCESSED_BY: ConversationProcessor.MANUAL.value,
                INTENT_FIELD: ConversationIntent.DNC.value,
            }
            if lead_id and LEAD_LINK_FIELD:
                payload[LEAD_LINK_FIELD] = [lead_id]
            if property_id and PROPERTY_ID_FIELD:
                payload[PROPERTY_ID_FIELD] = property_id
>>>>>>> codex-refactor-test

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
    result = handle_inbound(payload)
    if result.get("status") != "optout":
        raise HTTPException(status_code=422, detail="Payload is not an opt-out message")
    return result

# === TESTABLE STATUS HANDLER ===
def process_status(payload: dict):
    """Testable delivery status handler used by CI and webhook."""
    msg_id = payload.get("MessageSid")
    status = (payload.get("MessageStatus") or "").lower()
    to = payload.get("To")
    from_num = payload.get("From")

<<<<<<< HEAD
    if not to or not from_num:
        raise HTTPException(status_code=400, detail="Missing To or From")

    print(f"📡 [TEST] Delivery receipt for {to} [{status}]")

=======
# --- Delivery Status ---
@router.post("/status")
async def status_handler(request: Request):
    try:
        data = await request.form()
        msg_id = data.get("MessageSid")
        status = data.get("MessageStatus")
        to = data.get("To")
        from_num = data.get("From")

        print(f"📡 Delivery receipt for {to} [{status}]")

        # Update number metrics
        if status == "delivered":
            increment_delivered(from_num)
        elif status in ("failed", "undelivered"):
            increment_failed(from_num)

        mapped_status = _map_delivery_status(status)

        # Update conversation record
        if convos and msg_id:
            try:
                convos.update_by_fields({TG_ID_FIELD: msg_id}, {STATUS_FIELD: mapped_status})
            except Exception as log_err:
                print(f"⚠️ Failed to update delivery status in Conversations: {log_err}")

        # Update lead metrics
        if leads and to:
            try:
                results = leads.all(formula=f"{{{LEAD_PHONE_FIELD}}}='{to}'")
                if results:
                    lead = results[0]
                    lead_id = lead["id"]

                    delivered_count = lead["fields"].get(LEAD_DELIVERED_COUNT_FIELD, 0)
                    failed_count = lead["fields"].get(LEAD_FAILED_COUNT_FIELD, 0)

                    updates = {
                        LEAD_LAST_ACTIVITY_FIELD: iso_timestamp(),
                        LEAD_LAST_DELIVERY_STATUS_FIELD: mapped_status,
                    }
                    if status == "delivered":
                        updates[LEAD_DELIVERED_COUNT_FIELD] = delivered_count + 1
                    elif status in ("failed", "undelivered"):
                        updates[LEAD_FAILED_COUNT_FIELD] = failed_count + 1

                    leads.update(lead_id, updates)
            except Exception as e:
                print(f"⚠️ Failed to update lead delivery metrics: {e}")

        return {"ok": True}

    except Exception as e:
        print("❌ Status webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    # --- Test-friendly wrapper for status ---


def process_status(data: dict):
    """Sync wrapper for testing delivery receipts."""
    msg_id = data.get("MessageSid")
    status = data.get("MessageStatus")
    to = data.get("To")
    from_num = data.get("From")

    if not status or not to:
        raise HTTPException(status_code=400, detail="Missing required fields")

    print(f"📡 [TEST] Delivery receipt for {to} [{status}]")

    mapped_status = _map_delivery_status(status)

    # Update number metrics
>>>>>>> codex-refactor-test
    if status == "delivered":
        increment_delivered(from_num)
    elif status in ("failed", "undelivered"):
        increment_failed(from_num)

<<<<<<< HEAD
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
=======
    # Update conversation record
    if convos and msg_id:
        try:
            convos.update_by_fields({TG_ID_FIELD: msg_id}, {STATUS_FIELD: mapped_status})
        except Exception as log_err:
            print(f"⚠️ Failed to update delivery status in Conversations: {log_err}")

    # Update lead metrics
    if leads and to:
        try:
            results = leads.all(formula=f"{{{LEAD_PHONE_FIELD}}}='{to}'")
            if results:
                lead = results[0]
                lead_id = lead["id"]

                delivered_count = lead["fields"].get(LEAD_DELIVERED_COUNT_FIELD, 0)
                failed_count = lead["fields"].get(LEAD_FAILED_COUNT_FIELD, 0)

                updates = {
                    LEAD_LAST_ACTIVITY_FIELD: iso_timestamp(),
                    LEAD_LAST_DELIVERY_STATUS_FIELD: mapped_status,
                }
                if status == "delivered":
                    updates[LEAD_DELIVERED_COUNT_FIELD] = delivered_count + 1
                elif status in ("failed", "undelivered"):
                    updates[LEAD_FAILED_COUNT_FIELD] = failed_count + 1

                leads.update(lead_id, updates)
        except Exception as e:
            print(f"⚠️ Failed to update lead delivery metrics: {e}")

    return {"ok": True}

    # ------------------------


# Test-friendly wrappers
# ------------------------


def handle_inbound(data: dict):
    """Direct call version of inbound_handler for unit testing."""
    from_number = data.get("From")
    to_number = data.get("To")
    body = data.get("Body")
    msg_id = data.get("MessageSid")

    if not from_number or not body:
        raise HTTPException(status_code=400, detail="Missing From or Body")

    lead_id, property_id = promote_prospect_to_lead(from_number)

    payload = {
        FROM_FIELD: from_number,
        TO_FIELD: to_number,
        MSG_FIELD: body,
        STATUS_FIELD: ConversationDeliveryStatus.DELIVERED.value,
        DIR_FIELD: ConversationDirection.INBOUND.value,
        TG_ID_FIELD: msg_id,
        RECEIVED_AT: iso_timestamp(),
        PROCESSED_BY: ConversationProcessor.MANUAL.value,
    }
    if lead_id and LEAD_LINK_FIELD:
        payload[LEAD_LINK_FIELD] = [lead_id]
    if property_id and PROPERTY_ID_FIELD:
        payload[PROPERTY_ID_FIELD] = property_id

    log_conversation(payload)

    if lead_id:
        update_lead_activity(lead_id, body, ConversationDirection.INBOUND.value, reply_increment=True)

    return {"status": "ok"}


def process_optout(data: dict):
    """Direct call version of optout_handler for unit testing."""
    from_number = data.get("From")
    body = (data.get("Body") or "").lower()

    if "stop" in body or "unsubscribe" in body or "quit" in body:
        increment_opt_out(from_number)

        lead_id, property_id = promote_prospect_to_lead(from_number, source="Opt-Out")
        if lead_id:
            update_lead_activity(lead_id, body, ConversationDirection.INBOUND.value)

        payload = {
            FROM_FIELD: from_number,
            MSG_FIELD: body,
            STATUS_FIELD: ConversationDeliveryStatus.OPT_OUT.value,
            DIR_FIELD: ConversationDirection.INBOUND.value,
            RECEIVED_AT: iso_timestamp(),
            PROCESSED_BY: ConversationProcessor.MANUAL.value,
            INTENT_FIELD: ConversationIntent.DNC.value,
        }
        if lead_id and LEAD_LINK_FIELD:
            payload[LEAD_LINK_FIELD] = [lead_id]
        if property_id and PROPERTY_ID_FIELD:
            payload[PROPERTY_ID_FIELD] = property_id

        log_conversation(payload)

        return {"status": "optout"}

    return {"status": "ignored"}
>>>>>>> codex-refactor-test
