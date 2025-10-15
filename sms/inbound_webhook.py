import os, re, traceback
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table

from sms.number_pools import increment_delivered, increment_failed, increment_opt_out

router = APIRouter()

def process_optout(payload: dict):
    """
    Testable (non-async) version of the opt-out webhook.
    Mirrors the behavior of /optout route.
    """
    from_number = payload.get("From")
    body = (payload.get("Body") or "").lower()

    if not from_number or not body:
        raise ValueError("Missing From or Body")

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
        return {"status": "ok"}

    # If not opt-out keyword
    return {"status": "ignored"}

def handle_inbound(payload: dict):
    """
    Non-async testable version of the inbound webhook handler.
    Used by unit tests; mirrors inbound_handler() FastAPI route.
    """
    from_number = payload.get("From")
    to_number = payload.get("To")
    body = payload.get("Body")
    msg_id = payload.get("MessageSid")

    if not from_number or not body:
        raise ValueError("Missing From or Body")

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

# --- ENV CONFIG ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE         = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE     = os.getenv("PROSPECTS_TABLE", "Prospects")

# --- Field Mappings (Conversations schema) ---
FROM_FIELD     = os.getenv("CONV_FROM_FIELD", "phone")           # inbound sender
TO_FIELD       = os.getenv("CONV_TO_FIELD", "to_number")         # our number
MSG_FIELD      = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD   = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD      = os.getenv("CONV_DIRECTION_FIELD", "direction")  # values: IN / OUT
TG_ID_FIELD    = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
RECEIVED_AT    = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
SENT_AT        = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY   = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# --- Airtable clients ---
if not AIRTABLE_API_KEY or not BASE_ID:
    raise RuntimeError("Missing AIRTABLE_API_KEY or Base ID envs")

convos    = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
leads     = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
prospects = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE)

# --- Utils ---
PHONE_CANDIDATES = [
    "phone","Phone","Mobile","Cell","Phone Number","Primary Phone",
    "Phone 1","Phone 2","Phone 3",
    "Owner Phone","Owner Phone 1","Owner Phone 2",
    "Phone 1 (from Linked Owner)","Phone 2 (from Linked Owner)","Phone 3 (from Linked Owner)",
]

def iso_timestamp():
    # RFC3339 UTC with Z, second precision (Airtable is happy with this)
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
    """Return first record whose phone-like field matches last10 digits (scans with a small field list)."""
    if not tbl or not phone:
        return None
    want = _last10(phone)
    if not want:
        return None
    fields = _first_existing_fields(tbl, PHONE_CANDIDATES)
    try:
        # Iterate all (SDK paginates)
        for r in tbl.all():
            f = r.get("fields", {}) or {}
            for col in fields:
                if _last10(f.get(col)) == want:
                    return r
    except Exception:
        traceback.print_exc()
    return None

def _find_one_by_field(tbl: Table, field: str, value: str):
    """Efficient single-row fetch using filterByFormula."""
    try:
        rows = tbl.all(formula=f"{{{field}}}='{value}'", max_records=1) or []
        return rows[0] if rows else None
    except Exception:
        return None

def _safe_update(tbl: Table, rec_id: str, payload: dict):
    body = {k: v for k, v in (payload or {}).items() if v not in (None, "", [], {})}
    if not body:
        return
    try:
        tbl.update(rec_id, body)
    except Exception as e:
        print(f"⚠️ Update failed for {rec_id}: {e}")

def log_conversation(payload: dict):
    """Create a Conversations row; kept as a top-level function so tests can monkeypatch it."""
    if not convos:
        return
    try:
        convos.create(payload)
    except Exception as e:
        # Don’t raise here; just log so inbound path is resilient.
        print(f"⚠️ Failed to log to Conversations: {e}")

# --- Promotion: Prospect → Lead ---
def promote_prospect_to_lead(phone_number: str, source="Inbound"):
    """Makes sure a Lead exists for this phone; returns (lead_id, property_id, prospect_id)."""
    if not phone_number:
        return None, None, None
    try:
        # Already a Lead?
        existing = _find_by_phone_last10(leads, phone_number)
        if existing:
            lf = existing.get("fields", {})
            return existing["id"], lf.get("Property ID"), None

        # Prospect match?
        fields, property_id, prospect_id = {}, None, None
        prospect = _find_by_phone_last10(prospects, phone_number)
        if prospect:
            prospect_id = prospect["id"]
            p = prospect["fields"]
            # map selected fields forward
            mapping = {
                "phone": "phone",
                "Property ID": "Property ID",
                "Owner Name": "Owner Name",
                "Address": "Address",
                "Market": "Market",
                "Sync Source": "Synced From",
                "List": "Source List",
                "Property Type": "Property Type",
            }
            for src, dst in mapping.items():
                if src in p:
                    fields[dst] = p[src]
            property_id = p.get("Property ID")

        # Create Lead
        new_lead = leads.create({
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source,
            "Reply Count": 0,
            "Last Inbound": iso_timestamp(),
        })
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id, prospect_id

    except Exception as e:
        print(f"⚠️ Prospect promotion failed for {phone_number}: {e}")
        return None, None, None

# --- Activity updates on Lead ---
def update_lead_activity(lead_id: str, body: str, direction: str, reply_increment: bool = False):
    if not lead_id:
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
        _safe_update(leads, lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")

def _upsert_conversation_by_msgid(msg_id: str, payload: dict):
    """
    Idempotent create/update by provider message id.
    If msg_id exists, updates that row; otherwise creates a new one.
    """
    if not msg_id:
        # fall back to straight create
        return convos.create(payload)

    existing = _find_one_by_field(convos, TG_ID_FIELD, msg_id)
    if existing:
        _safe_update(convos, existing["id"], payload)
        return existing
    return convos.create(payload)

# --- Inbound Webhook ---
@router.post("/inbound")
async def inbound_handler(request: Request):
    try:
        data = await request.form()
        from_number = data.get("From")
        to_number   = data.get("To")
        body        = (data.get("Body") or "").strip()
        msg_id      = data.get("MessageSid")

        if not from_number or not body:
            raise HTTPException(status_code=400, detail="Missing From or Body")

        print(f"📥 Inbound SMS from {from_number}: {body}")

        lead_id, property_id, prospect_id = promote_prospect_to_lead(from_number)

        # Base payload for Conversations
        payload = {
            FROM_FIELD: from_number,
            TO_FIELD: to_number,
            MSG_FIELD: body[:10000],  # guardrail
            STATUS_FIELD: "UNPROCESSED",
            DIR_FIELD: "IN",
            TG_ID_FIELD: msg_id,
            RECEIVED_AT: iso_timestamp(),
        }

        # Proper links + mirror text fields
        if lead_id:
            payload["Lead"] = [lead_id]
            payload["Lead Record ID"] = lead_id
        if prospect_id:
            payload["Prospect"] = [prospect_id]
            payload["Prospect Record ID"] = prospect_id
        if property_id:
            payload["Property ID"] = property_id

        # Idempotent upsert (prevents dupes on provider retries)
        _upsert_conversation_by_msgid(msg_id, payload)

        if lead_id:
            update_lead_activity(lead_id, body, "IN", reply_increment=True)

        return {"ok": True}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Inbound webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# --- Opt-Out Webhook ---
@router.post("/optout")
async def optout_handler(request: Request):
    try:
        data = await request.form()
        from_number = data.get("From")
        body_lower  = (data.get("Body") or "").lower()
        msg_id      = data.get("MessageSid")  # some providers send it here too

        if not from_number:
            raise HTTPException(status_code=400, detail="Missing From")

        if any(x in body_lower for x in ("stop","unsubscribe","quit","stopall","remove","opt out")):
            print(f"🚫 Opt-out from {from_number}")
            increment_opt_out(from_number)

            lead_id, property_id, prospect_id = promote_prospect_to_lead(from_number, source="Opt-Out")
            payload = {
                FROM_FIELD: from_number,
                MSG_FIELD: body_lower,
                STATUS_FIELD: "OPTOUT",
                DIR_FIELD: "IN",
                RECEIVED_AT: iso_timestamp(),
                PROCESSED_BY: "OptOut Handler",
                TG_ID_FIELD: msg_id,
            }
            if lead_id:
                payload["Lead"] = [lead_id]
                payload["Lead Record ID"] = lead_id
            if prospect_id:
                payload["Prospect"] = [prospect_id]
                payload["Prospect Record ID"] = prospect_id
            if property_id:
                payload["Property ID"] = property_id

            _upsert_conversation_by_msgid(msg_id, payload)

            if lead_id:
                update_lead_activity(lead_id, body_lower, "IN")

        return {"ok": True}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Opt-out webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# --- Delivery Status Webhook ---
@router.post("/status")
async def status_handler(request: Request):
    try:
        data    = await request.form()
        msg_id  = data.get("MessageSid")
        status  = (data.get("MessageStatus") or "").lower()
        to      = data.get("To")
        from_no = data.get("From")

        print(f"📡 Delivery receipt for {to} [{status}]")

        # Pool metrics
        if status == "delivered":
            increment_delivered(from_no)
        elif status in ("failed", "undelivered"):
            increment_failed(from_no)

        # Update conversation row by provider MessageSid
        if msg_id:
            try:
                existing = _find_one_by_field(convos, TG_ID_FIELD, msg_id)
                if existing:
                    _safe_update(convos, existing["id"], {STATUS_FIELD: status.upper()})
            except Exception as e:
                print(f"⚠️ Failed to update Conversations status: {e}")

        # Update lead delivery metrics
        match = _find_by_phone_last10(leads, to)
        if match:
            lead_id = match["id"]
            lf = match.get("fields", {})
            delivered_count = lf.get("Delivered Count", 0)
            failed_count    = lf.get("Failed Count", 0)
            updates = {
                "Last Activity": iso_timestamp(),
                "Last Delivery Status": status.upper(),
            }
            if status == "delivered":
                updates["Delivered Count"] = delivered_count + 1
            elif status in ("failed", "undelivered"):
                updates["Failed Count"] = failed_count + 1
            _safe_update(leads, lead_id, updates)

        return {"ok": True}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Status webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
