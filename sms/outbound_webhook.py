import os, re, traceback
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table

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
        data = await request.form()
        to_number   = data.get("To")
        from_number = data.get("From")
        body        = (data.get("Body") or "").strip()
        msg_id      = data.get("MessageSid") or data.get("TextGridId")
        campaign_id = data.get("Campaign ID") or data.get("campaign_id")
        template_id = data.get("Template ID") or data.get("template_id")

        if not to_number or not body:
            raise HTTPException(status_code=400, detail="Missing To or Body")

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
            DIR_FIELD: "OUT",
            TG_ID_FIELD: msg_id,
            SENT_AT: iso_timestamp(),
            PROCESSED_BY: "Outbound Webhook",
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

        return {"ok": True, "record_id": record.get("id") if record else None}

    except HTTPException:
        raise
    except Exception as e:
        print("❌ Outbound webhook error:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))