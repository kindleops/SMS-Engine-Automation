import os
import time
import httpx
from datetime import datetime, timezone
from pyairtable import Table
from sms.number_pools import get_from_number   # auto-select pool numbers

# --- Env Config ---
ACCOUNT_SID        = os.getenv("TEXTGRID_ACCOUNT_SID")
AUTH_TOKEN         = os.getenv("TEXTGRID_AUTH_TOKEN")

AIRTABLE_API_KEY   = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE  = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE         = os.getenv("LEADS_TABLE", "Leads")

# --- Field Mappings ---
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# Airtable clients
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY and LEADS_CONVOS_BASE else None
leads  = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE) if AIRTABLE_API_KEY and LEADS_CONVOS_BASE else None

# --- Base URL (TextGrid API) ---
BASE_URL = f"https://api.textgrid.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json"

# --- Helpers ---
def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def find_or_create_lead(phone_number: str, source: str = "Outbound"):
    """Ensure every outbound is tied to a Lead record."""
    if not leads or not phone_number:
        return None
    try:
        results = leads.all(formula=f"{{phone}}='{phone_number}'")
        if results:
            return results[0]["id"]
        new_lead = leads.create({
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source,
            "Reply Count": 0,
            "Sent Count": 0,
            "Delivered Count": 0,
            "Failed Count": 0
        })
        print(f"✨ Created new Lead for {phone_number}")
        return new_lead["id"]
    except Exception as e:
        print(f"⚠️ Lead lookup/create failed for {phone_number}: {e}")
    return None

def update_lead_activity(lead_id: str, body: str, direction: str):
    """Update activity tracking fields on Lead record."""
    if not leads or not lead_id:
        return
    try:
        updates = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": body[:500] if body else ""
        }
        if direction == "OUT":
            updates["Last Outbound"] = iso_timestamp()
        leads.update(lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")

# --- Send Message ---
def send_message(to: str, body: str, from_number: str | None = None, market: str | None = None, retries: int = 3) -> dict:
    if not ACCOUNT_SID or not AUTH_TOKEN:
        raise RuntimeError("❌ TEXTGRID_ACCOUNT_SID or TEXTGRID_AUTH_TOKEN not set")

    sender = from_number or get_from_number(market=market)

    payload = {
        "To": to,
        "From": sender,
        "Body": body,
    }

    attempt = 0
    while attempt < retries:
        try:
            resp = httpx.post(
                BASE_URL,
                data=payload,
                auth=(ACCOUNT_SID, AUTH_TOKEN),
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            msg_id = data.get("sid")

            print(f"📤 Sent SMS → {to} (From {sender}): {body}")

            # --- Lead linking & activity update ---
            lead_id = find_or_create_lead(to, source="Outbound")
            if lead_id:
                update_lead_activity(lead_id, body, "OUT")

            # --- Airtable logging ---
            if convos:
                record = {
                    FROM_FIELD: sender,
                    TO_FIELD: to,
                    MSG_FIELD: body,
                    STATUS_FIELD: "SENT",
                    DIR_FIELD: "OUT",
                    TG_ID_FIELD: msg_id,
                    SENT_AT: iso_timestamp(),
                    PROCESSED_BY: "TextGrid Sender"
                }
                if lead_id:
                    record["lead_id"] = [lead_id]   # ✅ correct link field name

                try:
                    convos.create(record)
                except Exception as log_err:
                    print(f"⚠️ Failed to log outbound SMS to Airtable: {log_err}")

            return data

        except Exception as e:
            attempt += 1
            wait_time = 2 ** attempt
            err_msg = str(e)

            # Log failure in Airtable
            if convos:
                try:
                    convos.create({
                        FROM_FIELD: sender,
                        TO_FIELD: to,
                        MSG_FIELD: body,
                        STATUS_FIELD: "FAILED",
                        DIR_FIELD: "OUT",
                        TG_ID_FIELD: None,
                        SENT_AT: iso_timestamp(),
                        PROCESSED_BY: "TextGrid Sender"
                    })
                except Exception as log_err:
                    print(f"⚠️ Failed to log FAILED SMS to Airtable: {log_err}")

            print(f"❌ Attempt {attempt} failed for {to}: {err_msg}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"🚨 Giving up on {to} after {retries} attempts")
                return {"error": err_msg, "to": to, "body": body, "attempts": retries}