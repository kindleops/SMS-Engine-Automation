import os
import traceback
from datetime import datetime, timezone
from pyairtable import Table

from sms.textgrid_sender import send_message

# --- ENV CONFIG ---
AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE    = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE          = os.getenv("LEADS_TABLE", "Leads")

# --- Field Mappings ---
FROM_FIELD       = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD         = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD        = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD     = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD        = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD      = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
RECEIVED_AT      = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
PROCESSED_BY     = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")
SENT_AT          = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
INTENT_FIELD     = os.getenv("CONV_INTENT_FIELD", "intent_detected")

# Airtable clients
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY and LEADS_CONVOS_BASE else None
leads  = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE) if AIRTABLE_API_KEY and LEADS_CONVOS_BASE else None


# --- Helpers ---
def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def find_or_create_lead(phone_number: str, source: str = "Autoresponder"):
    """Ensure every phone is tied to a Lead record."""
    if not leads or not phone_number:
        return None
    try:
        formula = f"{{phone}}='{phone_number}'"
        results = leads.all(formula=formula)
        if results:
            return results[0]["id"]
        new_lead = leads.create({
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source
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
        leads.update(lead_id, {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": body[:500] if body else ""
        })
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


# --- Core Autoresponder Logic ---
def run_autoresponder(limit: int = 50, view: str = "Unprocessed Inbounds"):
    """
    Process inbound messages → generate AI response → send → log to Airtable.
    """
    if not convos:
        return {"ok": False, "error": "Conversations table not configured"}

    processed = 0
    breakdown = {}

    try:
        rows = convos.all(view=view, max_records=limit)
        for r in rows:
            f = r.get("fields", {})
            msg_id   = r.get("id")
            from_num = f.get(FROM_FIELD)
            to_num   = f.get(TO_FIELD)
            body     = f.get(MSG_FIELD)

            if not from_num or not body:
                continue

            print(f"🤖 Processing inbound from {from_num}: {body}")

            # 1. Generate reply (placeholder AI logic for now)
            reply_text = f"Thanks for your message: {body}"

            # 2. Send reply
            send_result = send_message(to_num, from_num, reply_text)

            # 3. Lead linking + activity
            lead_id = find_or_create_lead(from_num, source="Autoresponder")
            if lead_id:
                update_lead_activity(lead_id, reply_text, "OUT")

            # 4. Log outbound to Conversations
            payload = {
                FROM_FIELD: to_num,
                TO_FIELD: from_num,
                MSG_FIELD: reply_text,
                STATUS_FIELD: "SENT",
                DIR_FIELD: "OUT",
                TG_ID_FIELD: send_result.get("sid"),
                SENT_AT: iso_timestamp(),
                PROCESSED_BY: os.getenv("PROCESSED_BY_LABEL", "Autoresponder")
            }
            if lead_id:
                payload["lead_id"] = [lead_id]

            try:
                convos.create(payload)
                processed += 1
                breakdown["replied"] = breakdown.get("replied", 0) + 1
            except Exception as log_err:
                print(f"⚠️ Failed to log AI reply: {log_err}")

            # 5. Mark inbound as processed
            try:
                convos.update(msg_id, {
                    STATUS_FIELD: "RESPONDED",
                    PROCESSED_BY: os.getenv("PROCESSED_BY_LABEL", "Autoresponder"),
                    "processed_at": iso_timestamp(),
                    INTENT_FIELD: "auto_reply"
                })
            except Exception as mark_err:
                print(f"⚠️ Failed to update inbound row: {mark_err}")

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    return {"ok": True, "processed": processed, "breakdown": breakdown}