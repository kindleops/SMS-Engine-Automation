import os
import random
import traceback
from datetime import datetime, timezone
from pyairtable import Table

from sms.textgrid_sender import send_message

# --- ENV CONFIG ---
AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
BASE_ID              = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE          = os.getenv("LEADS_TABLE", "Leads")
TEMPLATES_TABLE      = os.getenv("TEMPLATES_TABLE", "Templates")

# --- Field Mappings ---
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
RECEIVED_AT  = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD", "intent_detected")

# --- Airtable clients ---
convos    = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
leads     = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
templates = Table(AIRTABLE_API_KEY, BASE_ID, TEMPLATES_TABLE)


# --- Helpers ---
def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def find_or_create_lead(phone_number: str, source: str = "Autoresponder"):
    """Ensure every phone is tied to a Lead record."""
    if not phone_number:
        return None
    try:
        results = leads.all(formula=f"{{phone}}='{phone_number}'")
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
    if not lead_id:
        return
    try:
        leads.update(lead_id, {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": body[:500] if body else ""
        })
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


# --- Template Fetcher ---
def get_template(intent: str, fields: dict) -> tuple[str, str]:
    """
    Fetch template text from Airtable by intent key.
    Returns (message_text, template_id).
    """
    try:
        results = templates.all(formula=f"{{Internal ID}}='{intent}'")
        if not results:
            return ("Hi, this is Ryan following up. Reply STOP to opt out.", None)
        template = random.choice(results)
        msg = template["fields"].get("Message", "Hi there")
        msg = msg.format(
            First=fields.get("First", "there"),
            Address=fields.get("Address", "your property")
        )
        return (msg, template["id"])
    except Exception as e:
        print(f"⚠️ Template lookup failed: {e}")
        return ("Hi, this is Ryan following up. Reply STOP to opt out.", None)


# --- KPI Logger ---
def log_template_kpi(template_id: str, event: str):
    if not template_id:
        return
    updates = {}
    if event == "send":
        updates["Sends"] = {"increment": 1}
    elif event == "positive":
        updates["Positive Replies"] = {"increment": 1}
    elif event == "negative":
        updates["Negative Replies"] = {"increment": 1}
    elif event == "optout":
        updates["Opt-Outs"] = {"increment": 1}
    elif event == "delivered":
        updates["Delivered"] = {"increment": 1}
    elif event == "failed":
        updates["Failed Deliveries"] = {"increment": 1}

    try:
        templates.update(template_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update KPI for template {template_id}: {e}")


# --- Intent Classifier ---
def classify_intent(body: str) -> str:
    text = (body or "").lower().strip()
    if any(w in text for w in ["yes", "yeah", "yep", "sure", "of course"]):
        return "followup_yes"
    if any(w in text for w in ["no", "nope", "nah", "not interested"]):
        return "followup_no"
    if "wrong" in text or "don't own" in text:
        return "followup_wrong"
    if "stop" in text or "unsubscribe" in text:
        return "optout"
    return "intro"


# --- Core Autoresponder ---
def run_autoresponder(limit: int = 50, view: str = "Unprocessed Inbounds"):
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

            # 1. Classify + fetch template
            intent = classify_intent(body)
            reply_text, template_id = get_template(intent, f)

            # 2. Send reply
            send_result = send_message(to_num, reply_text, from_number=None)

            # 3. Lead linking + activity
            lead_id = find_or_create_lead(from_num, source="Autoresponder")
            if lead_id:
                update_lead_activity(lead_id, reply_text, "OUT")

            # 4. Log outbound
            payload = {
                FROM_FIELD: to_num,
                TO_FIELD: from_num,
                MSG_FIELD: reply_text,
                STATUS_FIELD: "SENT",
                DIR_FIELD: "OUT",
                TG_ID_FIELD: send_result.get("sid"),
                SENT_AT: iso_timestamp(),
                PROCESSED_BY: "Autoresponder"
            }
            if lead_id:
                payload["lead_id"] = [lead_id]

            try:
                convos.create(payload)
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1
                log_template_kpi(template_id, "send")
            except Exception as log_err:
                print(f"⚠️ Failed to log AI reply: {log_err}")

            # 5. Mark inbound as processed
            try:
                convos.update(msg_id, {
                    STATUS_FIELD: "RESPONDED",
                    PROCESSED_BY: "Autoresponder",
                    "processed_at": iso_timestamp(),
                    INTENT_FIELD: intent
                })
            except Exception as mark_err:
                print(f"⚠️ Failed to update inbound row: {mark_err}")

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    return {"ok": True, "processed": processed, "breakdown": breakdown}