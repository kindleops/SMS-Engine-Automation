import os
import random
import traceback
from datetime import datetime, timezone
from pyairtable import Table

from sms.textgrid_sender import send_message

# --- ENV CONFIG ---
AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
BASE_ID              = os.getenv("LEADS_CONVOS_BASE")
CONVERSATIONS_TABLE  = "Conversations"
LEADS_TABLE          = "Leads"
PROSPECTS_TABLE      = "Prospects"
TEMPLATES_TABLE      = "Templates"

# --- Airtable clients ---
convos     = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
leads      = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
prospects  = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE)
templates  = Table(AIRTABLE_API_KEY, BASE_ID, TEMPLATES_TABLE)

# -----------------
# Configurable Field Mapping
# -----------------
FIELD_MAP = {
    "phone": "phone",
    "Owner Name": "Owner Name",
    "Address": "Address",
    "Market": "Market",
    "Sync Source": "Synced From",
    "List": "Source List",          # e.g. “Houston TX Tax Delinquent”
    "Property Type": "Property Type"
    # Add more here as needed → {"Prospects field": "Leads field"}
}


def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# -----------------
# Promote Prospects → Leads
# -----------------
def promote_to_lead(phone_number: str, source: str = "Autoresponder"):
    if not phone_number:
        return None
    try:
        existing = leads.all(formula=f"{{phone}}='{phone_number}'")
        if existing:
            return existing[0]["id"]

        # Try pulling from Prospects
        prospect_match = prospects.all(formula=f"{{phone}}='{phone_number}'")
        fields = {}
        if prospect_match:
            p_fields = prospect_match[0]["fields"]
            fields = {leads_col: p_fields.get(prospects_col) 
                      for prospects_col, leads_col in FIELD_MAP.items()}

        new_lead = leads.create({
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source
        })
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"]
    except Exception as e:
        print(f"⚠️ Lead promotion failed for {phone_number}: {e}")
        return None


def update_lead_activity(lead_id: str, body: str, direction: str):
    if not lead_id:
        return
    try:
        leads.update(lead_id, {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500]
        })
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


# -----------------
# Templates
# -----------------
def get_template(intent: str, fields: dict) -> tuple[str, str]:
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


# -----------------
# Intent Classifier
# -----------------
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


# -----------------
# Core Autoresponder
# -----------------
def run_autoresponder(limit: int = 50, view: str = "Unprocessed Inbounds"):
    processed, breakdown = 0, {}

    try:
        rows = convos.all(view=view, max_records=limit)
        for r in rows:
            f = r.get("fields", {})
            msg_id   = r.get("id")
            from_num = f.get("phone")
            to_num   = f.get("to_number")
            body     = f.get("message")

            if not from_num or not body:
                continue

            print(f"🤖 Processing inbound from {from_num}: {body}")

            # 1. Classify + template
            intent = classify_intent(body)
            reply_text, template_id = get_template(intent, f)

            # 2. Send reply
            send_result = send_message(to_num, reply_text, from_number=None)

            # 3. Promote → Lead
            lead_id = promote_to_lead(from_num, source="Autoresponder")
            if lead_id:
                update_lead_activity(lead_id, reply_text, "OUT")

            # 4. Log outbound
            payload = {
                "phone": to_num,
                "to_number": from_num,
                "message": reply_text,
                "status": "SENT",
                "direction": "OUT",
                "TextGrid ID": send_result.get("sid"),
                "sent_at": iso_timestamp(),
                "processed_by": "Autoresponder"
            }
            if lead_id:
                payload["lead_id"] = [lead_id]

            try:
                convos.create(payload)
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1
            except Exception as log_err:
                print(f"⚠️ Failed to log AI reply: {log_err}")

            # 5. Mark inbound processed
            try:
                convos.update(msg_id, {
                    "status": "RESPONDED",
                    "processed_by": "Autoresponder",
                    "processed_at": iso_timestamp(),
                    "intent_detected": intent
                })
            except Exception as mark_err:
                print(f"⚠️ Failed to update inbound row: {mark_err}")

    except Exception:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        return {"ok": False, "error": "failed"}

    return {"ok": True, "processed": processed, "breakdown": breakdown}