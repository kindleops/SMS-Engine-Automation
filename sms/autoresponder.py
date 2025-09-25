# sms/autoresponder.py
import os, random, traceback
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message
from sms.message_processor import MessageProcessor

# --- ENV CONFIG ---
AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
BASE_ID              = os.getenv("LEADS_CONVOS_BASE")

CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE          = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE      = os.getenv("PROSPECTS_TABLE", "Prospects")
TEMPLATES_TABLE      = os.getenv("TEMPLATES_TABLE", "Templates")

# --- Airtable clients ---
convos     = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
leads      = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
prospects  = Table(AIRTABLE_API_KEY, BASE_ID, PROSPECTS_TABLE)
templates  = Table(AIRTABLE_API_KEY, BASE_ID, TEMPLATES_TABLE)


# -----------------
# Helpers
# -----------------
def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------
# Promote Prospects → Leads
# -----------------
FIELD_MAP = {
    "phone": "phone",
    "Property ID": "Property ID",   # 🔑 keep property linkage
    "Owner Name": "Owner Name",
    "Address": "Address",
    "Market": "Market",
    "Sync Source": "Synced From",
    "List": "Source List",
    "Property Type": "Property Type"
}

def promote_to_lead(phone_number: str, source: str = "Autoresponder"):
    """Ensure a phone has a Lead record, pulling from Prospects if needed."""
    if not phone_number:
        return None, None
    try:
        existing = leads.all(formula=f"{{phone}}='{phone_number}'")
        if existing:
            lead = existing[0]
            return lead["id"], lead["fields"].get("Property ID")

        # Pull from Prospects
        prospect_match = prospects.all(formula=f"{{phone}}='{phone_number}'")
        fields, property_id = {}, None
        if prospect_match:
            p_fields = prospect_match[0]["fields"]
            fields = {
                leads_col: p_fields.get(prospects_col)
                for prospects_col, leads_col in FIELD_MAP.items()
            }
            property_id = p_fields.get("Property ID")

        new_lead = leads.create({
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source
        })
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id
    except Exception as e:
        print(f"⚠️ Lead promotion failed for {phone_number}: {e}")
        return None, None


def update_lead_activity(lead_id: str, body: str, direction: str):
    """Update last activity fields on Lead record."""
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
def get_template(intent: str, fields: dict) -> tuple[str, str | None]:
    """Fetch + personalize template by intent key."""
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
            from_num = f.get("phone")       # customer number
            to_num   = f.get("to_number")   # our number
            body     = f.get("message")

            if not from_num or not body:
                continue

            print(f"🤖 Processing inbound from {from_num}: {body}")

            # 1. Classify intent + fetch template
            intent = classify_intent(body)
            reply_text, template_id = get_template(intent, f)

            # 2. Promote → Lead
            lead_id, property_id = promote_to_lead(from_num, source="Autoresponder")

            # 3. Send reply via unified processor
            send_result = MessageProcessor.send(
                phone=from_num,
                body=reply_text,
                lead_id=lead_id,
                property_id=property_id,
                direction="OUT"
            )

            if send_result["status"] == "sent":
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1

            # 4. Mark inbound as processed
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