# sms/autoresponder.py
import os
import random
import traceback
from datetime import datetime, timezone
from functools import lru_cache

from sms.message_processor import MessageProcessor
from sms import templates as local_templates  # fallback if Airtable missing
from sms.ai_closer import run_ai_closer  # 🚀 AI takeover after Stage 3

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- ENV CONFIG ---
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE", "Templates")

# -----------------
# Airtable Clients
# -----------------
@lru_cache(maxsize=None)
def get_convos():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    return Table(api_key, base_id, CONVERSATIONS_TABLE) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_leads():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    return Table(api_key, base_id, LEADS_TABLE) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_prospects():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    return Table(api_key, base_id, PROSPECTS_TABLE) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_templates():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    return Table(api_key, base_id, TEMPLATES_TABLE) if api_key and base_id and Table else None

# -----------------
# Helpers
# -----------------
def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()

FIELD_MAP = {
    "phone": "phone",
    "Property ID": "Property ID",
    "Owner Name": "Owner Name",
    "Address": "Address",
    "Market": "Market",
    "Sync Source": "Synced From",
    "List": "Source List",
    "Property Type": "Property Type",
}

STAGE_MAP = {
    "intro": "Stage 1 - Owner Check",
    "followup_yes": "Stage 2 - Offer Interest",
    "followup_no": "Stage 2 - Offer Declined",
    "followup_wrong": "Stage 2 - Wrong Number",
    "not_owner": "Stage 2 - Not Owner",
    "price_response": "Stage 3 - Price Discussion",
    "condition_response": "Stage 3 - Condition Discussion",
    "optout": "Opt-Out",
}

# -----------------
# Lead Handling
# -----------------
def promote_to_lead(phone_number: str, source: str = "Autoresponder"):
    leads = get_leads()
    prospects = get_prospects()
    if not phone_number or not leads:
        return None, None
    try:
        existing = leads.all(formula=f"{{phone}}='{phone_number}'")
        if existing:
            lead = existing[0]
            return lead["id"], lead["fields"].get("Property ID")

        fields, property_id = {}, None
        if prospects:
            match = prospects.all(formula=f"{{phone}}='{phone_number}'")
            if match:
                pf = match[0]["fields"]
                fields = {leads_col: pf.get(prospects_col) for prospects_col, leads_col in FIELD_MAP.items()}
                property_id = pf.get("Property ID")

        new_lead = leads.create({
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source,
        })
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id
    except Exception as e:
        print(f"⚠️ Lead promotion failed: {e}")
        return None, None

def update_lead_activity(lead_id: str, body: str, direction: str, intent: str = None):
    leads = get_leads()
    if not lead_id or not leads:
        return
    try:
        updates = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        if intent == "followup_yes":
            updates["Lead Status"] = "Interested"
        leads.update(lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")

# -----------------
# Templates (Airtable → local fallback)
# -----------------
def get_template(intent: str, fields: dict) -> tuple[str, str | None]:
    templates = get_templates()
    if templates:
        try:
            results = templates.all(formula=f"{{Internal ID}}='{intent}'")
            if results:
                template = random.choice(results)
                msg = template["fields"].get("Message", "Hi there")
                msg = msg.format(
                    First=fields.get("First", "there"),
                    Address=fields.get("Address", "your property"),
                )
                return msg, template["id"]
        except Exception as e:
            print(f"⚠️ Template lookup failed: {e}")

    # fallback → local templates.py
    msg = local_templates.get_template(intent, fields)
    return msg, None

# -----------------
# Intent Classifier
# -----------------
def classify_intent(body: str) -> str:
    text = (body or "").lower().strip()
    if any(w in text for w in ["yes", "yeah", "yep", "sure", "i do", "that's me", "of course"]):
        return "followup_yes"
    if any(w in text for w in ["no", "nope", "nah", "not interested", "dont want to sell"]):
        return "followup_no"
    if any(w in text for w in ["wrong", "don't own", "not mine", "wrong number", "who is this"]):
        return "followup_wrong"
    if any(w in text for w in ["stop", "unsubscribe", "remove", "quit", "cancel"]):
        return "optout"
    if "$" in text or "k" in text or "price" in text or "asking" in text or "want" in text:
        return "price_response"
    if any(w in text for w in ["condition", "repairs", "needs work", "renovated", "tenant"]):
        return "condition_response"
    if any(w in text for w in ["maybe", "not sure", "thinking", "depends"]):
        return "neutral"
    return "intro"

# -----------------
# Core Autoresponder
# -----------------
def run_autoresponder(limit: int = 50, view: str = "Unprocessed Inbounds"):
    convos = get_convos()
    if not convos:
        return {"ok": False, "processed": 0, "breakdown": {}, "errors": ["Missing Conversations table"]}

    processed, breakdown, errors = 0, {}, []
    processed_by = os.getenv("PROCESSED_BY_LABEL", "Autoresponder")

    try:
        rows = convos.all(view=view, max_records=limit)
        for r in rows:
            f = r.get("fields", {})
            msg_id, from_num, body = r.get("id"), f.get("phone"), f.get("message")
            if not from_num or not body:
                continue

            print(f"🤖 {processed_by} inbound {from_num}: {body}")
            intent = classify_intent(body)

            # 🚀 Stage 3 → AI takeover
            if intent in ("price_response", "condition_response"):
                try:
                    ai_result = run_ai_closer(from_num, body, f)
                    convos.update(msg_id, {
                        "status": "AI_HANDOFF",
                        "processed_by": "AI Closer",
                        "processed_at": iso_timestamp(),
                        "intent_detected": intent,
                        "stage": STAGE_MAP.get(intent, "Stage 3 - AI Closing"),
                        "ai_result": str(ai_result),
                    })
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"AI closer failed: {e}"})
                continue

            # Normal flow (Stage 1-2)
            reply_text, template_id = get_template(intent, f)
            lead_id, property_id = promote_to_lead(from_num, source=processed_by)

            send_result = MessageProcessor.send(
                phone=from_num,
                body=reply_text,
                lead_id=lead_id,
                property_id=property_id,
                direction="OUT",
            )

            if send_result.get("status") == "sent":
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1
            else:
                errors.append({"phone": from_num, "error": send_result.get("error", "Send failed")})

            try:
                update_payload = {
                    "status": "RESPONDED",
                    "processed_by": processed_by,
                    "processed_at": iso_timestamp(),
                    "intent_detected": intent,
                    "stage": STAGE_MAP.get(intent, "Stage 1 - Owner Check"),
                }
                if template_id:
                    update_payload["template_id"] = template_id
                convos.update(msg_id, update_payload)
                if lead_id:
                    update_lead_activity(lead_id, body, "IN", intent=intent)
            except Exception as e:
                errors.append({"phone": from_num, "error": f"Failed to update row: {e}"})

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        errors.append(str(e))

    return {"ok": processed > 0, "processed": processed, "breakdown": breakdown, "errors": errors}
