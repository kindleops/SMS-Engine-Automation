# sms/autoresponder.py
import os
import random
import traceback
from datetime import datetime, timezone
from functools import lru_cache

from sms.message_processor import MessageProcessor

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- ENV CONFIG ---
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE", "Templates")


# --- Lazy Airtable clients ---
@lru_cache(maxsize=None)
def get_convos():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    return (
        Table(api_key, base_id, CONVERSATIONS_TABLE)
        if api_key and base_id and Table
        else None
    )


@lru_cache(maxsize=None)
def get_leads():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    return (
        Table(api_key, base_id, LEADS_TABLE) if api_key and base_id and Table else None
    )


@lru_cache(maxsize=None)
def get_prospects():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    return (
        Table(api_key, base_id, PROSPECTS_TABLE)
        if api_key and base_id and Table
        else None
    )


@lru_cache(maxsize=None)
def get_templates():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    return (
        Table(api_key, base_id, TEMPLATES_TABLE)
        if api_key and base_id and Table
        else None
    )


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
    "Property ID": "Property ID",
    "Owner Name": "Owner Name",
    "Address": "Address",
    "Market": "Market",
    "Sync Source": "Synced From",
    "List": "Source List",
    "Property Type": "Property Type",
}


def promote_to_lead(phone_number: str, source: str = "Autoresponder"):
    """Ensure a phone has a Lead record, pulling from Prospects if needed."""
    leads = get_leads()
    prospects = get_prospects()
    if not phone_number or not leads:
        return None, None

    try:
        existing = leads.all(formula=f"{{phone}}='{phone_number}'")
        if existing:
            lead = existing[0]
            return lead["id"], lead["fields"].get("Property ID")

        # Pull from Prospects
        fields, property_id = {}, None
        if prospects:
            prospect_match = prospects.all(formula=f"{{phone}}='{phone_number}'")
            if prospect_match:
                p_fields = prospect_match[0]["fields"]
                fields = {
                    leads_col: p_fields.get(prospects_col)
                    for prospects_col, leads_col in FIELD_MAP.items()
                }
                property_id = p_fields.get("Property ID")

        new_lead = leads.create(
            {**fields, "phone": phone_number, "Lead Status": "New", "Source": source}
        )
        print(f"✨ Promoted {phone_number} → Lead")
        return new_lead["id"], property_id
    except Exception as e:
        print(f"⚠️ Lead promotion failed for {phone_number}: {e}")
        return None, None


def update_lead_activity(lead_id: str, body: str, direction: str):
    leads = get_leads()
    if not lead_id or not leads:
        return
    try:
        leads.update(
            lead_id,
            {
                "Last Activity": iso_timestamp(),
                "Last Direction": direction,
                "Last Message": (body or "")[:500],
            },
        )
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


# -----------------
# Templates
# -----------------
def get_template(intent: str, fields: dict) -> tuple[str, str | None]:
    templates = get_templates()
    if not templates:
        return ("Hi, this is Ryan following up. Reply STOP to opt out.", None)

    try:
        results = templates.all(formula=f"{{Internal ID}}='{intent}'")
        if not results:
            return ("Hi, this is Ryan following up. Reply STOP to opt out.", None)

        template = random.choice(results)
        msg = template["fields"].get("Message", "Hi there")
        msg = msg.format(
            First=fields.get("First", "there"),
            Address=fields.get("Address", "your property"),
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
    convos = get_convos()
    if not convos:
        return {
            "ok": False,
            "type": "Inbound",
            "processed": 0,
            "breakdown": {},
            "errors": ["Missing Airtable Conversations table"],
        }

    processed, breakdown, errors = 0, {}, []
    try:
        rows = convos.all(view=view, max_records=limit)
        for r in rows:
            f = r.get("fields", {})
            msg_id = r.get("id")
            from_num = f.get("phone")
            body = f.get("message")

            if not from_num or not body:
                continue

            print(f"🤖 Processing inbound from {from_num}: {body}")

            # 1. Classify + template
            intent = classify_intent(body)
            reply_text, template_id = get_template(intent, f)

            # 2. Promote → Lead
            lead_id, property_id = promote_to_lead(from_num, source="Autoresponder")

            # 3. Send reply
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
                errors.append(f"Send failed for {from_num}")

            # 4. Mark inbound as processed
            try:
                convos.update(
                    msg_id,
                    {
                        "status": "RESPONDED",
                        "processed_by": "Autoresponder",
                        "processed_at": iso_timestamp(),
                        "intent_detected": intent,
                    },
                )
            except Exception as mark_err:
                errors.append(f"Failed to update inbound row {msg_id}: {mark_err}")

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        errors.append(str(e))

    return {
        "ok": True if processed > 0 else False,
        "type": "Inbound",
        "processed": processed,
        "breakdown": breakdown,
        "errors": errors,
    }
