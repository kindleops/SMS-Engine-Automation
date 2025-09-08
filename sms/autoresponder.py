import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
UNPROCESSED_VIEW = os.getenv("UNPROCESSED_VIEW", "Unprocessed Inbounds")

convos = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, CONVERSATIONS_TABLE)
leads = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, LEADS_TABLE)

# --- Classification logic ---
def classify_reply(body: str) -> str:
    b = (body or "").lower()

    if any(x in b for x in [
        "wrong", "not me", "who is this", "who are you", "don’t own", "dont own",
        "i rent", "tenant", "landlord", "this isn’t mine", "this is not mine"
    ]):
        return "WRONG"

    if any(x in b for x in [
        "stop", "unsubscribe", "quit", "end", "cancel", "no thanks",
        "not interested", "leave me alone", "do not contact", "remove me",
        "don’t text", "dont text"
    ]):
        return "NO"

    if any(x in b for x in [
        "yes", "maybe", "depends", "what’s the offer", "whats the offer",
        "offer", "price", "how much", "interested", "thinking about it",
        "make me an offer", "what would you pay", "send me an offer",
        "possibly", "potentially", "numbers", "give me a price"
    ]):
        return "YES"

    if any(x in b for x in [
        "not now", "later", "call me later", "maybe in future",
        "check back", "not right now", "thinking", "not selling yet"
    ]):
        return "LATER"

    return "UNKNOWN"

# --- Autoresponder runner ---
def run_autoresponder(limit: int = 50, view: str = UNPROCESSED_VIEW):
    records = convos.all(view=view)[:limit]
    processed = 0

    for r in records:
        try:
            fields = r.get("fields", {})
            msg = fields.get("message", "")
            phone = fields.get("phone")
            lead_id = fields.get("lead_id")
            intent = classify_reply(msg)

            if not phone:
                continue

            # Pick reply
            if intent == "WRONG":
                reply = "Thanks for letting me know—I’ll remove this number."
            elif intent == "NO":
                reply = "All good—thanks for confirming. I’ll mark our files. If anything changes, text me anytime."
            elif intent == "YES":
                reply = "Great — are you open to a cash offer if the numbers make sense?"
            elif intent == "LATER":
                reply = "Totally fine—I’ll check back later. If timing changes sooner, just shoot me a text."
            else:
                reply = "Thanks for the response. Just to clarify—are you the owner of the property and open to hearing an offer if the numbers work?"

            # Send SMS
            send_message(phone, reply)

            # Update Conversations
            convos.update(r["id"], {
                "status": f"PROCESSED-{intent}",
                "processed_at": datetime.now(timezone.utc).isoformat()
            })

            # Update Leads
            if lead_id:
                leads.update_by_fields({"property_id": lead_id}, {"intent": intent})

            print(f"🤖 Reply to {phone}: {intent} → {reply}")
            processed += 1

        except Exception as e:
            print(f"❌ Error processing {r.get('id')}: {e}")
            continue

    print(f"📊 Autoresponder finished — processed {processed} messages")
    return {"processed": processed}