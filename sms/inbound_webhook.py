from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table
import os
from sms.textgrid_sender import send_message
from sms.autoresponder import classify_reply

router = APIRouter()

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

# --- Airtable Tables ---
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Conversations")
leads = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, "Leads")

@router.post("/webhook")
async def inbound_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    from_number = payload.get("from")
    to_number = payload.get("to")
    message_body = payload.get("body")

    if not from_number or not message_body:
        raise HTTPException(status_code=422, detail="Missing 'from' or 'body' in payload")

    # Save inbound into Conversations
    record = convos.create({
        "phone": from_number,
        "to_number": to_number,
        "message": message_body,
        "direction": "IN",
        "status": "NEW"
    })

    print(f"📩 Inbound SMS from {from_number}: {message_body}")

    # Classify + pick reply
    intent = classify_reply(message_body)
    if intent == "WRONG":
        reply = "Thanks for letting me know—I’ll remove this number."
    elif intent == "NO":
        reply = "Got it—I'll mark you as not interested. If anything changes, text me anytime."
    elif intent == "YES":
        reply = "Got it—are you open to a cash offer if the numbers make sense?"
    elif intent == "LATER":
        reply = "Totally fine—I’ll check back later. If timing changes sooner, just let me know."
    else:
        reply = "Thanks for the response. Just to clarify—are you the owner and open to an offer if the numbers work?"

    # Send SMS
    send_message(from_number, reply)

    # Update convo record
    convos.update(record["id"], {
        "status": f"PROCESSED-{intent}"
    })

    # Update linked lead if exists
    lead_id = record["fields"].get("lead_id")
    if lead_id:
        leads.update_by_fields({"property_id": lead_id}, {"intent": intent})

    print(f"🤖 Replied to {from_number}: {intent} → {reply}")

    return {"status": "ok", "intent": intent, "reply": reply}