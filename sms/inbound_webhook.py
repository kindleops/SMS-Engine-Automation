from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table
import os

from sms.textgrid_sender import send_message
from sms.autoresponder import classify_reply

router = APIRouter()

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_LEADS_CONVOS_BASE_ID = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")

convos   = Table(AIRTABLE_API_KEY, AIRTABLE_LEADS_CONVOS_BASE_ID, "Conversations")
leads    = Table(AIRTABLE_API_KEY, AIRTABLE_LEADS_CONVOS_BASE_ID, "Leads")
numbers  = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Numbers")
campaigns = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, "Campaigns")

@router.post("/webhook")
async def inbound_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    from_number = payload.get("from")
    to_number = payload.get("to")
    body = payload.get("body")

    if not from_number or not body:
        raise HTTPException(status_code=422, detail="Missing 'from' or 'body'")

    # Try to resolve campaign based on "to_number"
    campaign_id = None
    for n in numbers.all():
        if n["fields"].get("Number") == to_number:
            campaign_id = n["fields"].get("Campaign")[0]
            break

    # Save inbound
    record = convos.create({
        "phone": from_number,
        "to_number": to_number,
        "message": body,
        "direction": "IN",
        "status": "NEW",
        "campaign_id": campaign_id
    })

    # Classify and reply
    intent = classify_reply(body)
    if intent == "WRONG":
        reply = "Thanks for letting me know—I’ll remove this number."
    elif intent == "NO":
        reply = "All good—thanks for confirming. I’ll mark our files. If anything changes, text me anytime."
    elif intent == "YES":
        reply = "Got it—are you open to a cash offer if the numbers make sense?"
    elif intent == "LATER":
        reply = "Totally fine—I’ll make a note to check back later. If timing changes sooner, just shoot me a text."
    else:
        reply = "Thanks for the response. Are you the property owner and open to an offer if the numbers work?"

    send_message(from_number, reply)

    convos.update(record["id"], {"status": f"PROCESSED-{intent}"})

    print(f"🤖 Replied to {from_number}: {intent} → {reply}")
    return {"status": "ok", "intent": intent, "reply": reply}