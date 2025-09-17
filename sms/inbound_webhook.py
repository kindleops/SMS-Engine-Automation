# sms/inbound_webhook.py
import os
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException
from pyairtable import Table
from sms.textgrid_sender import send_message
from sms.autoresponder import classify_reply

# --- Env ---
AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE_ID = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID") or os.getenv("LEADS_CONVOS_BASE")
CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE          = os.getenv("LEADS_TABLE", "Leads")

# --- Airtable Tables ---
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE_ID, CONVERSATIONS_TABLE)
leads  = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE_ID, LEADS_TABLE)

# --- Router ---
router = APIRouter(prefix="/inbound", tags=["Inbound"])

@router.post("")
async def inbound_webhook(request: Request):
    """
    TextGrid → Inbound SMS webhook
    Stores message in Airtable Conversations,
    classifies intent, sends auto-reply, and updates Leads.
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    from_number  = payload.get("from") or payload.get("From")
    to_number    = payload.get("to") or payload.get("To")
    message_body = payload.get("message") or payload.get("Body")
    msg_id       = payload.get("id")

    if not from_number or not message_body:
        raise HTTPException(status_code=422, detail="Missing 'from' or 'message' in payload")

    # Log to Conversations
    record = convos.create({
        "phone": from_number,
        "to_number": to_number,
        "message": message_body,
        "direction": "IN",
        "status": "RECEIVED",
        "TextGrid ID": msg_id,
        "received_at": datetime.now(timezone.utc).isoformat()
    })

    # Classify + auto-reply
    intent, _ = classify_reply(message_body)
    if intent == "WRONG":
        reply = "Thanks for letting me know—I’ll remove this number."
    elif intent == "NO":
        reply = "All good—thanks for confirming. If anything changes, text me anytime."
    elif intent == "YES":
        reply = "Got it—are you open to a cash offer if the numbers make sense?"
    elif intent == "LATER":
        reply = "Totally fine—I’ll check back later. If timing changes, just text me."
    else:
        reply = "Thanks for the response. Are you the owner and open to an offer if the numbers work?"

    send_message(from_number, reply)

    # Update Conversations record
    convos.update(record["id"], {
        "status": f"PROCESSED-{intent}",
        "intent": intent
    })

    # Update linked Lead if exists
    lead_id = record.get("fields", {}).get("lead_id")
    if lead_id:
        try:
            leads.update_by_fields({"property_id": lead_id}, {"intent": intent})
        except Exception as e:
            print(f"⚠️ Failed to update Lead {lead_id}: {e}")

    return {"ok": True, "intent": intent, "reply": reply}