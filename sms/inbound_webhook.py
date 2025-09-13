from fastapi import APIRouter, Request, HTTPException
from datetime import datetime, timezone
from sms.airtable_client import get_leads_table
from sms.textgrid_sender import send_message
from sms.autoresponder import classify_reply, REPLIES

router = APIRouter()

# Tables
convos = get_leads_table("Conversations")
leads  = get_leads_table("Leads")

@router.post("/inbound")
async def inbound_webhook(request: Request):
    """
    Webhook for inbound SMS.
    - Logs message in Conversations table
    - Classifies intent
    - Sends appropriate auto-reply
    - Updates lead record if linked
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    from_number = payload.get("from") or payload.get("From")
    to_number   = payload.get("to") or payload.get("To")
    message     = payload.get("body") or payload.get("Body") or payload.get("message")

    if not from_number or not message:
        raise HTTPException(status_code=422, detail="Missing 'from' or 'message' in payload")

    # --- Create conversation record ---
    conv_record = convos.create({
        "From Number": from_number,
        "To Number": to_number,
        "Message": message,
        "Direction": "IN",
        "Status": "UNPROCESSED",
        "Received At": datetime.now(timezone.utc).isoformat()
    })

    # --- Classify + reply ---
    intent, _ = classify_reply(message)
    reply = REPLIES.get(intent, REPLIES["OTHER"])

    try:
        send_message(from_number, reply)
    except Exception as e:
        print(f"❌ Failed to send reply to {from_number}: {e}")

    # --- Update conversation record ---
    try:
        convos.update(conv_record["id"], {
            "Status": f"PROCESSED-{intent}",
            "Intent": intent,
            "Processed At": datetime.now(timezone.utc).isoformat(),
            "Processed By": "Inbound Webhook"
        })
    except Exception as e:
        print(f"⚠️ Failed to update conversation {conv_record['id']}: {e}")

    # --- Update lead record if linked ---
    lead_id = conv_record.get("fields", {}).get("lead_id")
    if lead_id and leads:
        try:
            leads.update(lead_id, {"Intent": intent})
        except Exception as e:
            print(f"⚠️ Failed to update Lead {lead_id}: {e}")

    return {"ok": True, "intent": intent, "reply": reply}