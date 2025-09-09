from fastapi import APIRouter, Request, HTTPException
from sms.airtable_client import leads_table
from sms.textgrid_sender import send_message
from sms.autoresponder import classify_reply

router = APIRouter()

convos = leads_table("Conversations")
leads  = leads_table("Leads")

@router.post("/webhook")
async def inbound_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    from_number = payload.get("from") or payload.get("From")
    to_number   = payload.get("to") or payload.get("To")
    message_body = payload.get("body") or payload.get("Body")

    if not from_number or not message_body:
        raise HTTPException(status_code=422, detail="Missing 'from' or 'body' in payload")

    record = convos.create({
        "phone": from_number,
        "to_number": to_number,
        "message": message_body,
        "direction": "IN",
        "status": "RECEIVED"
    })

    intent = classify_reply(message_body)
    if intent == "WRONG":
        reply = "Thanks for letting me know—I’ll remove this number."
    elif intent == "NO":
        reply = "All good—thanks for confirming. I’ll mark our files. If anything changes, text me anytime."
    elif intent == "YES":
        reply = "Got it—are you the decision-maker and open to an offer if the numbers make sense?"
    elif intent == "LATER":
        reply = "Totally fine—I’ll check back later. If timing changes, just text me."
    else:
        reply = "Thanks for the response. Are you the owner and open to hearing an offer if the numbers work?"

    send_message(from_number, reply)

    convos.update(record["id"], {"status": f"PROCESSED-{intent}", "intent_detected": intent})
    lead_id = record.get("fields", {}).get("lead_id")
    if lead_id:
        leads.update_by_fields({"property_id": lead_id}, {"intent": intent})

    return {"status": "ok", "intent": intent, "reply": reply}