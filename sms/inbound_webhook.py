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

# Field Mappings (env-driven, must match Airtable field names exactly)
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")  # ⚠️ case-sensitive
RECEIVED_AT  = os.getenv("CONV_RECEIVED_AT_FIELD", "received_at")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD", "intent_detected")
PROC_BY      = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")

# --- Airtable Tables ---
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE_ID, CONVERSATIONS_TABLE)
leads  = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE_ID, LEADS_TABLE)

# --- Router ---
router = APIRouter(prefix="/inbound", tags=["Inbound"])

@router.post("")
async def inbound_webhook(request: Request):
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

    # Log inbound message
    record = convos.create({
        FROM_FIELD: from_number,
        TO_FIELD: to_number,
        MSG_FIELD: message_body,
        DIR_FIELD: "IN",
        STATUS_FIELD: "UNPROCESSED",
        TG_ID_FIELD: msg_id,
        RECEIVED_AT: datetime.now(timezone.utc).isoformat()
    })

    # Classify + build auto-reply
    intent, _ = classify_reply(message_body)
    replies = {
        "WRONG":  "Thanks for letting me know—I’ll remove this number.",
        "NO":     "All good—thanks for confirming. If anything changes, text me anytime.",
        "YES":    "Got it—are you open to a cash offer if the numbers make sense?",
        "LATER":  "Totally fine—I’ll check back later. If timing changes, just text me.",
        "OTHER":  "Thanks for the response. Are you the owner and open to an offer if the numbers work?",
    }
    reply = replies.get(intent, replies["OTHER"])

    # Send outbound reply and log it consistently
    try:
        send_message(from_number, reply)
        convos.create({
            FROM_FIELD: to_number,             # our sending number
            TO_FIELD: from_number,             # recipient
            MSG_FIELD: reply,
            DIR_FIELD: "OUT",
            STATUS_FIELD: "SENT",
            SENT_AT: datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        print(f"❌ Failed to send auto-reply to {from_number}: {e}")

    # Update inbound record
    convos.update(record["id"], {
        STATUS_FIELD: f"PROCESSED-{intent}",
        INTENT_FIELD: intent,
        PROC_BY: "Inbound Webhook"
    })

    # (Optional) Update linked Lead by phone match
    try:
        lead_matches = leads.all(formula=f"{{phone}} = '{from_number}'")
        if lead_matches:
            leads.update(lead_matches[0]["id"], {INTENT_FIELD: intent})
    except Exception as e:
        print(f"⚠️ Lead update failed for {from_number}: {e}")

    return {"ok": True, "intent": intent, "reply": reply}