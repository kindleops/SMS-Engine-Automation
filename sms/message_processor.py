import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message
from sms.utils.retry_handler import handle_retry

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")

# Field Mappings
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
leads  = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE)


class MessageProcessor:
    @staticmethod
    def send(phone: str, body: str, lead_id: str = None, direction: str = "OUT"):
        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        try:
            # Send message via TextGrid
            send_message(phone, body)

            # Log to Conversations
            record = convos.create({
                FROM_FIELD: phone,
                MSG_FIELD: body,
                "lead_id": lead_id,
                DIR_FIELD: direction,
                STATUS_FIELD: "SENT",
                SENT_AT: datetime.now(timezone.utc).isoformat()
            })

            # Update Leads (last_contacted)
            if lead_id:
                leads.update_by_fields({"property_id": lead_id}, {
                    "last_contacted": datetime.now(timezone.utc).isoformat()
                })

            print(f"📤 {direction} to {phone}: {body}")
            return {"status": "sent", "phone": phone, "body": body, "record_id": record["id"]}

        except Exception as e:
            # Retry handling should reference Conversations record, not lead_id
            if 'record' in locals():
                handle_retry(record["id"], str(e))
            print(f"❌ Failed sending to {phone}: {e}")
            return {"status": "failed", "phone": phone, "error": str(e)}