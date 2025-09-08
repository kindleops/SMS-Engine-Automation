import os
from datetime import datetime
from pyairtable import Table
from sms.textgrid_sender import send_message
from sms.utils.retry_handler import handle_retry

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
leads = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE)


class MessageProcessor:
    """Unified handler for sending, logging, and retrying SMS messages."""

    @staticmethod
    def send(phone: str, body: str, lead_id: str = None, direction: str = "OUT"):
        """
        Attempt to send an SMS. Logs to Conversations and pushes to retry if needed.
        """
        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        try:
            # Try sending
            send_message(phone, body)

            # Log success
            convos.create({
                "phone": phone,
                "lead_id": lead_id,
                "direction": direction,
                "message": body,
                "status": "SENT",
                "sent_at": str(datetime.utcnow())
            })

            if lead_id:
                leads.update_by_fields({"property_id": lead_id}, {"last_contacted": str(datetime.utcnow())})

            print(f"📤 {direction} to {phone}: {body}")
            return {"status": "sent", "phone": phone, "body": body}

        except Exception as e:
            # Handle retry
            if lead_id:
                handle_retry(lead_id, str(e))

            print(f"❌ Failed sending to {phone}: {e}")
            return {"status": "failed", "phone": phone, "error": str(e)}