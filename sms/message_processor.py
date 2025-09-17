import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message
from sms.utils.retry_handler import handle_retry

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
leads  = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE)


class MessageProcessor:
    """Unified handler for sending, logging, and retrying SMS messages."""

    @staticmethod
    def send(phone: str, body: str, lead_id: str = None, direction: str = "OUT", processed_by: str = "MessageProcessor"):
        """
        Attempt to send an SMS. Logs to Conversations and pushes to retry if needed.
        """
        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        ts = datetime.now(timezone.utc).isoformat()

        try:
            # --- Try sending ---
            send_message(phone, body)

            # --- Log success in Conversations ---
            convos.create({
                "phone" if direction == "IN" else "To Number": phone,
                "message": body,
                "direction": direction,
                "status": "SENT",
                "lead_id": [lead_id] if lead_id else None,
                "sent_at": ts,
                "processed_by": processed_by,
            })

            # --- Update linked Lead ---
            if lead_id:
                try:
                    leads.update(lead_id, {"Last Contacted": ts})
                except Exception as e:
                    print(f"⚠️ Failed to update lead {lead_id}: {e}")

            print(f"📤 {direction} → {phone}: {body}")
            return {"status": "sent", "phone": phone, "body": body, "timestamp": ts}

        except Exception as e:
            # --- Log failure in Conversations ---
            convos.create({
                "to_number": phone,
                "message": body,
                "direction": direction,
                "status": "FAILED",
                "Error": str(e),
                "lead_id": [lead_id] if lead_id else None,
                "sent_at": ts,
                "processed_by": processed_by,
            })

            # --- Push to retry handler ---
            if lead_id:
                try:
                    handle_retry(lead_id, str(e))
                except Exception as re:
                    print(f"⚠️ Retry handler failed for {lead_id}: {re}")

            print(f"❌ Failed sending to {phone}: {e}")
            return {"status": "failed", "phone": phone, "error": str(e), "timestamp": ts}