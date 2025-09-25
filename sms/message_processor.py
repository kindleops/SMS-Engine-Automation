# sms/message_processor.py
import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message
from sms.utils.retry_handler import handle_retry

# --- Airtable Config ---
AIRTABLE_API_KEY    = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE   = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE         = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE     = os.getenv("PROSPECTS_TABLE", "Prospects")

# --- Field Mappings ---
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")

# --- Airtable Clients ---
convos    = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE) if AIRTABLE_API_KEY else None
leads     = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE) if AIRTABLE_API_KEY else None
prospects = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, PROSPECTS_TABLE) if AIRTABLE_API_KEY else None


class MessageProcessor:
    @staticmethod
    def send(phone: str,
             body: str,
             lead_id: str | None = None,
             property_id: str | None = None,
             direction: str = "OUT") -> dict:
        """
        Core message processor:
          - Sends SMS via TextGrid
          - Logs into Conversations
          - Updates Leads with activity
          - Handles retries on failure
        """
        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        try:
            # --- Send Message ---
            send_result = send_message(phone, body)

            # --- Log to Conversations ---
            payload = {
                FROM_FIELD: phone,             # recipient phone (OUT) or sender (IN)
                MSG_FIELD: body,
                DIR_FIELD: direction,
                STATUS_FIELD: "SENT",
                SENT_AT: datetime.now(timezone.utc).isoformat()
            }
            if lead_id:
                payload["lead_id"] = [lead_id]
            if property_id:
                payload["Property ID"] = property_id  # 🔗 maintain linkage

            record = convos.create(payload) if convos else {"id": None}

            # --- Update Lead ---
            if lead_id and leads:
                now_iso = datetime.now(timezone.utc).isoformat()
                leads.update(lead_id, {
                    "Last Outbound": now_iso if direction == "OUT" else None,
                    "Last Activity": now_iso,
                    "Last Message": body[:500],
                    "Property ID": property_id
                })

            print(f"📤 {direction} → {phone} | Body: {body} | Property: {property_id or 'N/A'}")
            return {
                "status": "sent",
                "phone": phone,
                "body": body,
                "record_id": record.get("id"),
                "property_id": property_id,
                "textgrid": send_result
            }

        except Exception as e:
            # Handle retry if conversation record exists
            if 'record' in locals() and record.get("id"):
                handle_retry(record["id"], str(e))
            print(f"❌ Failed sending to {phone}: {e}")
            return {"status": "failed", "phone": phone, "error": str(e)}