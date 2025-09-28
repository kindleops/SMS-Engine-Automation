# sms/message_processor.py
import os
from datetime import datetime, timezone
from functools import lru_cache

try:
    from pyairtable import Table
except ImportError:
    Table = None

from sms.textgrid_sender import send_message
from sms.retry_handler import handle_retry

# --- Field Mappings ---
FROM_FIELD = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")
SENT_AT = os.getenv("CONV_SENT_AT_FIELD", "sent_at")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")


# --- Lazy Airtable Clients ---
@lru_cache(maxsize=1)
def get_convos():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    if api_key and base_id and Table:
        try:
            return Table(api_key, base_id, CONVERSATIONS_TABLE)
        except Exception as e:
            print(f"⚠️ MessageProcessor: failed to init Conversations → {e}")
    print("⚠️ MessageProcessor: Conversations in MOCK mode")
    return None


@lru_cache(maxsize=1)
def get_leads():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    if api_key and base_id and Table:
        try:
            return Table(api_key, base_id, LEADS_TABLE)
        except Exception as e:
            print(f"⚠️ MessageProcessor: failed to init Leads → {e}")
    return None


@lru_cache(maxsize=1)
def get_prospects():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    if api_key and base_id and Table:
        try:
            return Table(api_key, base_id, PROSPECTS_TABLE)
        except Exception as e:
            print(f"⚠️ MessageProcessor: failed to init Prospects → {e}")
    return None


# --- Core Processor ---
class MessageProcessor:
    @staticmethod
    def send(
        phone: str,
        body: str,
        lead_id: str | None = None,
        property_id: str | None = None,
        direction: str = "OUT",
    ) -> dict:
        """
        Core message processor:
          - Sends SMS via TextGrid
          - Logs into Conversations (if Airtable available)
          - Updates Leads with activity
          - Handles retries on failure
        """
        if not phone or not body:
            return {"status": "skipped", "reason": "missing phone or body"}

        try:
            # --- Send SMS ---
            send_result = send_message(phone, body)

            # --- Log to Conversations ---
            payload = {
                FROM_FIELD: phone,
                MSG_FIELD: body,
                DIR_FIELD: direction,
                STATUS_FIELD: "SENT",
                SENT_AT: datetime.now(timezone.utc).isoformat(),
            }
            if lead_id:
                payload["lead_id"] = [lead_id]
            if property_id:
                payload["Property ID"] = property_id

            convos = get_convos()
            record = convos.create(payload) if convos else {"id": "mock_convo"}
            if not convos:
                print(f"[MOCK] Would log conversation → {payload}")

            # --- Update Lead ---
            if lead_id:
                now_iso = datetime.now(timezone.utc).isoformat()
                leads = get_leads()
                if leads:
                    leads.update(
                        lead_id,
                        {
                            "Last Outbound": now_iso if direction == "OUT" else None,
                            "Last Activity": now_iso,
                            "Last Message": body[:500],
                            "Property ID": property_id,
                        },
                    )
                else:
                    print(f"[MOCK] Would update lead {lead_id} with activity")

            print(
                f"📤 {direction} → {phone} | Body: {body} | Property: {property_id or 'N/A'}"
            )
            return {
                "status": "sent",
                "phone": phone,
                "body": body,
                "record_id": record.get("id"),
                "property_id": property_id,
                "textgrid": send_result,
            }

        except Exception as e:
            if "record" in locals() and record and record.get("id"):
                handle_retry(record["id"], str(e))
            print(f"❌ Failed sending to {phone}: {e}")
            return {"status": "failed", "phone": phone, "error": str(e)}
