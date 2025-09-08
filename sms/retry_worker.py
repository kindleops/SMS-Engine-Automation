import os
from datetime import datetime
from pyairtable import Table
from sms.textgrid_sender import send_message

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_LEADS_CONVOS_BASE = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

convos = Table(AIRTABLE_API_KEY, AIRTABLE_LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)


def retry_failed(limit=50):
    """
    Reattempt sending for records with status RETRY or GAVE_UP (once).
    """
    records = convos.all(view="Retries Needed")[:limit]  # 🔑 Make Airtable view filter RETRY/GAVE_UP
    retried = 0

    for r in records:
        try:
            fields = r.get("fields", {})
            phone = fields.get("phone")
            message = fields.get("message")

            if not phone or not message:
                continue

            # Try sending again
            send_message(phone, message)

            # Update record
            convos.update(r["id"], {
                "status": "RETRIED-SUCCESS",
                "retried_at": str(datetime.utcnow())
            })

            print(f"🔄 Retried {phone} successfully")
            retried += 1

        except Exception as e:
            convos.update(r["id"], {
                "status": "RETRIED-FAILED",
                "retry_error": str(e),
                "retried_at": str(datetime.utcnow())
            })
            print(f"❌ Retry failed for {r.get('id')} ({phone}): {e}")

    print(f"📊 Retry worker finished — {retried}/{len(records)} retried successfully")
    return {"retried": retried}


if __name__ == "__main__":
    retry_failed()