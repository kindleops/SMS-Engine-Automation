from datetime import datetime, timedelta, timezone
from pyairtable import Table
import os

# Airtable setup (Conversations lives in Leads/Convos base)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)


def handle_retry(record_id: str, error: str, max_retries: int = 3, cooldown_minutes: int = 30):
    """
    Mark a conversation record for retry.
    - Increments retry_count
    - Logs error + timestamp
    - Sets retry_after with cooldown
    - Marks GAVE_UP if max_retries exceeded
    """
    try:
        rec = convos.get(record_id)
        f = rec.get("fields", {})

        retries = (f.get("retry_count") or 0) + 1
        status = "RETRY" if retries < max_retries else "GAVE_UP"

        updates = {
            "status": status,
            "retry_count": retries,
            "last_retry_error": error,
            "last_retry_at": datetime.now(timezone.utc).isoformat()
        }

        if status == "RETRY":
            updates["retry_after"] = (datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)).isoformat()

        convos.update(record_id, updates)

        print(f"🔄 RetryHandler → {record_id}: {status} (attempt {retries})")
        return status

    except Exception as e:
        print(f"❌ RetryHandler failed for {record_id}: {e}")
        return "ERROR"