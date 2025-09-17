import os
from datetime import datetime, timedelta, timezone
from pyairtable import Table

# --- Airtable Setup ---
AIRTABLE_API_KEY    = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE   = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

if not AIRTABLE_API_KEY or not LEADS_CONVOS_BASE:
    raise RuntimeError("⚠️ Missing Airtable env for Conversations table")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)

# --- Field Mapping (env-driven, with safe defaults) ---
STATUS_FIELD       = os.getenv("CONV_STATUS_FIELD", "status")
RETRY_COUNT_FIELD  = os.getenv("CONV_RETRY_COUNT_FIELD", "retry_count")
RETRY_AFTER_FIELD  = os.getenv("CONV_RETRY_AFTER_FIELD", "retry_after")
LAST_ERROR_FIELD   = os.getenv("CONV_LAST_ERROR_FIELD", "last_retry_error")
LAST_RETRY_AT      = os.getenv("CONV_LAST_RETRY_AT_FIELD", "last_retry_at")

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

        retries = (f.get(RETRY_COUNT_FIELD) or 0) + 1
        status = "RETRY" if retries < max_retries else "GAVE_UP"

        updates = {
            STATUS_FIELD: status,
            RETRY_COUNT_FIELD: retries,
            LAST_ERROR_FIELD: error,
            LAST_RETRY_AT: datetime.now(timezone.utc).isoformat()
        }

        if status == "RETRY":
            updates[RETRY_AFTER_FIELD] = (
                datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)
            ).isoformat()

        convos.update(record_id, updates)

        print(f"🔄 RetryHandler → {record_id}: {status} (attempt {retries})")
        return status

    except Exception as e:
        print(f"❌ RetryHandler failed for {record_id}: {e}")
        return "ERROR"