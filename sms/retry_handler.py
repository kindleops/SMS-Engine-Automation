# sms/retry_handler.py
import os
from datetime import datetime, timedelta, timezone
from typing import Literal
from functools import lru_cache

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- Field Mapping (env-driven, with safe defaults) ---
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
RETRY_COUNT_FIELD = os.getenv("CONV_RETRY_COUNT_FIELD", "retry_count")
RETRY_AFTER_FIELD = os.getenv("CONV_RETRY_AFTER_FIELD", "retry_after")
LAST_ERROR_FIELD = os.getenv("CONV_LAST_ERROR_FIELD", "last_retry_error")
LAST_RETRY_AT = os.getenv("CONV_LAST_RETRY_AT_FIELD", "last_retry_at")


# --- Lazy Airtable Client ---
@lru_cache(maxsize=1)
def get_convos():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv(
        "AIRTABLE_LEADS_CONVOS_BASE_ID"
    )
    table = os.getenv("CONVERSATIONS_TABLE", "Conversations")

    if api_key and base_id and Table:
        try:
            return Table(api_key, base_id, table)
        except Exception as e:
            print(f"❌ RetryHandler: failed to init Conversations table → {e}")
            return None
    print("⚠️ RetryHandler: No Airtable config → running in MOCK mode")
    return None


def handle_retry(
    record_id: str, error: str, max_retries: int = 3, cooldown_minutes: int = 30
) -> Literal["NEEDS_RETRY", "GAVE_UP", "ERROR", "MOCK"]:
    """
    Mark a conversation record for retry in Airtable.

    Effects:
    - Increments `retry_count`
    - Updates `last_retry_error` + `last_retry_at`
    - Sets `retry_after` to now + cooldown if under max_retries
    - Sets `status` to NEEDS_RETRY or GAVE_UP
    - If Airtable is not configured → logs + returns "MOCK"
    """
    convos = get_convos()
    if not convos:
        print(f"[MOCK] RetryHandler → would retry record={record_id}, error={error}")
        return "MOCK"

    try:
        rec = convos.get(record_id)
        if not rec:
            print(f"⚠️ RetryHandler → Record {record_id} not found")
            return "ERROR"

        fields = rec.get("fields", {})
        retries = (fields.get(RETRY_COUNT_FIELD) or 0) + 1

        status: Literal["NEEDS_RETRY", "GAVE_UP"]
        status = "NEEDS_RETRY" if retries < max_retries else "GAVE_UP"

        updates = {
            STATUS_FIELD: status,
            RETRY_COUNT_FIELD: retries,
            LAST_ERROR_FIELD: error,
            LAST_RETRY_AT: datetime.now(timezone.utc).isoformat(),
        }

        if status == "NEEDS_RETRY":
            updates[RETRY_AFTER_FIELD] = (
                datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)
            ).isoformat()

        convos.update(record_id, updates)

        print(f"🔄 RetryHandler → {record_id}: {status} (attempt {retries})")
        return status

    except Exception as e:
        print(f"❌ RetryHandler failed for {record_id}: {e}")
        return "ERROR"
