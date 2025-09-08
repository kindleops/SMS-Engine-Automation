from datetime import datetime
from pyairtable import Table
import os

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

convos = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, CONVERSATIONS_TABLE)


def handle_retry(record_id: str, error: str, max_retries: int = 3):
    """Mark record for retry with error logging"""
    record = convos.get(record_id)
    retries = record["fields"].get("retries", 0) + 1

    status = "RETRY" if retries < max_retries else "GAVE_UP"

    convos.update(record_id, {
        "status": status,
        "retry_count": retries,
        "last_retry_error": error,
        "last_retry_at": str(datetime.utcnow())
    })

    print(f"🔄 RetryHandler → {record_id}: {status} (attempt {retries})")
    return status