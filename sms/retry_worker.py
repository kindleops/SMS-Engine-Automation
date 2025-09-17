# sms/retry_worker.py
import os
from datetime import datetime, timedelta, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

# --- Airtable setup ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

if not AIRTABLE_API_KEY or not LEADS_CONVOS_BASE:
    raise RuntimeError("⚠️ Missing Airtable config for Conversations")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)

# --- Config ---
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))

# Airtable filter (if no view is set)
FORMULA = f"""
AND(
  {{Direction}} = 'OUT',
  OR({{Status}} = 'FAILED', {{Status}} = 'RETRY', {{Status}} = 'DELIVERY_FAILED'),
  OR({{Retry Count}} = BLANK(), {{Retry Count}} < {MAX_RETRIES}),
  OR({{Retry After}} = BLANK(), {{Retry After}} <= NOW())
)
"""

def _backoff_delay(retry_count: int) -> timedelta:
    """Exponential backoff: 30m, 60m, 120m..."""
    return timedelta(minutes=BASE_BACKOFF_MINUTES * (2 ** (retry_count - 1)))

def retry_failed(limit: int = 50, view: str | None = None):
    """
    Retry outbound messages that previously failed.
    Respects Retry Count + Retry After for throttling.
    """
    if view:
        records = convos.all(view=view)[:limit]
    else:
        records = convos.all(formula=FORMULA)[:limit]

    retried = 0
    failed = 0

    for r in records:
        f = r.get("fields", {})
        phone = f.get("phone")
        body = f.get("message")
        retry_count = f.get("retry_count", 0) or 0

        if not phone or not body:
            continue

        try:
            send_message(phone, body)
            convos.update(r["id"], {
                "status": "RETRIED-SUCCESS",
                "retry_count": retry_count + 1,
                "last_retry_at": datetime.now(timezone.utc).isoformat()
            })
            retried += 1
            print(f"📤 Retried → {phone} | Retry #{retry_count + 1}")

        except Exception as e:
            new_count = retry_count + 1
            update = {
                "retry_count": new_count,
                "last_retry_error": str(e),
                "last_retry_at": datetime.now(timezone.utc).isoformat(),
            }

            if new_count >= MAX_RETRIES:
                update["status"] = "GAVE_UP"
                print(f"🚨 Giving up on {phone} after {new_count} retries: {e}")
            else:
                backoff = _backoff_delay(new_count)
                update["retry_after"] = (datetime.now(timezone.utc) + backoff).isoformat()
                update["status"] = "RETRY"
                print(f"⚠️ Retry failed → {phone} | Will retry after {backoff}: {e}")

            convos.update(r["id"], update)
            failed += 1

    print(f"🔁 Retry worker finished | ✅ Success: {retried} | ❌ Still failing: {failed}")
    return {"retried": retried, "failed": failed, "limit": limit}


if __name__ == "__main__":
    retry_failed()