# sms/retry_runner.py
import os
from datetime import datetime, timedelta, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

if not AIRTABLE_API_KEY or not LEADS_CONVOS_BASE:
    raise RuntimeError("⚠️ Missing Airtable env for Conversations table")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)

# --- Config ---
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))

# --- Formula (fallback if no Airtable View) ---
FORMULA = f"""
AND(
  {{Direction}} = 'OUT',
  OR({{Status}} = 'FAILED', {{Status}} = 'DELIVERY_FAILED', {{Status}} = 'THROTTLED'),
  OR({{Retry Count}} = BLANK(), {{Retry Count}} < {MAX_RETRIES}),
  OR({{Retry After}} = BLANK(), {{Retry After}} <= NOW())
)
"""

def _backoff_delay(retry_count: int) -> timedelta:
    """Exponential backoff: 30m, 60m, 120m, ..."""
    return timedelta(minutes=BASE_BACKOFF_MINUTES * (2 ** (retry_count - 1)))

def run_retry(limit: int = 100, view: str | None = None):
    """
    Retry previously failed outbound SMS:
    - Fetches rows from Conversations marked for retry
    - Attempts resend
    - Updates Airtable with retry_count, status, and backoff
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
        body  = f.get("message")
        retry_count = f.get("Retry Count", 0) or 0

        if not phone or not body:
            continue

        try:
            send_message(phone, body)
            convos.update(r["id"], {
                "Status": "RETRIED",
                "Retry Count": retry_count + 1,
                "Retried At": datetime.now(timezone.utc).isoformat()
            })
            retried += 1
            print(f"📤 Retried → {phone} | Retry #{retry_count + 1}")

        except Exception as e:
            new_count = retry_count + 1
            update = {
                "Retry Count": new_count,
                "Last Error": str(e),
            }
            if new_count >= MAX_RETRIES:
                update["Status"] = "GAVE_UP"
                print(f"🚨 Giving up on {phone} after {new_count} retries: {e}")
            else:
                backoff = _backoff_delay(new_count)
                update["Retry After"] = (datetime.now(timezone.utc) + backoff).isoformat()
                update["Status"] = "NEEDS_RETRY"
                print(f"⚠️ Retry failed → {phone} | Will retry after {backoff}: {e}")

            convos.update(r["id"], update)
            failed += 1

    print(f"🔁 Retry runner finished | ✅ Retried: {retried} | ❌ Still failing: {failed}")
    return {"retried": retried, "failed": failed, "limit": limit}