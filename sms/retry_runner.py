# sms/retry_runner.py
import os, time
from datetime import datetime, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_LEADS_CONVOS_BASE_ID = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

convos = Table(AIRTABLE_API_KEY, AIRTABLE_LEADS_CONVOS_BASE_ID, CONVERSATIONS_TABLE)

# Airtable formula for a "Needs Retry" pull if you don't want to use a View:
FORMULA = """
AND(
  {direction} = 'OUT',
  OR({status} = 'FAILED', {status} = 'DELIVERY_FAILED', {status} = 'THROTTLED'),
  OR({retry_count} = BLANK(), {retry_count} < 3),
  OR({retry_after} = BLANK(), DATETIME_PARSE({retry_after}) <= NOW())
)
"""

def run_retry(limit: int = 100, view: str | None = None):
    records = []
    if view:
        # Preferred: drive from a curated Airtable View named "Needs Retry"
        records = convos.all(view=view)[:limit]
    else:
        # Fallback: use formula if you didn't make the view
        records = convos.all(formula=FORMULA)[:limit]

    retried = 0
    for r in records:
        f = r.get("fields", {})
        phone = f.get("phone")
        body  = f.get("message")
        if not phone or not body:
            continue

        try:
            send_message(phone, body)
            convos.update(r["id"], {
                "status": "RETRIED",
                "retry_count": (f.get("retry_count") or 0) + 1,
                "retried_at": datetime.now(timezone.utc).isoformat()
            })
            retried += 1
        except Exception as e:
            convos.update(r["id"], {
                "status": "FAILED",
                "retry_count": (f.get("retry_count") or 0) + 1,
                "last_error": str(e),
                # back off 30 min
                "retry_after": (datetime.now(timezone.utc)
                                .replace(microsecond=0)).isoformat()
            })

    print(f"🔁 Retry runner processed {retried} messages")
    return {"retried": retried}