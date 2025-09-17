import os
from datetime import datetime, timedelta, timezone
from pyairtable import Table
from sms.textgrid_sender import send_message

# --- Airtable Config ---
AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE    = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")

convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)

# --- Retry Config ---
MAX_RETRIES          = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))

# --- Field Mapping ---
PHONE_FIELD              = os.getenv("CONV_FROM_FIELD", "phone")
MESSAGE_FIELD            = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD             = os.getenv("CONV_STATUS_FIELD", "status")
DIRECTION_FIELD          = os.getenv("CONV_DIRECTION_FIELD", "direction")

RETRY_COUNT_FIELD        = os.getenv("CONV_RETRY_COUNT_FIELD", "retry_count")
RETRY_AFTER_FIELD        = os.getenv("CONV_RETRY_AFTER_FIELD", "retry_after")
RETRIED_AT_FIELD         = os.getenv("CONV_RETRIED_AT_FIELD", "retried_at")
LAST_ERROR_FIELD         = os.getenv("CONV_LAST_ERROR_FIELD", "last_retry_error")
PERMANENT_FAIL_REASON    = os.getenv("CONV_PERM_FAIL_FIELD", "permanent_fail_reason")

# --- Formula (fetch OUT + failed) ---
FORMULA = f"""
AND(
  {{{DIRECTION_FIELD}}} = 'OUT',
  OR({{{STATUS_FIELD}}} = 'FAILED', {{{STATUS_FIELD}}} = 'DELIVERY_FAILED', {{{STATUS_FIELD}}} = 'THROTTLED'),
  OR({{{RETRY_COUNT_FIELD}}} = BLANK(), {{{RETRY_COUNT_FIELD}}} < {MAX_RETRIES}),
  OR({{{RETRY_AFTER_FIELD}}} = BLANK(), {{{RETRY_AFTER_FIELD}}} <= NOW())
)
""".strip()

# --- Helpers ---
def _backoff_delay(retry_count: int) -> timedelta:
    """Exponential backoff: 30m, 60m, 120m..."""
    return timedelta(minutes=BASE_BACKOFF_MINUTES * (2 ** (retry_count - 1)))

def _is_permanent_error(err: str) -> bool:
    """Check if error is non-retryable (invalid/blocked/disconnected)."""
    hard_signals = [
        "invalid", "not a valid", "unreachable", "blacklisted",
        "blocked", "landline", "disconnected", "undeliverable"
    ]
    err_lc = err.lower()
    return any(sig in err_lc for sig in hard_signals)

# --- Main ---
def run_retry(limit: int = 100, view: str | None = None):
    if view:
        records = convos.all(view=view)[:limit]
    else:
        records = convos.all(formula=FORMULA)[:limit]

    retried, failed = 0, 0

    for r in records:
        f = r.get("fields", {})
        phone = f.get(PHONE_FIELD)
        body  = f.get(MESSAGE_FIELD)
        retry_count = f.get(RETRY_COUNT_FIELD, 0) or 0

        if not phone or not body:
            continue

        try:
            send_message(phone, body)
            convos.update(r["id"], {
                STATUS_FIELD: "SENT",
                RETRY_COUNT_FIELD: retry_count + 1,
                RETRIED_AT_FIELD: datetime.now(timezone.utc).isoformat()
            })
            retried += 1
            print(f"📤 Retried → {phone} | Retry #{retry_count + 1}")

        except Exception as e:
            err_msg = str(e)
            new_count = retry_count + 1
            update = {
                RETRY_COUNT_FIELD: new_count,
                LAST_ERROR_FIELD: err_msg,
            }

            if _is_permanent_error(err_msg):
                update[STATUS_FIELD] = "GAVE_UP"
                update[PERMANENT_FAIL_REASON] = err_msg
                print(f"🚨 Permanent error → {phone}: {err_msg} | Logged + skipped retries")

            elif new_count >= MAX_RETRIES:
                update[STATUS_FIELD] = "GAVE_UP"
                print(f"🚨 Giving up on {phone} after {new_count} retries: {err_msg}")

            else:
                backoff = _backoff_delay(new_count)
                update[RETRY_AFTER_FIELD] = (datetime.now(timezone.utc) + backoff).isoformat()
                update[STATUS_FIELD] = "NEEDS_RETRY"
                print(f"⚠️ Retry failed → {phone} | Will retry after {backoff}: {err_msg}")

            convos.update(r["id"], update)
            failed += 1

    print(f"🔁 Retry runner finished | ✅ Retried: {retried} | ❌ Still failing: {failed}")
    return {"retried": retried, "failed": failed, "limit": limit}