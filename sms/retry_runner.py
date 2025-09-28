# sms/retry_runner.py
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from sms.textgrid_sender import send_message

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- Retry Config ---
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_BACKOFF_MINUTES = int(os.getenv("BASE_BACKOFF_MINUTES", "30"))

# --- Field Mapping ---
PHONE_FIELD = os.getenv("CONV_FROM_FIELD", "phone")
MESSAGE_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIRECTION_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")

RETRY_COUNT_FIELD = os.getenv("CONV_RETRY_COUNT_FIELD", "retry_count")
RETRY_AFTER_FIELD = os.getenv("CONV_RETRY_AFTER_FIELD", "retry_after")
RETRIED_AT_FIELD = os.getenv("CONV_RETRIED_AT_FIELD", "retried_at")
LAST_ERROR_FIELD = os.getenv("CONV_LAST_ERROR_FIELD", "last_retry_error")
PERM_FAIL_REASON = os.getenv("CONV_PERM_FAIL_FIELD", "permanent_fail_reason")


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
            print(f"❌ RetryRunner: failed to init Conversations table → {e}")
            return None
    print("⚠️ RetryRunner: No Airtable config → running in MOCK mode")
    return None


# --- Formula (OUT + failed + retryable) ---
FORMULA = f"""
AND(
  {{{DIRECTION_FIELD}}}='OUT',
  OR({{{STATUS_FIELD}}}='FAILED', {{{STATUS_FIELD}}}='DELIVERY_FAILED', {{{STATUS_FIELD}}}='THROTTLED'),
  OR({{{RETRY_COUNT_FIELD}}}=BLANK(), {{{RETRY_COUNT_FIELD}}}<{MAX_RETRIES}),
  OR({{{RETRY_AFTER_FIELD}}}=BLANK(), {{{RETRY_AFTER_FIELD}}}<=NOW())
)
""".strip()


# --- Helpers ---
def _backoff_delay(retry_count: int) -> timedelta:
    return timedelta(minutes=BASE_BACKOFF_MINUTES * (2 ** (retry_count - 1)))


def _is_permanent_error(err: str) -> bool:
    signals = [
        "invalid",
        "not a valid",
        "unreachable",
        "blacklisted",
        "blocked",
        "landline",
        "disconnected",
        "undeliverable",
    ]
    return any(sig in err.lower() for sig in signals)


# --- Main ---
def run_retry(limit: int = 100, view: str | None = None):
    convos = get_convos()
    if not convos:
        print("⚠️ RetryRunner: Skipping because Airtable is not configured")
        return {"ok": False, "retried": 0, "failed": 0, "permanent": 0, "limit": limit}

    records = (
        convos.all(view=view)[:limit] if view else convos.all(formula=FORMULA)[:limit]
    )

    retried, failed, permanent = 0, 0, 0

    for r in records:
        f = r.get("fields", {})
        phone = f.get(PHONE_FIELD)
        body = f.get(MESSAGE_FIELD)
        retry_count = f.get(RETRY_COUNT_FIELD, 0) or 0

        if not phone or not body:
            continue

        try:
            send_message(phone, body)
            convos.update(
                r["id"],
                {
                    STATUS_FIELD: "SENT",
                    RETRY_COUNT_FIELD: retry_count + 1,
                    RETRIED_AT_FIELD: datetime.now(timezone.utc).isoformat(),
                },
            )
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
                update[PERM_FAIL_REASON] = err_msg
                permanent += 1
                print(f"🚨 Permanent fail → {phone}: {err_msg}")

            elif new_count >= MAX_RETRIES:
                update[STATUS_FIELD] = "GAVE_UP"
                print(f"🚨 Giving up on {phone} after {new_count} retries: {err_msg}")

            else:
                backoff = _backoff_delay(new_count)
                update[RETRY_AFTER_FIELD] = (
                    datetime.now(timezone.utc) + backoff
                ).isoformat()
                update[STATUS_FIELD] = "NEEDS_RETRY"
                print(
                    f"⚠️ Retry failed → {phone} | Next attempt after {backoff}: {err_msg}"
                )

            convos.update(r["id"], update)
            failed += 1

    print(
        f"🔁 Retry runner done | ✅ Retried: {retried} | ❌ Fails: {failed} | 🚫 Permanent: {permanent}"
    )
    return {
        "ok": True,
        "retried": retried,
        "failed": failed,
        "permanent": permanent,
        "limit": limit,
    }


if __name__ == "__main__":
    run_retry()
