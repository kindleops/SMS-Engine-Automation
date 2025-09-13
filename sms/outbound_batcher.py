# sms/outbound_batcher.py
import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.quota_reset import reset_daily_quotas

# --- Config ---
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

_last_reset_date = None  # safeguard for daily quota reset


def get_numbers_table() -> Table:
    """
    Lazy initializer for the Airtable Numbers table.
    Throws RuntimeError if env vars aren't set.
    """
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")

    if not api_key or not base_id:
        raise RuntimeError("⚠️ Missing Airtable config: AIRTABLE_API_KEY or CAMPAIGN_CONTROL_BASE")

    return Table(api_key, base_id, NUMBERS_TABLE)


def ensure_today_rows():
    """
    Auto-reset daily quotas once per day across all numbers.
    """
    global _last_reset_date
    today = datetime.now(timezone.utc).date().isoformat()

    if _last_reset_date != today:
        print(f"⚡ Auto-resetting quotas for {today}")
        reset_daily_quotas()
        _last_reset_date = today


def send_batch(limit: int = 50):
    """
    Main outbound batching loop:
    - Ensures quotas are reset for the day
    - Pulls Numbers table
    - Decrements Remaining quota for each number used
    """
    ensure_today_rows()
    numbers = get_numbers_table()

    results = []
    sent = 0

    # Fetch pool of available numbers
    available = numbers.all(max_records=limit)

    for n in available:
        f = n.get("fields", {})
        remaining = f.get("Remaining", 0)
        phone = f.get("Number")

        if remaining and remaining > 0:
            # Decrement and mark usage
            numbers.update(n["id"], {
                "Remaining": remaining - 1,
                "Last Used": datetime.now(timezone.utc).isoformat()
            })
            sent += 1
            results.append({"number": phone, "status": "sent"})

    return {"total_sent": sent, "results": results}