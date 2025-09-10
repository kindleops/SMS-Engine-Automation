# sms/outbound_batcher.py
import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.quota_reset import reset_daily_quotas

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
NUMBERS_TABLE = "Numbers"

numbers = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)

# --- Daily quota reset safeguard ---
_last_reset_date = None

def ensure_today_rows():
    global _last_reset_date
    today = datetime.now(timezone.utc).date().isoformat()
    if _last_reset_date != today:
        print(f"⚡ Auto-resetting quotas for {today}")
        reset_daily_quotas()
        _last_reset_date = today

# --- Example send_batch loop ---
def send_batch(limit: int = 50):
    ensure_today_rows()  # 👈 always refresh quotas once per day

    # Your normal batching logic (pick numbers, decrement quota, send SMS)
    results = []
    sent = 0

    # Example: fetch pool of active numbers
    available = numbers.all(max_records=limit)

    for n in available:
        f = n["fields"]
        remaining = f.get("Remaining", 0)
        phone = f.get("Number")

        if remaining > 0:
            # decrement and mark use
            numbers.update(n["id"], {
                "Remaining": remaining - 1,
                "Last Used": datetime.now(timezone.utc).isoformat()
            })
            sent += 1
            results.append({"number": phone, "status": "sent"})

    return {"total_sent": sent, "results": results}