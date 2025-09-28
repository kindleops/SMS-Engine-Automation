# sms/reset_daily_stats.py
import os
from datetime import datetime, timezone
from pyairtable import Table

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

if not AIRTABLE_API_KEY or not CONTROL_BASE:
    raise RuntimeError("⚠️ Missing Airtable config for Numbers reset script")

tbl = Table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)

# --- Field Mapping ---
FIELD_NUMBER = os.getenv("NUMBERS_FIELD_NUMBER", "Number")
FIELD_MARKET = os.getenv("NUMBERS_FIELD_MARKET", "Market")
FIELD_LAST_USED = os.getenv("NUMBERS_FIELD_LAST_USED", "Last Used")
FIELD_COUNT = os.getenv("NUMBERS_FIELD_COUNT", "Count")
FIELD_REMAINING = os.getenv("NUMBERS_FIELD_REMAINING", "Remaining")
FIELD_SENT = os.getenv("NUMBERS_FIELD_SENT", "Sent")
FIELD_DELIVERED = os.getenv("NUMBERS_FIELD_DELIVERED", "Delivered")
FIELD_FAILED = os.getenv("NUMBERS_FIELD_FAILED", "Failed")

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))


# --- Reset Logic ---
def reset_daily_stats():
    """Reset all Numbers counters at start of a new day."""
    today = datetime.now(timezone.utc).isoformat()
    records = tbl.all()

    for rec in records:
        try:
            updates = {
                FIELD_COUNT: 0,
                FIELD_REMAINING: DAILY_LIMIT,
                FIELD_SENT: 0,
                FIELD_DELIVERED: 0,
                FIELD_FAILED: 0,
                FIELD_LAST_USED: today,
            }
            tbl.update(rec["id"], updates)
            print(
                f"🔄 Reset stats for {rec['fields'].get(FIELD_NUMBER)} in {rec['fields'].get(FIELD_MARKET)}"
            )
        except Exception as e:
            print(f"⚠️ Failed reset for {rec.get('id')}: {e}")

    print("✅ Daily number stats reset complete")


if __name__ == "__main__":
    reset_daily_stats()
