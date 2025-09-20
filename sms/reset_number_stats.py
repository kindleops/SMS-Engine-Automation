import os
from datetime import datetime, timezone
from pyairtable import Table

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

FIELD_NUMBER    = os.getenv("NUMBERS_FIELD_NUMBER", "Number")
FIELD_MARKET    = os.getenv("NUMBERS_FIELD_MARKET", "Market")
FIELD_LAST_USED = os.getenv("NUMBERS_FIELD_LAST_USED", "Last Used")
FIELD_COUNT     = os.getenv("NUMBERS_FIELD_COUNT", "Count")
FIELD_REMAINING = os.getenv("NUMBERS_FIELD_REMAINING", "Remaining")
FIELD_SENT      = os.getenv("NUMBERS_FIELD_SENT", "Sent")
FIELD_DELIVERED = os.getenv("NUMBERS_FIELD_DELIVERED", "Delivered")
FIELD_FAILED    = os.getenv("NUMBERS_FIELD_FAILED", "Failed")

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "750"))

def reset_daily_stats():
    tbl = Table(AIRTABLE_API_KEY, CONTROL_BASE, NUMBERS_TABLE)
    records = tbl.all()

    for rec in records:
        try:
            tbl.update(rec["id"], {
                FIELD_COUNT: 0,
                FIELD_REMAINING: DAILY_LIMIT,
                FIELD_SENT: 0,
                FIELD_DELIVERED: 0,
                FIELD_FAILED: 0,
                FIELD_LAST_USED: datetime.now(timezone.utc).isoformat()
            })
            print(f"🔄 Reset stats for {rec['fields'].get(FIELD_NUMBER)}")
        except Exception as e:
            print(f"⚠️ Failed reset for {rec['id']}: {e}")

if __name__ == "__main__":
    reset_daily_stats()