import os
from datetime import datetime, timezone
from pyairtable import Table
from sms.quota_reset import reset_daily_quotas
import traceback

# --- Config ---
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

_last_reset_date = None  # safeguard for daily quota reset


def get_numbers_table() -> Table | None:
    """Lazy initializer for Airtable Numbers table."""
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")

    if not api_key or not base_id:
        print("⚠️ Missing Airtable config: AIRTABLE_API_KEY or CAMPAIGN_CONTROL_BASE")
        return None

    try:
        return Table(api_key, base_id, NUMBERS_TABLE)
    except Exception:
        print("❌ Failed to init Numbers table")
        traceback.print_exc()
        return None


def ensure_today_rows():
    """Auto-reset daily quotas once per day across all numbers."""
    global _last_reset_date
    today = datetime.now(timezone.utc).date().isoformat()

    if _last_reset_date != today:
        print(f"⚡ Auto-resetting quotas for {today}")
        reset_daily_quotas()
        _last_reset_date = today


def send_batch(limit: int = 50):
    """
    Outbound batching loop:
    - Ensures quotas are reset
    - Pulls Numbers table rows with Remaining > 0
    - Decrements Remaining + increments Count
    """
    ensure_today_rows()
    numbers = get_numbers_table()
    if not numbers:
        return {"ok": False, "error": "Numbers table not available"}

    results = []
    sent = 0

    try:
        available = numbers.all(
            formula="{Remaining} > 0",
            max_records=limit,
        )

        for n in available:
            f = n.get("fields", {})
            phone = f.get("Number")
            remaining = f.get("Remaining", 0)
            count = f.get("Count", 0)

            if not phone:
                continue

            if remaining and remaining > 0:
                try:
                    numbers.update(n["id"], {
                        "Remaining": remaining - 1,
                        "Count": count + 1,
                        # Write ISO datetime so Airtable Date fields stay happy
                        "Last Used": datetime.now(timezone.utc).isoformat(),
                    })
                    sent += 1
                    results.append({"number": phone, "status": "sent", "remaining": remaining - 1})
                except Exception as e:
                    print(f"❌ Failed to decrement quota for {phone}: {e}")
                    traceback.print_exc()
                    results.append({"number": phone, "status": "error", "error": str(e)})
            else:
                results.append({"number": phone, "status": "skipped", "reason": "no quota"})
    except Exception as e:
        print("❌ Error in send_batch:", e)
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    return {"ok": True, "total_sent": sent, "results": results}