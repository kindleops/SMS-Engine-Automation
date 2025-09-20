# sms/logger.py
import os
from datetime import datetime, timezone
from pyairtable import Table
import traceback

AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")

def log_run(run_type: str, processed: int = 0, breakdown: dict | str = None, status: str = "OK"):
    """
    Write a row into Runs/Logs table in Performance base.
    """
    if not (AIRTABLE_KEY and PERF_BASE):
        print(f"⚠️ Skipping log for {run_type}, Airtable not configured")
        return

    try:
        tbl = Table(AIRTABLE_KEY, PERF_BASE, "Runs/Logs")
        record = {
            "Type": run_type,
            "Processed": processed,
            "Breakdown": str(breakdown or {}),
            "Status": status,
            "Timestamp": datetime.now(timezone.utc).isoformat(),
        }
        tbl.create(record)
        print(f"📝 Logged run → {run_type} | {status}")
    except Exception as e:
        print(f"❌ Failed to log run {run_type}: {e}")
        traceback.print_exc()