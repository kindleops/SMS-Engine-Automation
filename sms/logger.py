# sms/logger.py
import os
import traceback
from datetime import datetime, timezone
from pyairtable import Table

AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")


def _get_table():
    if not (AIRTABLE_KEY and PERF_BASE):
        return None
    try:
        return Table(AIRTABLE_KEY, PERF_BASE, "Runs/Logs")
    except Exception:
        traceback.print_exc()
        return None


def log_run(
    run_type: str,
    processed: int = 0,
    breakdown: dict | str | None = None,
    status: str = "OK",
    campaign: str | None = None,
    extra: dict | None = None,
):
    """
    Write a row into Runs/Logs table in Performance base.

    Args:
        run_type (str): Type of run (e.g. "CAMPAIGN_RUN", "AUTORESPONDER", "INBOUND").
        processed (int): Number of records processed.
        breakdown (dict|str|None): Any extra breakdown/debug info.
        status (str): Status flag (OK, ERROR, PARTIAL, etc).
        campaign (str|None): Optional campaign name for traceability.
        extra (dict|None): Extra fields to attach into the record.
    """
    tbl = _get_table()
    if not tbl:
        print(f"⚠️ Skipping run log for {run_type}, Airtable not configured")
        return

    try:
        record = {
            "Type": run_type,
            "Processed": processed,
            "Breakdown": str(breakdown or {}),
            "Status": status,
            "Timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if campaign:
            record["Campaign"] = campaign
        if extra:
            record.update(extra)

        tbl.create(record)
        print(f"📝 Logged run → {run_type} | {status} | {processed} processed")
    except Exception as e:
        print(f"❌ Failed to log run {run_type}: {e}")
        traceback.print_exc()
