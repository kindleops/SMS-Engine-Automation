# sms/kpi_logger.py
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
        return Table(AIRTABLE_KEY, PERF_BASE, "KPIs")
    except Exception:
        traceback.print_exc()
        return None


def log_kpi(
    metric: str, value: int | float, campaign: str = "ALL", overwrite: bool = False
):
    """
    Write a single KPI row into the KPIs table.

    Example:
        log_kpi("OUTBOUND_SENT", 125)

    Args:
        metric (str): KPI metric name (e.g. "OUTBOUND_SENT").
        value (int | float): KPI value (auto-cast to int).
        campaign (str): Campaign name, default "ALL".
        overwrite (bool): If True, update today's KPI instead of creating a new one.
    """
    kpi_tbl = _get_table()
    if not kpi_tbl:
        print(f"⚠️ Skipping KPI {metric}, Airtable not configured")
        return

    try:
        today = datetime.now(timezone.utc).date().isoformat()
        timestamp = datetime.now(timezone.utc).isoformat()
        val = int(value) if value is not None else 0

        # Optional overwrite logic: avoid duplicate KPI rows per day
        if overwrite:
            try:
                existing = kpi_tbl.all(
                    formula=f"AND({{Metric}}='{metric}', {{Date}}='{today}', {{Campaign}}='{campaign}')"
                )
                if existing:
                    rec_id = existing[0]["id"]
                    kpi_tbl.update(rec_id, {"Value": val, "Timestamp": timestamp})
                    print(f"📊 Updated KPI → {metric}: {val} (overwrite)")
                    return
            except Exception:
                traceback.print_exc()

        # Default: append new KPI row
        record = {
            "Campaign": campaign,
            "Metric": metric,
            "Value": val,
            "Date": today,
            "Timestamp": timestamp,
        }
        kpi_tbl.create(record)
        print(f"📊 Logged KPI → {metric}: {val}")

    except Exception as e:
        print(f"❌ Failed to log KPI {metric}: {e}")
        traceback.print_exc()
