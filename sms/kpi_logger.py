# sms/kpi_logger.py
import os
from datetime import datetime, timezone
from pyairtable import Table
import traceback

AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")

def log_kpi(metric: str, value: int, campaign: str = "ALL"):
    """
    Write a single KPI row into the KPIs table.
    Example: log_kpi("OUTBOUND_SENT", 125)
    """
    if not (AIRTABLE_KEY and PERF_BASE):
        print(f"⚠️ Skipping KPI {metric}, Airtable not configured")
        return

    try:
        tbl = Table(AIRTABLE_KEY, PERF_BASE, "KPIs")
        record = {
            "Campaign": campaign,
            "Metric": metric,
            "Value": value,
            "Date": datetime.now(timezone.utc).date().isoformat(),
            "Timestamp": datetime.now(timezone.utc).isoformat(),
        }
        tbl.create(record)
        print(f"📊 Logged KPI → {metric}: {value}")
    except Exception as e:
        print(f"❌ Failed to log KPI {metric}: {e}")
        traceback.print_exc()