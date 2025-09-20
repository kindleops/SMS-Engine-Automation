# sms/kpi_aggregator.py
import os
from datetime import datetime, timedelta, timezone
from pyairtable import Table
import traceback
import calendar

AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")

def _get_tables():
    if not (AIRTABLE_KEY and PERF_BASE):
        return None
    try:
        return Table(AIRTABLE_KEY, PERF_BASE, "KPIs")
    except Exception:
        traceback.print_exc()
        return None

def aggregate_kpis():
    """
    Aggregate KPIs into daily, weekly, and monthly totals.
    - Query all KPI rows
    - Sum by metric
    - Insert *_DAILY_TOTAL, *_WEEKLY_TOTAL, *_MONTHLY_TOTAL rows
    """
    kpi_tbl = _get_tables()
    if not kpi_tbl:
        return {"ok": False, "error": "Airtable KPI table not configured"}

    today = datetime.now(timezone.utc).date()
    start_week = today - timedelta(days=7)
    start_month = today.replace(day=1)

    try:
        rows = kpi_tbl.all()
        daily_totals, weekly_totals, monthly_totals = {}, {}, {}

        for r in rows:
            f = r.get("fields", {})
            metric = f.get("Metric")
            value = f.get("Value") or 0
            date_str = f.get("Date")
            if not (metric and date_str):
                continue

            try:
                date_obj = datetime.fromisoformat(date_str).date()
            except Exception:
                continue

            # Daily
            if date_obj == today:
                daily_totals[metric] = daily_totals.get(metric, 0) + value

            # Weekly
            if date_obj >= start_week:
                weekly_totals[metric] = weekly_totals.get(metric, 0) + value

            # Monthly
            if date_obj >= start_month:
                monthly_totals[metric] = monthly_totals.get(metric, 0) + value

        timestamp = datetime.now(timezone.utc).isoformat()

        def _write_totals(suffix, totals):
            for metric, total in totals.items():
                kpi_tbl.create({
                    "Campaign": "ALL",
                    "Metric": f"{metric}_{suffix}",
                    "Value": total,
                    "Date": str(today),
                    "Timestamp": timestamp,
                })

        _write_totals("DAILY_TOTAL", daily_totals)
        _write_totals("WEEKLY_TOTAL", weekly_totals)
        _write_totals("MONTHLY_TOTAL", monthly_totals)

        return {
            "ok": True,
            "daily": daily_totals,
            "weekly": weekly_totals,
            "monthly": monthly_totals,
        }

    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}