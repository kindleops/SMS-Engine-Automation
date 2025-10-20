# sms/kpi_aggregator.py
from __future__ import annotations

import os
import re
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# --- Ensure Table is always defined (prevents NameError) ---
try:
    from pyairtable import Table as _RealTable
except Exception:  # pyairtable missing or import error
    _RealTable = None


class Table:  # thin wrapper so symbol 'Table' always exists
    def __init__(self, api_key: str, base_id: str, table_name: str):
        if _RealTable is None:
            raise ImportError("pyairtable is not installed or failed to import. Install with: pip install pyairtable")
        self._t = _RealTable(api_key, base_id, table_name)

    def all(self, **kwargs):
        return self._t.all(**kwargs)

    def create(self, fields: dict):
        return self._t.create(fields)

    def update(self, record_id: str, fields: dict):
        return self._t.update(record_id, fields)

    def iterate(self, **kwargs):
        if hasattr(self._t, "iterate"):
            return self._t.iterate(**kwargs)
        raise AttributeError("Underlying Table instance does not support iterate()")


# -----------------------
# ENV / CONFIG
# -----------------------
AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")
KPI_TABLE = os.getenv("KPI_TABLE_NAME", "KPIs")

# Business-timezone for daily rollups
KPI_TZ = os.getenv("KPI_TZ", "America/Chicago")
MAX_SCAN = int(os.getenv("KPI_MAX_SCAN", "10000"))  # safety cap
PAGE_SIZE = 100

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


# -----------------------
# Helpers
# -----------------------
def _tz_now():
    if ZoneInfo:
        return datetime.now(ZoneInfo(KPI_TZ))
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_date_local(s: str) -> Optional[datetime.date]:
    """
    Accepts YYYY-MM-DD or ISO timestamp (with Z or offset).
    Converts to KPI_TZ date to keep late-night events on the correct business day.
    """
    if not s:
        return None
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if ZoneInfo:
                dt = dt.astimezone(ZoneInfo(KPI_TZ))
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.date()
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def _norm(s):  # normalize field names
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


# -----------------------
# Airtable helpers
# -----------------------
def _kpi_table() -> Optional[Table]:
    if not (AIRTABLE_KEY and PERF_BASE):
        print("⚠️ KPI Aggregator: missing AIRTABLE key or PERFORMANCE_BASE")
        return None
    try:
        return Table(AIRTABLE_KEY, PERF_BASE, KPI_TABLE)
    except Exception:
        traceback.print_exc()
        return None


def _auto_field_map(tbl: Table) -> Dict[str, str]:
    try:
        rows = tbl.all(max_records=1)
        keys = list(rows[0].get("fields", {}).keys()) if rows else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl: Table, payload: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            out[mk] = v
    return out


def _fetch_kpi_rows(tbl: Table) -> List[dict]:
    """Return KPI rows using the Airtable iterator with retry and pagination."""

    attempts = 2
    last_exc: Optional[Exception] = None
    total_limit = MAX_SCAN if MAX_SCAN > 0 else None

    for attempt in range(attempts):
        try:
            rows: List[dict] = []
            fetched = 0
            iterator = tbl.iterate(page_size=PAGE_SIZE)
            for record in iterator:
                rows.append(record)
                fetched += 1
                if total_limit is not None and fetched >= total_limit:
                    break
            return rows
        except Exception as exc:  # pragma: no cover - network failure path
            last_exc = exc
            traceback.print_exc()
            if attempt == attempts - 1:
                break
    if last_exc:
        raise last_exc
    return []


# -----------------------
# Core
# -----------------------
def aggregate_kpis():
    """
    Aggregates raw KPI rows into *_DAILY_TOTAL, *_WEEKLY_TOTAL, *_MONTHLY_TOTAL by Metric.
    - Idempotent per day (updates today’s total rows if they already exist).
    - Timezone-aware (KPI_TZ, default America/Chicago).
    - Safe writes (only updates fields that exist in the table).
    """
    kpi_tbl = _kpi_table()
    if not kpi_tbl:
        return {"ok": False, "error": "KPI table not configured"}

    today_local = _tz_now().date()
    start_week = today_local - timedelta(days=7)
    start_month = today_local.replace(day=1)
    now_iso = _utcnow_iso()

    try:
        rows = _fetch_kpi_rows(kpi_tbl)
    except Exception:
        return {"ok": False, "error": "Failed to read KPI rows"}

    # Partition: raw vs existing totals for today (so we upsert, not duplicate)
    raw = []
    existing_totals_daily: Dict[str, dict] = {}
    existing_totals_weekly: Dict[str, dict] = {}
    existing_totals_monthly: Dict[str, dict] = {}

    for r in rows:
        f = r.get("fields", {})
        metric = (f.get("Metric") or "").strip()
        d = _to_date_local(str(f.get("Date") or ""))

        if not metric or not d:
            continue

        # capture already-written totals for today
        if metric.endswith("_DAILY_TOTAL") and d == today_local:
            existing_totals_daily[metric] = r
            continue
        if metric.endswith("_WEEKLY_TOTAL") and d == today_local:
            existing_totals_weekly[metric] = r
            continue
        if metric.endswith("_MONTHLY_TOTAL") and d == today_local:
            existing_totals_monthly[metric] = r
            continue

        raw.append(r)

    # Aggregate
    daily: Dict[str, float] = {}
    weekly: Dict[str, float] = {}
    monthly: Dict[str, float] = {}

    for r in raw:
        f = r.get("fields", {})
        metric = (f.get("Metric") or "").strip()
        d = _to_date_local(str(f.get("Date") or ""))

        if not metric or not d:
            continue

        v = f.get("Value")
        try:
            val = float(v) if v is not None else 0.0
        except Exception:
            try:
                val = float(str(v).replace(",", "").strip())
            except Exception:
                val = 0.0

        if d == today_local:
            daily[metric] = daily.get(metric, 0.0) + val
        if d >= start_week:
            weekly[metric] = weekly.get(metric, 0.0) + val
        if d >= start_month:
            monthly[metric] = monthly.get(metric, 0.0) + val

    written = {"daily": 0, "weekly": 0, "monthly": 0}
    errors = []

    def _upsert_totals(suffix: str, totals: Dict[str, float], existing_map: Dict[str, dict]):
        nonlocal written
        for base_metric, total in totals.items():
            metric_name = f"{base_metric}_{suffix}"
            payload = {
                "Campaign": "ALL",
                "Metric": metric_name,
                "Value": total,
                "Date": str(today_local),
                "Timestamp": now_iso,
                # Optional range fields if they exist in your table:
                "Date Start": str(start_month if suffix == "MONTHLY_TOTAL" else (start_week if suffix == "WEEKLY_TOTAL" else today_local)),
                "Date End": str(today_local),
            }
            try:
                if metric_name in existing_map:
                    kpi_tbl.update(existing_map[metric_name]["id"], _remap_existing_only(kpi_tbl, payload))
                else:
                    kpi_tbl.create(_remap_existing_only(kpi_tbl, payload))

                if suffix == "DAILY_TOTAL":
                    written["daily"] += 1
                elif suffix == "WEEKLY_TOTAL":
                    written["weekly"] += 1
                else:
                    written["monthly"] += 1
            except Exception as e:
                traceback.print_exc()
                errors.append(f"{metric_name}: {e}")

    _upsert_totals("DAILY_TOTAL", daily, existing_totals_daily)
    _upsert_totals("WEEKLY_TOTAL", weekly, existing_totals_weekly)
    _upsert_totals("MONTHLY_TOTAL", monthly, existing_totals_monthly)

    return {
        "ok": True,
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
        "written": written,
        "errors": errors,
    }
