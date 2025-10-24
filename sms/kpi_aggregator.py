# sms/kpi_aggregator.py
"""
KPI Aggregator
--------------
Rolls up raw KPI records (Value, Metric, Date)
into *_DAILY_TOTAL, *_WEEKLY_TOTAL, *_MONTHLY_TOTAL metrics.
Timezone-aware, idempotent, and datastore-safe.
"""

from __future__ import annotations
import os, re, time, traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from sms.runtime import get_logger
from sms.datastore import CONNECTOR

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

logger = get_logger("kpi_aggregator")

# ---------------------
# Config
# ---------------------
AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE = os.getenv("PERFORMANCE_BASE")
KPI_TABLE = os.getenv("KPI_TABLE_NAME", "KPIs")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true"}
KPI_TZ = os.getenv("KPI_TZ", "America/Chicago")
MAX_SCAN = int(os.getenv("KPI_MAX_SCAN", "10000"))

# ---------------------
# Helpers
# ---------------------
def _tz_now():
    try:
        return datetime.now(ZoneInfo(KPI_TZ))
    except Exception:
        return datetime.now(timezone.utc)

def _to_date_local(s: str) -> Optional[datetime.date]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if ZoneInfo:
            dt = dt.astimezone(ZoneInfo(KPI_TZ))
        return dt.date()
    except Exception:
        return None

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower().strip())

def _auto_map(tbl) -> Dict[str, str]:
    try:
        one = tbl.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap(tbl, data: Dict) -> Dict:
    amap = _auto_map(tbl)
    return {amap.get(_norm(k), k): v for k, v in data.items() if amap.get(_norm(k))}

def _fetch(tbl) -> Tuple[list, Optional[str]]:
    """Fetch KPI rows safely with retries."""
    for i in range(3):
        try:
            rows = tbl.all(page_size=100, max_records=min(100, MAX_SCAN))
            return rows, None
        except Exception as e:
            logger.error(f"KPI fetch attempt {i+1} failed: {e}")
            time.sleep(1)
    return [], "Fetch failed after retries"

# ---------------------
# Core Aggregator
# ---------------------
def aggregate_kpis() -> Dict:
    logger.info("Starting KPI aggregation...")
    if TEST_MODE:
        logger.info("TEST_MODE active – skipping writes.")
        return {"ok": True, "note": "test mode", "written": 0}

    tbl = CONNECTOR.performance()
    if not tbl:
        return {"ok": False, "error": "No performance table configured"}

    rows, fetch_err = _fetch(tbl)
    today = _tz_now().date()
    start_week = today - timedelta(days=7)
    start_month = today.replace(day=1)
    now_iso = datetime.now(timezone.utc).isoformat()

    raw, daily_exist, weekly_exist, monthly_exist = [], {}, {}, {}
    for r in rows:
        f = r.get("fields", {})
        metric = str(f.get("Metric", "")).strip()
        d = _to_date_local(str(f.get("Date", "")))
        if not metric or not d:
            continue
        if metric.endswith("_DAILY_TOTAL") and d == today: daily_exist[metric] = r; continue
        if metric.endswith("_WEEKLY_TOTAL") and d == today: weekly_exist[metric] = r; continue
        if metric.endswith("_MONTHLY_TOTAL") and d == today: monthly_exist[metric] = r; continue
        raw.append(r)

    # Aggregate
    agg = {"daily": {}, "weekly": {}, "monthly": {}}
    for r in raw:
        f = r["fields"]
        m = f.get("Metric"); d = _to_date_local(str(f.get("Date"))); v = f.get("Value") or 0
        try: val = float(str(v).replace(",", ""))
        except Exception: val = 0
        if not (m and d): continue
        if d == today: agg["daily"][m] = agg["daily"].get(m, 0) + val
        if d >= start_week: agg["weekly"][m] = agg["weekly"].get(m, 0) + val
        if d >= start_month: agg["monthly"][m] = agg["monthly"].get(m, 0) + val

    def _upsert(suffix: str, data: Dict[str, float], existing: Dict[str, dict]):
        written = 0
        for base, val in data.items():
            metric_name = f"{base}_{suffix}"
            payload = {
                "Campaign": "ALL",
                "Metric": metric_name,
                "Value": val,
                "Date": str(today),
                "Timestamp": now_iso,
                "Date Start": str(start_month if "MONTHLY" in suffix else (start_week if "WEEKLY" in suffix else today)),
                "Date End": str(today),
            }
            try:
                if metric_name in existing:
                    tbl.update(existing[metric_name]["id"], _remap(tbl, payload))
                else:
                    tbl.create(_remap(tbl, payload))
                written += 1
                time.sleep(0.2)
            except Exception as e:
                logger.error(f"Upsert fail {metric_name}: {e}", exc_info=True)
        return written

    written = {
        "daily": _upsert("DAILY_TOTAL", agg["daily"], daily_exist),
        "weekly": _upsert("WEEKLY_TOTAL", agg["weekly"], weekly_exist),
        "monthly": _upsert("MONTHLY_TOTAL", agg["monthly"], monthly_exist),
    }

    out = {"ok": True, "written": written, "errors": []}
    if fetch_err: out["errors"].append(fetch_err)
    logger.info("✅ KPI aggregation complete: %s", written)
    return out