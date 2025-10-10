# sms/kpi_logger.py
from __future__ import annotations

import os
import re
import traceback
from datetime import datetime, timezone
from typing import Dict, Optional

# --- Ensure Table is always defined (prevents NameError) ---
try:
    from pyairtable import Table as _RealTable
except Exception:  # pyairtable missing or import error
    _RealTable = None

class Table:  # thin wrapper so symbol 'Table' always exists
    def __init__(self, api_key: str, base_id: str, table_name: str):
        if _RealTable is None:
            raise ImportError(
                "pyairtable is not installed or failed to import. "
                "Install with: pip install pyairtable"
            )
        self._t = _RealTable(api_key, base_id, table_name)

    def all(self, **kwargs):
        return self._t.all(**kwargs)

    def create(self, fields: dict):
        return self._t.create(fields)

    def update(self, record_id: str, fields: dict):
        return self._t.update(record_id, fields)


# -----------------------
# ENV / CONFIG
# -----------------------
AIRTABLE_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
PERF_BASE    = os.getenv("PERFORMANCE_BASE")
KPI_TABLE    = os.getenv("KPI_TABLE_NAME", "KPIs")
KPI_TZ       = os.getenv("KPI_TZ", "America/Chicago")  # business timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


# -----------------------
# Time helpers
# -----------------------
def _tz_now():
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo(KPI_TZ))
        except Exception:
            pass
    return datetime.now(timezone.utc)

def _today_local_str() -> str:
    return _tz_now().date().isoformat()

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------
# Airtable helpers
# -----------------------
def _kpi_table() -> Optional[Table]:
    if not (AIRTABLE_KEY and PERF_BASE):
        print("⚠️ KPI Logger: missing AIRTABLE key or PERFORMANCE_BASE")
        return None
    try:
        return Table(AIRTABLE_KEY, PERF_BASE, KPI_TABLE)
    except Exception:
        traceback.print_exc()
        return None

def _norm(s):  # normalize field names
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

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

def _fquote(s: str) -> str:
    """Escape single quotes for Airtable formulas."""
    return (s or "").replace("'", "\\'")


# -----------------------
# Public API
# -----------------------
def log_kpi(
    metric: str,
    value: int | float,
    campaign: str = "ALL",
    overwrite: bool = False,
    *,
    date_override: str | None = None,
    extra: Dict | None = None,
) -> Dict:
    """
    Write a single KPI row into the KPIs table.

    Example:
        log_kpi("OUTBOUND_SENT", 125)
        log_kpi("OUTBOUND_SENT", 130, overwrite=True)  # upsert for today+campaign

    Args:
        metric (str): KPI metric name (e.g., "OUTBOUND_SENT").
        value (int | float): KPI value; coerced to int for consistency.
        campaign (str): Campaign label (default "ALL").
        overwrite (bool): If True, updates today's KPI row for this metric+campaign instead of creating a new one.
        date_override (str): Optional 'YYYY-MM-DD' to force the KPI date.
        extra (dict): Optional additional fields to write (safely remapped).
    Returns:
        dict: { ok: bool, action: 'created'|'updated'|'skipped', record_id: str|None, error?: str }
    """
    kpi_tbl = _kpi_table()
    if not kpi_tbl:
        msg = f"⚠️ Skipping KPI {metric}, Airtable not configured"
        print(msg)
        return {"ok": False, "action": "skipped", "error": msg}

    try:
        today = date_override or _today_local_str()
        ts = _utcnow_iso()

        # Coerce numeric value
        try:
            val = int(value) if value is not None else 0
        except Exception:
            try:
                val = int(float(str(value).replace(",", "")))
            except Exception:
                val = 0

        base_payload = {
            "Campaign": campaign,
            "Metric": metric,
            "Value": val,
            "Date": today,
            "Timestamp": ts,
        }
        if extra and isinstance(extra, dict):
            base_payload.update(extra)

        # Overwrite/upsert logic (today + metric + campaign)
        if overwrite:
            try:
                formula = (
                    f"AND("
                    f"{{Metric}}='{_fquote(metric)}',"
                    f"{{Date}}='{_fquote(today)}',"
                    f"{{Campaign}}='{_fquote(campaign)}'"
                    f")"
                )
                existing = kpi_tbl.all(formula=formula, max_records=1)
                if existing:
                    rec_id = existing[0]["id"]
                    kpi_tbl.update(rec_id, _remap_existing_only(kpi_tbl, base_payload))
                    print(f"📊 Updated KPI → {metric}: {val} (campaign={campaign}, date={today})")
                    return {"ok": True, "action": "updated", "record_id": rec_id}
            except Exception:
                traceback.print_exc()

        # Default: create
        rec = kpi_tbl.create(_remap_existing_only(kpi_tbl, base_payload))
        print(f"📊 Logged KPI → {metric}: {val} (campaign={campaign}, date={today})")
        return {"ok": True, "action": "created", "record_id": rec.get("id") if rec else None}

    except Exception as e:
        print(f"❌ Failed to log KPI {metric}: {e}")
        traceback.print_exc()
        return {"ok": False, "action": "skipped", "error": str(e)}
