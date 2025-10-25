# sms/kpi_logger.py
"""
KPI Logger
----------
Lightweight utility to upsert individual KPI metrics to Airtable.
Integrates with datastore + logger.
"""

from __future__ import annotations
import os, traceback, re
from datetime import datetime, timezone
from typing import Dict, Optional
from sms.runtime import get_logger
from sms.datastore import CONNECTOR

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

logger = get_logger("kpi_logger")

# -----------------------------
# Config
# -----------------------------
KPI_TZ = os.getenv("KPI_TZ", "America/Chicago")


# -----------------------------
# Time helpers
# -----------------------------
def _tz_now():
    try:
        return datetime.now(ZoneInfo(KPI_TZ))
    except Exception:
        return datetime.now(timezone.utc)


def _today_local_str() -> str:
    return _tz_now().date().isoformat()


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------------
# Airtable field normalization
# -----------------------------
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


def _fquote(s: str) -> str:
    return (s or "").replace("'", "\\'")


# -----------------------------
# Public API
# -----------------------------
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
    Log or upsert a KPI row in Airtable.
    - metric: e.g. "OUTBOUND_SENT"
    - value: numeric (int or float)
    - overwrite=True → update today's row for metric+campaign
    """
    tbl_handle = CONNECTOR.performance()
    tbl = getattr(tbl_handle, "table", tbl_handle)
    if not tbl:
        msg = "⚠️ KPI Logger: PERFORMANCE table not configured"
        logger.warning(msg)
        return {"ok": False, "action": "skipped", "error": msg}

    # Pre-flight permission check to avoid noisy 403 errors.
    probe = getattr(tbl, "first", None)
    if callable(probe):
        try:
            probe()
        except Exception as e:
            if "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND" in str(e):
                logger.warning(
                    f"⚠️ KPI table inaccessible; skipping event {metric}"
                )
                return {"ok": False, "action": "skipped", "error": str(e)}

    today = date_override or _today_local_str()
    ts = _utcnow_iso()

    # Coerce numeric
    try:
        val = int(float(str(value).replace(",", ""))) if value is not None else 0
    except Exception:
        val = 0

    payload = {
        "Campaign": campaign,
        "Metric": metric,
        "Value": val,
        "Date": today,
        "Timestamp": ts,
    }
    if extra:
        payload.update(extra)

    try:
        if overwrite:
            formula = f"AND({{Metric}}='{_fquote(metric)}',{{Date}}='{_fquote(today)}',{{Campaign}}='{_fquote(campaign)}')"
            existing = tbl.all(formula=formula, max_records=1)
            if existing:
                rec_id = existing[0]["id"]
                tbl.update(rec_id, _remap(tbl, payload))
                logger.info(f"📊 KPI updated → {metric}={val} ({campaign})")
                return {"ok": True, "action": "updated", "record_id": rec_id}

        rec = tbl.create(_remap(tbl, payload))
        logger.info(f"📊 KPI logged → {metric}={val} ({campaign})")
        return {"ok": True, "action": "created", "record_id": rec.get("id") if rec else None}

    except Exception as e:
        if "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND" in str(e):
            logger.warning("⚠️ KPI write skipped: Performance table inaccessible")
            return {"ok": False, "action": "skipped", "error": str(e)}
        logger.error(f"❌ KPI log failed {metric}: {e}", exc_info=True)
        return {"ok": False, "action": "skipped", "error": str(e)}
