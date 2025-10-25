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
def log_kpi(event_name: str, value: int = 1, **kwargs):
    """Safely log KPI events without interrupting workers."""
    try:
        tbl_handle = CONNECTOR.performance()
        tbl = getattr(tbl_handle, "table", tbl_handle)
        if not hasattr(tbl, "create"):
            raise AttributeError("Performance table is not writable")

        payload = {"Event": event_name, "Value": value, **kwargs}

        try:
            record = tbl.create(payload)
            logger.info(f"📈 KPI logged {event_name} → {value}")
            return record
        except Exception as exc:
            message = str(exc)
            if "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND" in message or "403" in message:
                logger.warning(f"⚠️ KPI base not accessible — skipping {event_name}")
                return None
            raise
    except Exception as exc:
        logger.warning(f"⚠️ KPI logger suppressed: {exc}")
        return None
