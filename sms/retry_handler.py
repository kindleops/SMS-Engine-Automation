"""
🔁 retry_handler.py (v3.1 — Telemetry Edition)
──────────────────────────────────────────────
Handles automatic retry logic for Conversations table:
 - Increments retry_count
 - Sets last_retry_error + retry_after
 - Marks status as NEEDS_RETRY or GAVE_UP
Adds:
 - Structured logging
 - KPI + Run telemetry
"""

from __future__ import annotations
import os, traceback, re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Literal, Optional, Dict, Any

from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS
from sms.runtime import get_logger

log = get_logger("retry_handler")

try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


# -----------------------------
# Airtable setup
# -----------------------------
_PyTable = _PyApi = None
try:
    from pyairtable import Table as _PyTable
except Exception:
    pass
try:
    from pyairtable import Api as _PyApi
except Exception:
    pass


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id and table_name):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        log.error("Failed to init Airtable Conversations table", exc_info=True)
    return None


# -----------------------------
# Env-driven Field Mapping
# -----------------------------
STATUS_FIELD = CONV_FIELDS["STATUS"]
RETRY_COUNT_FIELD = CONVERSATIONS_FIELDS.get("RETRY_COUNT", "retry_count")
RETRY_AFTER_FIELD = CONVERSATIONS_FIELDS.get("RETRY_AFTER", "retry_after")
LAST_ERROR_FIELD = CONVERSATIONS_FIELDS.get("LAST_ERROR", "last_retry_error")
LAST_RETRY_AT_FIELD = CONVERSATIONS_FIELDS.get("LAST_RETRY_AT", "last_retry_at")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")


# -----------------------------
# Helpers
# -----------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        page = tbl.all(max_records=1)
        keys = list(page[0].get("fields", {}).keys()) if page else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    amap = _auto_field_map(tbl)
    out = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


@lru_cache(maxsize=1)
def get_convos():
    tbl = _make_table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
    if not tbl:
        log.warning("⚠️ RetryHandler: No Airtable config → mock mode")
    return tbl


# -----------------------------
# Core API
# -----------------------------
def handle_retry(
    record_id: str,
    error: str,
    max_retries: int = 3,
    cooldown_minutes: int = 30,
) -> Dict[str, Any]:
    """
    Marks a Conversations record for retry.
    Returns structured info for dashboards.
    """
    convos = get_convos()
    now = datetime.now(timezone.utc)
    if not convos:
        log.info(f"[MOCK] Would mark retry for {record_id} | err={error}")
        return {"ok": True, "mock": True, "record_id": record_id, "status": "MOCK"}

    try:
        retries_current = 0
        try:
            rec = convos.get(record_id)
            if rec:
                f = rec.get("fields", {})
                retries_current = int(f.get(RETRY_COUNT_FIELD) or f.get("retry_count") or 0)
        except Exception:
            rec = None

        retries = retries_current + 1
        status = "NEEDS_RETRY" if retries < max_retries else "GAVE_UP"
        next_retry = (now + timedelta(minutes=cooldown_minutes)).isoformat()

        patch = {
            STATUS_FIELD: status,
            RETRY_COUNT_FIELD: retries,
            LAST_ERROR_FIELD: (error or "")[:500],
            LAST_RETRY_AT_FIELD: _now_iso(),
        }
        if status == "NEEDS_RETRY":
            patch[RETRY_AFTER_FIELD] = next_retry

        safe_patch = _remap_existing_only(convos, patch)
        if not safe_patch:
            log.warning(f"No matching fields for Conversations; retry skipped for {record_id}")
            return {"ok": False, "error": "no_matching_fields"}

        convos.update(record_id, safe_patch)
        log.info(f"🔁 Retry marked for {record_id} → {status} (attempt {retries})")

        # Telemetry
        log_run("RETRY_HANDLER", processed=1, breakdown={"status": status, "attempt": retries})
        log_kpi("RETRY_ATTEMPT", retries)

        return {
            "ok": True,
            "record_id": record_id,
            "status": status,
            "retries": retries,
            "next_retry_at": next_retry if status == "NEEDS_RETRY" else None,
        }

    except Exception as e:
        log.error(f"❌ RetryHandler failed for {record_id}", exc_info=True)
        return {"ok": False, "record_id": record_id, "error": str(e), "status": "ERROR"}
