# sms/retry_handler.py
from __future__ import annotations

import os
import traceback
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Literal, Optional, Dict, Any, List

from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS

# -----------------------------
# pyairtable compatibility
# -----------------------------
_PyTable = None
_PyApi = None
try:
    from pyairtable import Table as _PyTable  # v1 style
except Exception:
    _PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2 style
except Exception:
    _PyApi = None


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """
    Returns a Table-like object exposing .get() / .update() / .all() across pyairtable versions.
    """
    if not (api_key and base_id and table_name):
        return None
    try:
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
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
# Helpers for safe updates
# -----------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(s: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_field_map(tbl) -> Dict[str, str]:
    """
    Build normalized->actual field name map from a sample record.
    """
    keys: List[str] = []
    try:
        sample = None
        try:
            page = tbl.all(max_records=1)
            sample = page[0] if page else None
        except Exception:
            sample = None
        if sample:
            keys = list(sample.get("fields", {}).keys())
    except Exception:
        pass
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only keys that exist on the table to avoid 422 UNKNOWN_FIELD_NAME.
    """
    amap = _auto_field_map(tbl)
    if not amap:
        # If we couldn't probe, attempt optimistic write (Airtable will reject unknowns).
        return dict(payload)
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


# -----------------------------
# Lazy Conversations client
# -----------------------------
@lru_cache(maxsize=1)
def get_convos():
    tbl = _make_table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
    if not tbl:
        print("⚠️ RetryHandler: No Airtable config → running in MOCK mode")
    return tbl


# -----------------------------
# Core API
# -----------------------------
def handle_retry(
    record_id: str,
    error: str,
    max_retries: int = 3,
    cooldown_minutes: int = 30,
) -> Literal["NEEDS_RETRY", "GAVE_UP", "ERROR", "MOCK"]:
    """
    Mark a Conversations row for retry.

    Effects:
      - Increments `retry_count`
      - Updates `last_retry_error` + `last_retry_at`
      - Sets `retry_after` = now + cooldown if under max_retries
      - Sets `status` = NEEDS_RETRY or GAVE_UP
    Safe behaviors:
      - Works with pyairtable v1 (Table) and v2 (Api.table)
      - Only writes fields that exist on the table
      - Gracefully degrades to MOCK mode when Airtable not configured
    """
    convos = get_convos()
    if not convos:
        # MOCK mode (no Airtable)
        print(f"[MOCK] RetryHandler → would mark retry for record={record_id} | err={error}")
        return "MOCK"

    try:
        # Get current row (best effort). If fetch fails, fall back to retries=0.
        retries_current = 0
        try:
            rec = convos.get(record_id)
            if rec:
                f = rec.get("fields", {})
                retries_current = int(f.get(RETRY_COUNT_FIELD) or f.get("retry_count") or 0)
        except Exception:
            rec = None

        retries = retries_current + 1
        status: Literal["NEEDS_RETRY", "GAVE_UP"] = "NEEDS_RETRY" if retries < max_retries else "GAVE_UP"

        patch = {
            STATUS_FIELD: status,
            RETRY_COUNT_FIELD: retries,
            LAST_ERROR_FIELD: (error or "")[:500],
            LAST_RETRY_AT_FIELD: _now_iso(),
        }
        if status == "NEEDS_RETRY":
            patch[RETRY_AFTER_FIELD] = (datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)).isoformat()

        safe_patch = _remap_existing_only(convos, patch)
        if not safe_patch:
            # If the configured names don't exist, try common casing fallbacks.
            fallback_patch = {
                "Status": status,
                "retry_count": retries,
                "last_retry_error": (error or "")[:500],
                "last_retry_at": _now_iso(),
            }
            if status == "NEEDS_RETRY":
                fallback_patch["retry_after"] = (datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)).isoformat()
            safe_patch = _remap_existing_only(convos, fallback_patch)

        if not safe_patch:
            print(f"⚠️ RetryHandler: No matching fields on Conversations; cannot update {record_id}")
            return "ERROR"

        convos.update(record_id, safe_patch)
        print(f"🔄 RetryHandler → {record_id}: {status} (attempt {retries})")
        return status

    except Exception as e:
        print(f"❌ RetryHandler failed for {record_id}: {e}")
        traceback.print_exc()
        return "ERROR"
