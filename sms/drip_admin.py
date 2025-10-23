# sms/drip_admin.py
"""
Drip Queue admin utilities.

Normalizes QUEUED/READY rows so the engine has a proper UTC send time:
  • Ensures valid UTC ISO in Next Send At / next_send_at_utc
  • Backfills from CT UI date if needed
  • If missing or past-due (or force_now=True), bumps to now + jitter
  • Mirrors a CT-naive display timestamp for Airtable UI
  • Sets Status → Ready

This module is schema-aware and tolerates Airtable column drift.
"""

from __future__ import annotations

import os
import re
import random
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo

from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS
from sms.airtable_schema import DripStatus
from sms.runtime import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Environment / Schema
# ---------------------------------------------------------------------------

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")

# Quiet-time zone for UI display (CT by default)
QUIET_TZ = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago"))

# Canonical field names from config mapping (already resolved)
DRIP_STATUS_FIELD = DRIP_FIELDS["STATUS"]
DRIP_NEXT_SEND_DATE_FIELD = DRIP_FIELDS["NEXT_SEND_DATE"]     # CT-naive UI field
DRIP_NEXT_SEND_AT_FIELD = DRIP_FIELDS["NEXT_SEND_AT"]         # primary engine field (UTC ISO)
DRIP_NEXT_SEND_AT_UTC_FIELD = DRIP_FIELDS["NEXT_SEND_AT_UTC"] # alias/compat (UTC ISO)

# ---------------------------------------------------------------------------
# Airtable client (supports pyairtable v1/v2)
# ---------------------------------------------------------------------------

_PyApi = None
_PyTable = None
try:
    from pyairtable import Api as _PyApi  # v2
except Exception:
    _PyApi = None
try:
    from pyairtable import Table as _PyTable  # v1
except Exception:
    _PyTable = None


def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    """Create a table handle without hard-crashing if the lib/version varies."""
    if not (api_key and base_id):
        logger.error("⚠️ Drip Queue unavailable — missing AIRTABLE_API_KEY or LEADS_CONVOS_BASE")
        return None
    try:
        if _PyApi:
            return _PyApi(api_key).table(base_id, table_name)
        if _PyTable:
            return _PyTable(api_key, base_id, table_name)
    except Exception as exc:
        logger.exception("Failed to init Airtable client: %s", exc)
    return None


# ---------------------------------------------------------------------------
# Field mapping helpers (tolerate UI renames / drift)
# ---------------------------------------------------------------------------

def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _auto_map(tbl) -> Dict[str, str]:
    """
    Build a map {normalized_key -> actual_column_name} from a sample row.
    Avoids 422s when teams tweak display names in Airtable.
    """
    try:
        rows = tbl.all(max_records=1)  # type: ignore[attr-defined]
        keys = list(rows[0].get("fields", {}).keys()) if rows else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _smart_fields(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    amap = _auto_map(tbl)
    if not amap:
        return dict(payload)
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        mk = amap.get(_norm(k))
        if mk:
            out[mk] = v
    return out


def _safe_update(tbl, record_id: str, payload: Dict[str, Any]):
    if not (tbl and record_id and payload):
        return None
    try:
        return tbl.update(record_id, _smart_fields(tbl, payload))  # type: ignore[attr-defined]
    except Exception:
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Time utilities
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso_to_utc(value: str) -> Optional[datetime]:
    """
    Parse a variety of ISO-ish strings to UTC.
    Accepts '...Z', explicit offsets, or naive (assumed UTC).
    """
    if not (isinstance(value, str) and value.strip()):
        return None
    s = value.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _ct_to_utc_from_naive(value: str) -> Optional[datetime]:
    """
    Interpret a naive CT timestamp and convert to UTC.
    Used for the Airtable UI display field (CT local).
    """
    if not (isinstance(value, str) and value.strip()):
        return None
    try:
        ct = datetime.fromisoformat(value.strip())
        ct = ct.replace(tzinfo=QUIET_TZ)
        return ct.astimezone(timezone.utc)
    except Exception:
        return None


def _to_ct_local_naive(dt_utc: datetime) -> str:
    """UTC → CT-naive ISO string for UI fields."""
    return dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def _parse_send_time(fields: Dict[str, Any]) -> Tuple[Optional[datetime], str]:
    """
    Returns (send_at_utc, source_field).
    Priority:
      1) Next Send At (UTC ISO)
      2) next_send_at_utc (UTC ISO)
      3) legacy aliases
      4) Next Send Date (CT naive)
    """
    utc_candidates = [
        DRIP_NEXT_SEND_AT_FIELD,
        DRIP_NEXT_SEND_AT_UTC_FIELD,
        "Next Send At",
        "next_send_at_utc",
        "Send At UTC",
        "send_at_utc",
    ]
    for k in utc_candidates:
        v = fields.get(k)
        dt = _iso_to_utc(v) if isinstance(v, str) else None
        if dt:
            return dt, k

    date_candidates = [DRIP_NEXT_SEND_DATE_FIELD, "next_send_date"]
    for k in date_candidates:
        v = fields.get(k)
        dt = _ct_to_utc_from_naive(v) if isinstance(v, str) else None
        if dt:
            return dt, k

    return None, ""


# ---------------------------------------------------------------------------
# Core admin operation
# ---------------------------------------------------------------------------

def normalize_next_send_dates(
    dry_run: bool = True,
    force_now: bool = False,
    limit: int = 1000,
    jitter_seconds: Tuple[int, int] = (2, 12),
) -> Dict[str, Any]:
    """
    Normalize queued/ready drip rows so the sender can run safely.

    Args:
      dry_run:      Do not write changes if True.
      force_now:    Treat any timestamp as past-due; bump to now + jitter.
      limit:        Max number of records to update.
      jitter_seconds: (min,max) added when bumping forward.

    Returns:
      { ok, examined, updated, dry_run, force_now, limit }
    """
    tbl = _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    if not tbl:
        return {"ok": False, "error": "Drip table unavailable"}

    try:
        rows = tbl.all()  # type: ignore[attr-defined]
    except Exception as exc:
        traceback.print_exc()
        return {"ok": False, "error": str(exc)}

    now = _utcnow()
    examined = 0
    updated = 0

    valid_statuses = {
        DripStatus.QUEUED.value,
        DripStatus.READY.value,
        DripStatus.RETRY.value,
        DripStatus.THROTTLED.value,
    }

    for r in rows:
        if updated >= limit:
            break

        fields: Dict[str, Any] = (r.get("fields") or {})
        status_raw = str(fields.get(DRIP_STATUS_FIELD) or "").strip()
        if status_raw not in valid_statuses:
            continue

        examined += 1

        send_at_utc, src = _parse_send_time(fields)

        # Determine if we must bump the time forward
        needs_bump = force_now or (send_at_utc is None) or (send_at_utc < now - timedelta(seconds=5))
        if needs_bump:
            bump = timedelta(seconds=random.randint(*jitter_seconds))
            send_at_utc = now + bump

        # Safety fallback
        if send_at_utc is None:
            send_at_utc = now + timedelta(seconds=random.randint(*jitter_seconds))

        # Mirror CT UI
        ct_local_str = _to_ct_local_naive(send_at_utc)

        payload: Dict[str, Any] = {
            DRIP_NEXT_SEND_AT_FIELD: send_at_utc.isoformat(),
            DRIP_NEXT_SEND_AT_UTC_FIELD: send_at_utc.isoformat(),
            DRIP_NEXT_SEND_DATE_FIELD: ct_local_str,
            DRIP_STATUS_FIELD: DripStatus.READY.value,
        }

        # Maintain legacy aliases if present in this base (prevents 422s)
        # These keys will be auto-mapped to the actual columns via _smart_fields()
        if DRIP_NEXT_SEND_DATE_FIELD != "next_send_date":
            payload["next_send_date"] = ct_local_str
        if DRIP_NEXT_SEND_AT_FIELD != "Next Send At":
            payload["Next Send At"] = send_at_utc.isoformat()
        if DRIP_NEXT_SEND_AT_UTC_FIELD != "next_send_at_utc":
            payload["next_send_at_utc"] = send_at_utc.isoformat()

        if not dry_run:
            if _safe_update(tbl, r["id"], payload):
                updated += 1
        else:
            # Log a terse preview in dry-run mode
            logger.info(
                "DRY-RUN normalize: id=%s status=%s src=%s → send_at_utc=%s CT=%s",
                r.get("id"),
                status_raw,
                src or "(none)",
                send_at_utc.isoformat(),
                ct_local_str,
            )

    return {
        "ok": True,
        "examined": examined,
        "updated": 0 if dry_run else updated,
        "dry_run": dry_run,
        "force_now": force_now,
        "limit": limit,
    }
