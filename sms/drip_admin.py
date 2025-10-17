# sms/drip_admin.py
from __future__ import annotations
import os
import re
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo

# ---- pyairtable compat (no hard crash) -------------------------------------
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

AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
QUIET_TZ = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago"))

def _make_table(api_key: Optional[str], base_id: Optional[str], table_name: str):
    if not (api_key and base_id): return None
    try:
        if _PyApi:  return _PyApi(api_key).table(base_id, table_name)
        if _PyTable:return _PyTable(api_key, base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None

def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _auto_map(tbl) -> Dict[str, str]:
    cache_attr = "__sms_field_map"
    cached = getattr(tbl, cache_attr, None)
    if isinstance(cached, dict):
        return cached
    try:
        rows = tbl.all(max_records=1)  # type: ignore[attr-defined]
        keys = list(rows[0].get("fields", {}).keys()) if rows else []
    except Exception:
        keys = []
    amap = {_norm(k): k for k in keys}
    try:
        setattr(tbl, cache_attr, amap)
    except Exception:
        pass
    return amap

def _sf(tbl, payload: Dict) -> Dict:
    amap = _auto_map(tbl)
    if not amap: return dict(payload)
    out = {}
    for k, v in payload.items():
        m = amap.get(_norm(k))
        if m: out[m] = v
    return out

def _safe_update(tbl, rid: str, payload: Dict):
    if not (tbl and rid and payload): return None
    try:
        return tbl.update(rid, _sf(tbl, payload))  # type: ignore[attr-defined]
    except Exception:
        traceback.print_exc()
        return None

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _strip_microseconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)

def _format_utc(dt_utc: datetime) -> str:
    return _strip_microseconds(dt_utc.astimezone(timezone.utc)).isoformat()

def _format_ct_local_naive(dt_utc: datetime) -> str:
    return _strip_microseconds(dt_utc.astimezone(QUIET_TZ)).replace(tzinfo=None).isoformat()

def _parse_send_time(fields: Dict[str, Any]) -> Tuple[Optional[datetime], str]:
    """
    Returns (send_at_utc, source). Recognizes:
      - Next Send At / next_send_at_utc (UTC ISO)
      - Next Send Date / next_send_date (assume CT naive)
    """
    # Prefer explicit UTC field
    for k in ("Next Send At", "next_send_at_utc", "Send At UTC", "send_at_utc"):
        v = fields.get(k)
        if isinstance(v, str) and v.strip():
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc), k
            except Exception:
                pass
    # Fall back to CT-naive
    for k in ("Next Send Date", "next_send_date"):
        v = fields.get(k)
        if isinstance(v, str) and v.strip():
            try:
                # treat as America/Chicago local naive
                ct = datetime.fromisoformat(v)
                ct = ct.replace(tzinfo=QUIET_TZ)
                return ct.astimezone(timezone.utc), k
            except Exception:
                pass
    return None, ""

def normalize_next_send_dates(dry_run: bool = True, force_now: bool = False, limit: int = 1000) -> Dict[str, Any]:
    """
    Fixes queued/ready rows:
      - Ensure a real UTC timestamp in Next Send At / next_send_at_utc
      - If time is missing or in the past (or force_now), bump to the current moment
      - Mark status READY
      - Keep Next Send Date (CT) in sync for UI consumers
    """
    drip = _make_table(AIRTABLE_KEY, LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    if not drip:
        return {"ok": False, "error": "Drip table unavailable"}

    updated = 0
    examined = 0
    skipped = 0
    would_update = 0

    try:
        rows = drip.all()  # type: ignore[attr-defined]
    except Exception as e:
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    now = _utcnow()
    tolerance = timedelta(seconds=1)
    for r in rows:
        if updated >= limit: break
        f = r.get("fields", {}) or {}
        status = str(f.get("status") or f.get("Status") or "").upper()
        if status not in ("QUEUED", "READY"):
            continue

        examined += 1
        send_at_utc, _ = _parse_send_time(f)

        # decide new time
        if force_now or send_at_utc is None or send_at_utc + tolerance < now:
            new_send_utc = now
        else:
            new_send_utc = send_at_utc

        if new_send_utc is None:
            new_send_utc = now

        new_send_utc = _strip_microseconds(new_send_utc.astimezone(timezone.utc))

        next_send_at_iso = _format_utc(new_send_utc)
        next_send_date_iso = _format_ct_local_naive(new_send_utc)

        payload = {
            "Next Send At": next_send_at_iso,
            "next_send_at_utc": next_send_at_iso,
            "Next Send Date": next_send_date_iso,
            "next_send_date": next_send_date_iso,
            "status": "READY",
        }

        current_values = {
            "Next Send At": f.get("Next Send At"),
            "next_send_at_utc": f.get("next_send_at_utc"),
            "Next Send Date": f.get("Next Send Date"),
            "next_send_date": f.get("next_send_date"),
            "status": f.get("status") or f.get("Status"),
        }

        if all(payload[k] == current_values.get(k) for k in payload):
            skipped += 1
            continue

        if dry_run:
            would_update += 1
            continue

        would_update += 1
        if _safe_update(drip, r["id"], payload):
            updated += 1

    return {
        "ok": True,
        "examined": examined,
        "updated": updated if not dry_run else 0,
        "skipped": skipped,
        "would_update": would_update,
        "dry_run": dry_run,
        "force_now": force_now,
        "limit": limit,
    }
