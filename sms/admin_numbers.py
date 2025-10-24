"""
🚀 Admin Numbers Manager
------------------------
Handles number assignment, drip pacing, and quiet-hour enforcement
for Drip Queue campaigns.

Features:
 • Market-based DID allocation
 • Quiet-hour shifting
 • Per-number rate resequencing
 • Retry-safe Airtable updates
"""

from __future__ import annotations

import os, re, math, traceback
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

from dotenv import load_dotenv

load_dotenv()

from sms.config import DRIP_FIELD_MAP as DRIP_FIELDS, PROSPECT_FIELD_MAP as PROSPECT_FIELDS
from sms.airtable_schema import DripStatus

# Optional Airtable client (pyairtable v2)
try:
    from pyairtable import Api as _Api
except Exception:
    _Api = None  # type: ignore


# ==========================================================
# ENV CONFIG
# ==========================================================
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")

PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

QUIET_TZ_NAME = os.getenv("QUIET_TZ", "America/Chicago")
QUIET_START_HOUR = int(os.getenv("QUIET_START_HOUR", "21"))
QUIET_END_HOUR = int(os.getenv("QUIET_END_HOUR", "9"))
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))


# ==========================================================
# TIME HELPERS
# ==========================================================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _tz() -> Optional[Any]:
    try:
        return ZoneInfo(QUIET_TZ_NAME) if ZoneInfo else None
    except Exception:
        return None


def _in_quiet_hours(dt_utc: datetime) -> bool:
    z = _tz()
    local = dt_utc.astimezone(z) if z else dt_utc
    return (local.hour >= QUIET_START_HOUR) or (local.hour < QUIET_END_HOUR)


def _shift_to_window(dt_utc: datetime) -> datetime:
    """Shift UTC datetime to next allowed quiet-hour exit window."""
    z = _tz()
    local = dt_utc.astimezone(z) if z else dt_utc
    if local.hour >= QUIET_START_HOUR:
        local = (local + timedelta(days=1)).replace(hour=QUIET_END_HOUR, minute=0)
    elif local.hour < QUIET_END_HOUR:
        local = local.replace(hour=QUIET_END_HOUR, minute=0)
    return local.astimezone(timezone.utc) if z else local


def _local_naive_iso(dt_utc: datetime) -> str:
    """Return local-naive ISO string (Airtable-friendly)."""
    z = _tz()
    local = dt_utc.astimezone(z) if z else dt_utc
    return local.replace(tzinfo=None).isoformat(timespec="seconds")


# ==========================================================
# AIRTABLE CLIENT
# ==========================================================
_api_cache = None


def _api():
    global _api_cache
    if not _Api or not AIRTABLE_KEY:
        print("[AdminNumbers] ⚠️ pyairtable or API key missing → MOCK mode")
        return None
    if not _api_cache:
        try:
            _api_cache = _Api(AIRTABLE_KEY)
        except Exception:
            print("[AdminNumbers] ❌ Failed to init Api()")
            traceback.print_exc()
            return None
    return _api_cache


def _tbl(base_id: Optional[str], name: str):
    api = _api()
    if not (api and base_id):
        return None
    try:
        return api.table(base_id, name)
    except Exception:
        print(f"[AdminNumbers] ❌ Failed to get table {name} (base={base_id})")
        traceback.print_exc()
        return None


def _safe_all(tbl, **kwargs) -> List[Dict[str, Any]]:
    if not tbl:
        return []
    try:
        return list(tbl.all(**kwargs))
    except Exception:
        traceback.print_exc()
        return []


def _safe_update(tbl, rid: str, payload: Dict[str, Any]) -> None:
    try:
        if tbl and rid and payload:
            tbl.update(rid, payload)
    except Exception:
        traceback.print_exc()


# ==========================================================
# MARKET + NUMBER PICKER
# ==========================================================
def _digits_only(s: Any) -> Optional[str]:
    return "".join(re.findall(r"\d+", s)) if isinstance(s, str) else None


def _to_e164(fields: Dict[str, Any]) -> Optional[str]:
    for key in ("Number", "A Number", "Phone", "E164", "Friendly Name"):
        v = fields.get(key)
        if isinstance(v, str):
            digits = _digits_only(v)
            if digits:
                return v if v.strip().startswith("+") else f"+{digits}"
    return None


def _supports_market(f: Dict[str, Any], market: Optional[str]) -> bool:
    if not market:
        return True
    if f.get("Market") == market:
        return True
    ms = f.get("Markets")
    return isinstance(ms, list) and (market in ms)


def _remaining_calc(f: Dict[str, Any]) -> int:
    """Compute remaining sends for a DID."""
    try:
        return max(0, int(f.get("Remaining") or 0))
    except Exception:
        pass
    sent_today = int(f.get("Sent Today") or 0)
    daily_cap = int(f.get("Daily Reset") or DAILY_LIMIT_FALLBACK)
    return max(0, daily_cap - sent_today)


def _pick_number_for_market(market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    nums = _tbl(CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)
    if not nums:
        return None, None
    rows = _safe_all(nums)
    elig = []
    for r in rows:
        f = r.get("fields", {})
        if not _supports_market(f, market):
            continue
        if f.get("Active") is False or str(f.get("Status", "")).lower() == "paused":
            continue
        rem = _remaining_calc(f)
        if rem <= 0:
            continue
        last_used = str(f.get("Last Used") or "1970-01-01T00:00:00Z")
        elig.append(((-rem, last_used), r))
    if not elig:
        return None, None
    elig.sort(key=lambda x: x[0])
    chosen = elig[0][1]
    did = _to_e164(chosen.get("fields", {}))
    return (did, chosen.get("id")) if did else (None, None)


# ==========================================================
# PUBLIC FUNCTIONS
# ==========================================================
def backfill_numbers_for_existing_queue(per_number_rate: int = 20, respect_quiet_hours: bool = True) -> Dict[str, int]:
    drip = _tbl(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    if not drip:
        return {"scanned": 0, "updated": 0, "skipped": 0}

    rows = _safe_all(drip)
    now = utcnow()
    if respect_quiet_hours and _in_quiet_hours(now):
        now = _shift_to_window(now)
        print(f"[AdminNumbers] ⏸ Quiet hours active — deferring to {now.isoformat()}")

    sec_per_msg = max(1, int(math.ceil(60.0 / max(1, per_number_rate))))
    next_by_num: Dict[str, datetime] = defaultdict(lambda: now)

    scanned = updated = skipped = 0
    for r in rows:
        scanned += 1
        f = r.get("fields", {})
        status = str(f.get(DRIP_FIELDS["STATUS"]) or "").strip().upper()
        if status not in (DripStatus.QUEUED.value, DripStatus.READY.value, DripStatus.SENDING.value):
            continue
        did = f.get(DRIP_FIELDS["FROM_NUMBER"])
        market = f.get(DRIP_FIELDS["MARKET"])

        # Assign DID if missing
        if not did:
            did, _ = _pick_number_for_market(market)
            if not did:
                skipped += 1
                continue
            _safe_update(drip, r["id"], {DRIP_FIELDS["FROM_NUMBER"]: did})

        t = max(next_by_num[did], now)
        next_by_num[did] = t + timedelta(seconds=sec_per_msg)
        _safe_update(drip, r["id"], {DRIP_FIELDS["NEXT_SEND_DATE"]: _local_naive_iso(t), DRIP_FIELDS["UI"]: "⏳"})
        updated += 1

    print(f"[AdminNumbers] ✅ Backfill complete — scanned={scanned}, updated={updated}, skipped={skipped}")
    return {"scanned": scanned, "updated": updated, "skipped": skipped}


def resequence_next_send(per_number_rate: int = 20, respect_quiet_hours: bool = True) -> Dict[str, int]:
    drip = _tbl(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    if not drip:
        return {"groups": 0, "updated": 0}

    rows = _safe_all(drip)
    now = utcnow()
    if respect_quiet_hours and _in_quiet_hours(now):
        now = _shift_to_window(now)

    sec_per_msg = max(1, int(math.ceil(60.0 / max(1, per_number_rate))))
    pernum: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for r in rows:
        f = r.get("fields", {})
        status = str(f.get(DRIP_FIELDS["STATUS"]) or "").upper()
        if status not in (DripStatus.QUEUED.value, DripStatus.READY.value):
            continue
        did = f.get(DRIP_FIELDS["FROM_NUMBER"])
        if not did:
            continue
        pernum[did].append(r)

    updated = 0
    for did, group in pernum.items():
        group.sort(key=lambda r: (str(r.get("fields", {}).get("created_at") or ""), r["id"]))
        next_t = now
        for r in group:
            _safe_update(drip, r["id"], {DRIP_FIELDS["NEXT_SEND_DATE"]: _local_naive_iso(next_t), DRIP_FIELDS["UI"]: "⏳"})
            next_t += timedelta(seconds=sec_per_msg)
            updated += 1

    print(f"[AdminNumbers] 🔁 Resequenced {updated} records across {len(pernum)} DIDs.")
    return {"groups": len(pernum), "updated": updated}
