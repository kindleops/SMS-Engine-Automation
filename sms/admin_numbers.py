# sms/admin_numbers.py
from __future__ import annotations

import os
import re
import math
import traceback
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

from dotenv import load_dotenv

load_dotenv()

# ---- Optional Airtable client (pyairtable v2+)
try:
    from pyairtable import Api as _Api  # v2 canonical
except Exception:
    _Api = None  # type: ignore


# =========================
# ENV / CONFIG
# =========================
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")

PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
DRIP_QUEUE_TABLE = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

QUIET_TZ_NAME = os.getenv("QUIET_TZ", "America/Chicago")
QUIET_START_HOUR = int(os.getenv("QUIET_START_HOUR", "21"))  # 21:00
QUIET_END_HOUR = int(os.getenv("QUIET_END_HOUR", "9"))  # 09:00
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))


# =========================
# TIME HELPERS
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _tz() -> Any:
    try:
        return ZoneInfo(QUIET_TZ_NAME) if ZoneInfo else None
    except Exception:
        return None


def _in_quiet_hours(dt_utc: datetime) -> bool:
    z = _tz()
    local = dt_utc.astimezone(z) if z else dt_utc
    return (local.hour >= QUIET_START_HOUR) or (local.hour < QUIET_END_HOUR)


def _shift_to_window(dt_utc: datetime) -> datetime:
    z = _tz()
    local = dt_utc.astimezone(z) if z else dt_utc
    if local.hour >= QUIET_START_HOUR:
        local = (local + timedelta(days=1)).replace(hour=QUIET_END_HOUR, minute=0, second=0, microsecond=0)
    elif local.hour < QUIET_END_HOUR:
        local = local.replace(hour=QUIET_END_HOUR, minute=0, second=0, microsecond=0)
    # Return UTC
    return local.astimezone(timezone.utc) if z else local


def _local_naive_iso(dt_utc: datetime) -> str:
    """Return local-naive ISO string (Airtable-friendly) in QUIET_TZ or UTC if tz missing."""
    z = _tz()
    local = dt_utc.astimezone(z) if z else dt_utc
    return local.replace(tzinfo=None).isoformat(timespec="seconds")


# =========================
# AIRTABLE HELPERS
# =========================
def _api() -> Optional[Any]:
    if not (_Api and AIRTABLE_KEY):
        print("[AdminNumbers] ⚠️ pyairtable or API key missing → MOCK mode")
        return None
    try:
        return _Api(AIRTABLE_KEY)  # type: ignore
    except Exception:
        print("[AdminNumbers] ❌ Failed to init Api()")
        traceback.print_exc()
        return None


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


def _auto_field_map(tbl) -> Dict[str, str]:
    def _norm(s: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

    try:
        page = tbl.all(max_records=1)
        keys = list((page[0] if page else {"fields": {}}).get("fields", {}).keys())
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only fields that exist on the destination table (avoid 422 UNKNOWN_FIELD_NAME)."""
    if not tbl or not payload:
        return dict(payload or {})
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)

    def _norm(s: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


def _safe_update(tbl, rid: str, payload: Dict[str, Any]) -> None:
    try:
        if tbl and rid and payload:
            tbl.update(rid, _remap_existing_only(tbl, payload))
    except Exception:
        traceback.print_exc()


def _safe_create(tbl, payload: Dict[str, Any]):
    try:
        if tbl and payload:
            return tbl.create(_remap_existing_only(tbl, payload))
    except Exception:
        traceback.print_exc()


def _safe_all(tbl, **kwargs) -> List[Dict[str, Any]]:
    if not tbl:
        return []
    try:
        return list(tbl.all(**kwargs))
    except Exception:
        traceback.print_exc()
        return []


# =========================
# GENERIC UTIL
# =========================
def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None


def _to_e164(fields: Dict[str, Any]) -> Optional[str]:
    """Try common DID columns and normalize to E.164 (+1...)."""
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


# =========================
# LOOKUPS / MAPPERS
# =========================
def _campaign_market_map() -> Dict[str, Any]:
    cmap: Dict[str, Any] = {}
    t = _tbl(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE)
    if not t:
        return cmap
    try:
        for r in _safe_all(t):
            cmap[r["id"]] = r.get("fields", {}).get("Market")
    except Exception:
        traceback.print_exc()
    return cmap


def _prospect_market_map() -> Dict[str, Any]:
    pmap: Dict[str, Any] = {}
    t = _tbl(LEADS_CONVOS_BASE, PROSPECTS_TABLE)
    if not t:
        return pmap
    try:
        for r in _safe_all(t):
            pmap[r["id"]] = r.get("fields", {}).get("Market")
    except Exception:
        traceback.print_exc()
    return pmap


# =========================
# NUMBER PICKER
# =========================
def _remaining_calc(f: Dict[str, Any]) -> int:
    """Compute remaining sends for a DID using either explicit Remaining or (Daily Reset - Sent Today)."""
    try:
        if f.get("Remaining") is not None:
            return max(0, int(f.get("Remaining") or 0))
    except Exception:
        pass
    try:
        sent_today = int(f.get("Sent Today") or 0)
    except Exception:
        sent_today = 0
    try:
        daily_cap = int(f.get("Daily Reset") or DAILY_LIMIT_FALLBACK)
    except Exception:
        daily_cap = DAILY_LIMIT_FALLBACK
    return max(0, daily_cap - sent_today)


def _pick_number_for_market(market: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (from_number E.164, numbers_record_id) or (None, None) if none available.
    Chooses the DID with the highest remaining and least recently used.
    """
    nums = _tbl(CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)
    if not nums:
        return None, None

    rows = _safe_all(nums)
    elig: List[Tuple[Tuple[int, str], Dict[str, Any]]] = []

    for r in rows:
        f = r.get("fields", {})
        if f.get("Active") is False:
            continue
        if str(f.get("Status") or "").strip().lower() == "paused":
            continue
        if not _supports_market(f, market):
            continue

        rem = _remaining_calc(f)
        if rem <= 0:
            continue

        last_used = str(f.get("Last Used") or "1970-01-01T00:00:00Z")
        elig.append(((-rem, last_used), r))

    if not elig:
        return None, None

    elig.sort(key=lambda x: x[0])  # highest remaining, least recently used
    chosen = elig[0][1]
    did = _to_e164(chosen.get("fields", {}))
    return (did, chosen.get("id")) if did else (None, None)


# =========================
# PUBLIC: BACKFILL + RESEQUENCE
# =========================
def backfill_numbers_for_existing_queue(
    view: Optional[str] = None,
    per_number_rate: int = 20,
    respect_quiet_hours: bool = True,
) -> Dict[str, int]:
    """
    For QUEUED/READY/SENDING Drip rows missing `from_number`, assign a DID by market and
    resequence `next_send_date` paced at `per_number_rate` (messages per DID per minute).
    Also resequences rows that already have a `from_number`.
    """
    drip = _tbl(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    if not drip:
        return {"scanned": 0, "updated": 0, "skipped": 0}

    cmap = _campaign_market_map()
    pmap = _prospect_market_map()

    rows = _safe_all(drip, view=view)
    now = utcnow()
    if respect_quiet_hours and _in_quiet_hours(now):
        now = _shift_to_window(now)

    sec_per_msg = max(1, int(math.ceil(60.0 / max(1, per_number_rate))))
    next_by_num: Dict[str, datetime] = defaultdict(lambda: now)

    scanned = updated = skipped = 0

    for r in rows:
        scanned += 1
        f = r.get("fields", {})
        status = str(f.get("status") or f.get("Status") or "").strip().upper()
        if status not in ("QUEUED", "READY", "SENDING"):
            continue

        did = f.get("from_number") or f.get("From Number")

        # Resolve market if needed
        market = f.get("Market")
        cids = f.get("Campaign") or []
        if isinstance(cids, list) and cids:
            market = market or cmap.get(cids[0])
        pids = f.get("Prospect") or []
        if isinstance(pids, list) and pids:
            market = market or pmap.get(pids[0])

        # If missing DID, pick one
        if not did:
            did, _rid = _pick_number_for_market(market)
            if not did:
                skipped += 1
                continue
            _safe_update(drip, r["id"], {"from_number": did, "From Number": did})

        # Pace next_send_date
        t = max(next_by_num[did], now)
        next_by_num[did] = t + timedelta(seconds=sec_per_msg)
        _safe_update(drip, r["id"], {"next_send_date": _local_naive_iso(t), "UI": "⏳"})
        updated += 1

    return {"scanned": scanned, "updated": updated, "skipped": skipped}


def resequence_next_send(
    per_number_rate: int = 20,
    only_campaign_id: Optional[str] = None,
    view: Optional[str] = None,
    respect_quiet_hours: bool = True,
) -> Dict[str, int]:
    """
    Recompute `next_send_date` for QUEUED/READY items that already have a `from_number`,
    paced at N msgs / DID / minute. Optionally filter to a single Campaign record id.
    """
    drip = _tbl(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    if not drip:
        return {"groups": 0, "updated": 0}

    rows = _safe_all(drip, view=view)

    now = utcnow()
    if respect_quiet_hours and _in_quiet_hours(now):
        now = _shift_to_window(now)

    sec_per_msg = max(1, int(math.ceil(60.0 / max(1, per_number_rate))))
    pernum: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # Collect due items grouped by DID
    for r in rows:
        f = r.get("fields", {})
        status = str(f.get("status") or f.get("Status") or "").strip().upper()
        if status not in ("QUEUED", "READY"):
            continue
        if only_campaign_id:
            cids = f.get("Campaign") or []
            cids = cids if isinstance(cids, list) else [cids]
            if only_campaign_id not in cids:
                continue
        did = f.get("from_number") or f.get("From Number")
        if not did:
            continue
        pernum[did].append(r)

    updated = 0
    for did, group in pernum.items():
        # Deterministic order: created_at if present, else record id
        group.sort(key=lambda r: (str(r.get("fields", {}).get("created_at") or ""), r["id"]))
        next_t = now
        for r in group:
            _safe_update(drip, r["id"], {"next_send_date": _local_naive_iso(next_t), "UI": "⏳"})
            next_t = next_t + timedelta(seconds=sec_per_msg)
            updated += 1

    return {"groups": len(pernum), "updated": updated}
