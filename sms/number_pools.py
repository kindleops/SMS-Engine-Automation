# sms/number_pools.py
from __future__ import annotations

import os
import re
import random
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

# --- pyairtable shims: support either Table or Api(.table) ---
try:
    from pyairtable import Table as _ATTable  # v1/v2 direct Table
except Exception:
    _ATTable = None

try:
    from pyairtable import Api as _ATApi  # v2 Api().table(...)
except Exception:
    _ATApi = None

# =========================
# ENV / CONFIG
# =========================
CONTROL_BASE_ID = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))

# =========================
# FIELD NAMES (Numbers)
# =========================
F_NUMBER = "Number"  # E.164 DID (preferred)
F_FRIENDLY = "Friendly Name"  # optional alt storage for DID
F_MARKET = "Market"
F_MARKETS_MULTI = "Markets"
F_ACTIVE = "Active"
F_STATUS = "Status"  # treat "Paused" as inactive

F_SENT_TODAY = "Sent Today"
F_DELIV_TODAY = "Delivered Today"
F_FAILED_TODAY = "Failed Today"
F_OPTOUT_TODAY = "Opt-Outs Today"

F_SENT_TOTAL = "Sent Total"
F_DELIV_TOTAL = "Delivered Total"
F_FAILED_TOTAL = "Failed Total"
F_OPTOUT_TOTAL = "Opt-Outs Total"

F_REMAINING = "Remaining"
F_DAILY_RESET = "Daily Reset"
F_LAST_USED = "Last Used"


# =========================
# Time & parsing helpers
# =========================
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _today_str() -> str:
    return _now().date().isoformat()


def _parse_dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _digits_only(s: str | None) -> Optional[str]:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None


def _same_did(a: str | None, b: str | None) -> bool:
    da, db = _digits_only(a), _digits_only(b)
    return bool(da and db and da == db)


# =========================
# Airtable access (Table shim)
# =========================
def _make_table(base_id: str, table_name: str):
    """
    Returns an object with .all(), .get(id), .update(id, fields), .create(fields)
    using pyairtable.Table if present, else Api(...).table(...).
    If neither is available or env is missing → None (MOCK mode).
    """
    if not (AIRTABLE_KEY and base_id):
        return None
    try:
        if _ATTable is not None:
            return _ATTable(AIRTABLE_KEY, base_id, table_name)
        if _ATApi is not None:
            return _ATApi(AIRTABLE_KEY).table(base_id, table_name)
    except Exception:
        traceback.print_exc()
    return None


@lru_cache(maxsize=1)
def _numbers_tbl():
    tbl = _make_table(CONTROL_BASE_ID, NUMBERS_TABLE)
    if not tbl:
        print("⚠️ NumberPools: Airtable not configured; running in MOCK mode.")
    return tbl


def _auto_field_map(tbl) -> Dict[str, str]:
    try:
        one = tbl.all(max_records=1)
        keys = list(one[0].get("fields", {}).keys()) if one else []
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}


def _remap_existing_only(tbl, payload: Dict) -> Dict:
    amap = _auto_field_map(tbl)
    if not amap:
        return dict(payload)
    out = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out


# =========================
# Remaining & daily reset
# =========================
def _remaining_calc(f: Dict) -> int:
    if isinstance(f.get(F_REMAINING), (int, float)):
        try:
            return max(0, int(f[F_REMAINING]))
        except Exception:
            pass
    sent_today = int(f.get(F_SENT_TODAY) or 0)
    daily_cap = int(f.get(F_DAILY_RESET) or DAILY_LIMIT_FALLBACK)
    return max(0, daily_cap - sent_today)


def _reset_daily_if_needed(rec: Dict) -> Dict:
    tbl = _numbers_tbl()
    if not (tbl and rec and rec.get("fields")):
        return rec

    f = rec["fields"]
    last_used = _parse_dt(f.get(F_LAST_USED))
    needs_reset = (not last_used) or (last_used.date().isoformat() != _today_str())

    if needs_reset:
        patch = {
            F_SENT_TODAY: 0,
            F_DELIV_TODAY: 0,
            F_FAILED_TODAY: 0,
            F_OPTOUT_TODAY: 0,
            F_LAST_USED: _now().isoformat(),
        }
        patch[F_REMAINING] = int(f.get(F_DAILY_RESET) or DAILY_LIMIT_FALLBACK)
        try:
            tbl.update(rec["id"], _remap_existing_only(tbl, patch))
            rec["fields"].update(patch)
        except Exception:
            traceback.print_exc()
    return rec


# =========================
# Lookups / Filtering
# =========================
def _is_active(f: Dict) -> bool:
    if f.get(F_ACTIVE) is False:
        return False
    if str(f.get(F_STATUS) or "").strip().lower() == "paused":
        return False
    return True


def _supports_market(f: Dict, market: Optional[str]) -> bool:
    if not market:
        return True
    if f.get(F_MARKET) == market:
        return True
    ms = f.get(F_MARKETS_MULTI)
    return isinstance(ms, list) and (market in ms)


def _all_numbers() -> List[Dict]:
    tbl = _numbers_tbl()
    if not tbl:
        return []
    try:
        return tbl.all()
    except Exception:
        traceback.print_exc()
        return []


def _find_record_by_number(did: str) -> Optional[Dict]:
    tbl = _numbers_tbl()
    if not (tbl and did):
        return None
    try:
        for r in _all_numbers():
            f = r.get("fields", {})
            if _same_did(f.get(F_NUMBER), did) or _same_did(f.get(F_FRIENDLY), did):
                return r
        return None
    except Exception:
        traceback.print_exc()
        return None


# =========================
# Picker
# =========================
def get_from_number(market: Optional[str] = None) -> str:
    """
    Pick a DID by:
      1) Active & supports market
      2) Remaining > 0
      3) Max remaining, then least recently used
    """
    tbl = _numbers_tbl()
    if not tbl:
        dummy = f"+1999{random.randint(1000000, 9999999)}"
        print(f"[MOCK] get_from_number({market!r}) → {dummy}")
        return dummy

    elig: List[Tuple[Tuple[int, datetime], Dict]] = []
    for r in _all_numbers():
        r = _reset_daily_if_needed(r)
        f = r.get("fields", {})
        if not _is_active(f):
            continue
        if not _supports_market(f, market):
            continue
        remaining = _remaining_calc(f)
        if remaining <= 0:
            continue
        last_used = _parse_dt(f.get(F_LAST_USED)) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        # sort key: (-remaining, last_used)
        elig.append(((-remaining, last_used), r))

    if not elig:
        raise RuntimeError(f"🚨 No available numbers for market='{market}'")

    elig.sort(key=lambda x: x[0])
    chosen = elig[0][1]
    cf = chosen.get("fields", {})
    did = cf.get(F_NUMBER) or cf.get(F_FRIENDLY)
    if not did or not _digits_only(did):
        raise RuntimeError("🚨 Chosen number row has no valid DID")

    # Soft bump Last Used (don’t touch counters here)
    try:
        tbl.update(chosen["id"], _remap_existing_only(tbl, {F_LAST_USED: _now().isoformat()}))
    except Exception:
        traceback.print_exc()

    return did


# =========================
# Counter increments
# =========================
def _bump(row: Dict, day_field: str, total_field: str, delta: int = 1, dec_remaining: bool = False):
    tbl = _numbers_tbl()
    if not tbl:
        print(f"[MOCK] bump({row.get('id')}, {day_field}, {total_field}, +{delta})")
        return

    try:
        rec = tbl.get(row["id"])
        rec = _reset_daily_if_needed(rec)
    except Exception:
        traceback.print_exc()
        rec = row

    f = rec.get("fields", {})
    day_val = int(f.get(day_field) or 0) + delta
    tot_val = int(f.get(total_field) or 0) + delta

    patch = {
        day_field: day_val,
        total_field: tot_val,
        F_LAST_USED: _now().isoformat(),
    }
    if dec_remaining:
        # derive remaining from current calc minus delta
        cur_rem = _remaining_calc(f)
        patch[F_REMAINING] = max(0, cur_rem - delta)

    try:
        tbl.update(rec["id"], _remap_existing_only(tbl, patch))
    except Exception:
        traceback.print_exc()


def _ensure_row(did: str) -> Dict:
    row = _find_record_by_number(did)
    if not row:
        raise RuntimeError(f"🚨 Number not found in Numbers table: {did}")
    return row


def _valid_did(did: Optional[str]) -> bool:
    return bool(_digits_only(did))


def _get_row_or_none(did: Optional[str]) -> Optional[Dict]:
    if not _valid_did(did):
        print(f"⚠️ NumberPools: invalid or missing DID: {did!r}")
        return None
    try:
        return _ensure_row(did)  # raises if Airtable has no matching row
    except Exception:
        traceback.print_exc()
        return None


def increment_sent(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_SENT_TODAY, F_SENT_TOTAL, delta=1, dec_remaining=True)


def increment_delivered(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_DELIV_TODAY, F_DELIV_TOTAL, delta=1, dec_remaining=False)


def increment_failed(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_FAILED_TODAY, F_FAILED_TOTAL, delta=1, dec_remaining=False)


def increment_opt_out(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_OPTOUT_TODAY, F_OPTOUT_TOTAL, delta=1, dec_remaining=False)


# =========================
# Convenience
# =========================
def pick_and_mark_sent(market: Optional[str] = None) -> str:
    did = get_from_number(market)
    try:
        increment_sent(did)
    except Exception:
        traceback.print_exc()
    return did
