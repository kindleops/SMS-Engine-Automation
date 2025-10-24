"""
📞 Number Pools v3.1
────────────────────────────
Handles:
 - DID selection by market
 - Daily reset + usage counters
 - KPI + run telemetry integration
"""

from __future__ import annotations
import os, re, random, traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from sms.runtime import get_logger

logger = get_logger("number_pools")

# Optional deps
try:
    from pyairtable import Table as _ATTable
except Exception:
    _ATTable = None

try:
    from pyairtable import Api as _ATApi
except Exception:
    _ATApi = None

try:
    from sms.kpi_logger import log_kpi
except Exception:

    def log_kpi(*_a, **_k):
        pass


try:
    from sms.logger import log_run
except Exception:

    def log_run(*_a, **_k):
        pass


# =========================
# ENV / CONFIG
# =========================
CONTROL_BASE_ID = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))

# FIELD CONSTANTS
F_NUMBER, F_FRIENDLY, F_MARKET, F_MARKETS_MULTI = "Number", "Friendly Name", "Market", "Markets"
F_ACTIVE, F_STATUS = "Active", "Status"
F_SENT_TODAY, F_DELIV_TODAY, F_FAILED_TODAY, F_OPTOUT_TODAY = "Sent Today", "Delivered Today", "Failed Today", "Opt-Outs Today"
F_SENT_TOTAL, F_DELIV_TOTAL, F_FAILED_TOTAL, F_OPTOUT_TOTAL = "Sent Total", "Delivered Total", "Failed Total", "Opt-Outs Total"
F_REMAINING, F_DAILY_RESET, F_LAST_USED = "Remaining", "Daily Reset", "Last Used"


# =========================
# Time helpers
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
# Airtable connection
# =========================
def _make_table(base_id: str, table_name: str):
    if not (AIRTABLE_KEY and base_id):
        return None
    try:
        if _ATTable:
            return _ATTable(AIRTABLE_KEY, base_id, table_name)
        if _ATApi:
            return _ATApi(AIRTABLE_KEY).table(base_id, table_name)
    except Exception as e:
        logger.error(f"Airtable init failed: {e}", exc_info=True)
    return None


@lru_cache(maxsize=1)
def _numbers_tbl():
    tbl = _make_table(CONTROL_BASE_ID, NUMBERS_TABLE)
    if not tbl:
        logger.warning("⚠️ NumberPools running in MOCK mode (no Airtable).")
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
    out = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out or dict(payload)


# =========================
# Logic helpers
# =========================
def _remaining_calc(f: Dict) -> int:
    try:
        return max(0, int(f.get(F_REMAINING) or 0))
    except Exception:
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
    if not needs_reset:
        return rec
    patch = {
        F_SENT_TODAY: 0,
        F_DELIV_TODAY: 0,
        F_FAILED_TODAY: 0,
        F_OPTOUT_TODAY: 0,
        F_REMAINING: int(f.get(F_DAILY_RESET) or DAILY_LIMIT_FALLBACK),
        F_LAST_USED: _now().isoformat(),
    }
    try:
        tbl.update(rec["id"], _remap_existing_only(tbl, patch))
        rec["fields"].update(patch)
        logger.info(f"🔄 Daily reset for {f.get(F_NUMBER)}")
    except Exception as e:
        logger.warning(f"⚠️ Reset failed: {e}")
    return rec


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
    return isinstance(ms, list) and market in ms


# =========================
# Public API
# =========================
def get_from_number(market: Optional[str] = None) -> str:
    tbl = _numbers_tbl()
    if not tbl:
        dummy = f"+1999{random.randint(1000000, 9999999)}"
        logger.info(f"[MOCK] get_from_number({market!r}) → {dummy}")
        return dummy

    elig: List[Tuple[Tuple[int, datetime], Dict]] = []
    for r in tbl.all():
        r = _reset_daily_if_needed(r)
        f = r.get("fields", {})
        if not _is_active(f) or not _supports_market(f, market):
            continue
        remaining = _remaining_calc(f)
        if remaining <= 0:
            continue
        last_used = _parse_dt(f.get(F_LAST_USED)) or datetime(1970, 1, 1, tzinfo=timezone.utc)
        elig.append(((-remaining, last_used), r))

    if not elig:
        raise RuntimeError(f"🚨 No available numbers for market='{market}'")

    elig.sort(key=lambda x: x[0])
    chosen = elig[0][1]
    cf = chosen["fields"]
    did = cf.get(F_NUMBER) or cf.get(F_FRIENDLY)
    if not did or not _digits_only(did):
        raise RuntimeError("🚨 Chosen number row has no valid DID")

    try:
        tbl.update(chosen["id"], _remap_existing_only(tbl, {F_LAST_USED: _now().isoformat()}))
    except Exception as e:
        logger.warning(f"⚠️ Failed to bump last_used: {e}")

    log_kpi("NUMBER_PICKED", 1, extra={"Market": market or "ALL"})
    return did


def _bump(row: Dict, day_field: str, total_field: str, delta: int = 1, dec_remaining: bool = False):
    tbl = _numbers_tbl()
    if not tbl:
        logger.info(f"[MOCK] bump({row.get('id')}, {day_field})")
        return
    try:
        rec = tbl.get(row["id"])
        rec = _reset_daily_if_needed(rec)
        f = rec["fields"]
        patch = {
            day_field: int(f.get(day_field) or 0) + delta,
            total_field: int(f.get(total_field) or 0) + delta,
            F_LAST_USED: _now().isoformat(),
        }
        if dec_remaining:
            cur_rem = _remaining_calc(f)
            patch[F_REMAINING] = max(0, cur_rem - delta)
        tbl.update(rec["id"], _remap_existing_only(tbl, patch))
    except Exception as e:
        logger.warning(f"⚠️ bump failed: {e}")


def _get_row_or_none(did: Optional[str]) -> Optional[Dict]:
    if not did or not _digits_only(did):
        logger.warning(f"⚠️ invalid DID: {did}")
        return None
    tbl = _numbers_tbl()
    try:
        for r in tbl.all():
            f = r.get("fields", {})
            if _same_did(f.get(F_NUMBER), did) or _same_did(f.get(F_FRIENDLY), did):
                return r
    except Exception as e:
        logger.warning(f"⚠️ fetch row failed: {e}")
    return None


# =========================
# Increment helpers + telemetry
# =========================
def increment_sent(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_SENT_TODAY, F_SENT_TOTAL, delta=1, dec_remaining=True)
    log_kpi("OUTBOUND_SENT", 1)
    log_run("NUMBER_BUMP", processed=1, breakdown={"did": did, "metric": "SENT"})


def increment_delivered(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_DELIV_TODAY, F_DELIV_TOTAL)
    log_kpi("OUTBOUND_DELIVERED", 1)


def increment_failed(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_FAILED_TODAY, F_FAILED_TOTAL)
    log_kpi("OUTBOUND_FAILED", 1)


def increment_opt_out(did: str):
    row = _get_row_or_none(did)
    if not row:
        return
    _bump(row, F_OPTOUT_TODAY, F_OPTOUT_TOTAL)
    log_kpi("OUTBOUND_OPTOUT", 1)


def pick_and_mark_sent(market: Optional[str] = None) -> str:
    did = get_from_number(market)
    increment_sent(did)
    logger.info(f"📤 pick_and_mark_sent({market}) → {did}")
    return did
