from __future__ import annotations
import os, re, json, math, traceback
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

AIRTABLE_KEY          = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE     = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
PROSPECTS_TABLE       = os.getenv("PROSPECTS_TABLE", "Prospects")
DRIP_QUEUE_TABLE      = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
CAMPAIGNS_TABLE       = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
NUMBERS_TABLE         = os.getenv("NUMBERS_TABLE", "Numbers")

# pacing + quiet hours
QUIET_TZ          = ZoneInfo(os.getenv("QUIET_TZ", "America/Chicago"))
QUIET_START_HOUR  = int(os.getenv("QUIET_START_HOUR", "21"))  # 9pm
QUIET_END_HOUR    = int(os.getenv("QUIET_END_HOUR", "9"))     # 9am
DAILY_LIMIT_FALLBACK = int(os.getenv("DAILY_LIMIT", "750"))

def utcnow() -> datetime: return datetime.now(timezone.utc)

def _api(bid: str) -> Api: return Api(AIRTABLE_KEY)
def _tbl(base_id: str, name: str):
    return _api(base_id).table(base_id, name)

def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str): return None
    ds = "".join(__import__("re").findall(r"\d+", s))
    return ds if len(ds) >= 10 else None

def _local_naive_iso(dt_utc: datetime) -> str:
    return dt_utc.astimezone(QUIET_TZ).replace(tzinfo=None).isoformat(timespec="seconds")

def _in_quiet_hours(dt_utc: datetime) -> bool:
    local = dt_utc.astimezone(QUIET_TZ)
    return (local.hour >= QUIET_START_HOUR) or (local.hour < QUIET_END_HOUR)

def _shift_to_window(dt_utc: datetime) -> datetime:
    local = dt_utc.astimezone(QUIET_TZ)
    if local.hour >= QUIET_START_HOUR:
        local = (local + timedelta(days=1)).replace(hour=QUIET_END_HOUR, minute=0, second=0, microsecond=0)
    elif local.hour < QUIET_END_HOUR:
        local = local.replace(hour=QUIET_END_HOUR, minute=0, second=0, microsecond=0)
    return local.astimezone(timezone.utc)

def _safe_update(tbl, rid: str, payload: Dict): 
    try:
        if payload: tbl.update(rid, payload)
    except Exception: traceback.print_exc()

def _safe_create(tbl, payload: Dict):
    try:
        if payload: return tbl.create(payload)
    except Exception: traceback.print_exc()

def _campaign_market_map():
    cmap = {}
    try:
        t = _tbl(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE)
        for r in t.all():
            cmap[r["id"]] = r.get("fields", {}).get("Market")
    except Exception: traceback.print_exc()
    return cmap

def _prospect_market_map():
    pmap = {}
    try:
        t = _tbl(LEADS_CONVOS_BASE, PROSPECTS_TABLE)
        for r in t.all():
            pmap[r["id"]] = r.get("fields", {}).get("Market")
    except Exception: traceback.print_exc()
    return pmap

def _to_e164(f: Dict[str, Any]) -> Optional[str]:
    for key in ("Number", "A Number", "Phone", "E164", "Friendly Name"):
        v = f.get(key)
        if isinstance(v, str) and _digits_only(v):
            # Prefer + prefix if present, else return digits
            return v if v.strip().startswith("+") else "+" + _digits_only(v)
    return None

def _supports_market(f: Dict[str, Any], market: Optional[str]) -> bool:
    if not market: return True
    if f.get("Market") == market: return True
    ms = f.get("Markets")
    return isinstance(ms, list) and market in ms

def _pick_number_for_market(market: Optional[str]):
    nums = _tbl(CAMPAIGN_CONTROL_BASE, NUMBERS_TABLE)
    try:
        rows = nums.all()
    except Exception:
        traceback.print_exc(); return (None, None)
    elig = []
    for r in rows:
        f = r.get("fields", {})
        if f.get("Active") is False: continue
        if str(f.get("Status") or "").lower() == "paused": continue
        if not _supports_market(f, market): continue

        rem = f.get("Remaining")
        try: rem = int(rem) if rem is not None else None
        except Exception: rem = None
        if rem is None:
            sent_today = int(f.get("Sent Today") or 0)
            daily_cap  = int(f.get("Daily Reset") or DAILY_LIMIT_FALLBACK)
            rem = max(0, daily_cap - sent_today)
        if rem <= 0: continue

        last_used = f.get("Last Used") or "1970-01-01T00:00:00Z"
        elig.append((( -rem, str(last_used) ), r))
    if not elig: return (None, None)
    elig.sort(key=lambda x: x[0])
    chosen = elig[0][1]
    did = _to_e164(chosen.get("fields", {}))
    return did, chosen["id"]

def backfill_numbers_for_existing_queue(view: Optional[str] = None,
                                        per_number_rate: int = 20,
                                        respect_quiet_hours: bool = True) -> Dict[str, int]:
    """
    Finds Drip rows with status QUEUED and missing from_number / From Number.
    Assigns Numbers.Number by market (Campaign -> row.Market -> Prospect) and
    resequences next_send_date at 'per_number_rate' (msgs per number per minute).
    """
    drip = _tbl(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    cmap = _campaign_market_map()
    pmap = _prospect_market_map()

    rows = drip.all(view=view) if view else drip.all()
    now = utcnow()
    if respect_quiet_hours and _in_quiet_hours(now):
        now = _shift_to_window(now)

    # pacing per number
    sec_per_msg = max(1, int(math.ceil(60.0 / max(1, per_number_rate))))
    next_by_num: Dict[str, datetime] = defaultdict(lambda: now)

    updated = 0; skipped = 0; scanned = 0
    for r in rows:
        scanned += 1
        f = r.get("fields", {})
        if (f.get("status") or f.get("Status")) not in ("QUEUED","READY","SENDING"):
            continue
        if f.get("from_number") or f.get("From Number"):
            # If number exists, just resequence
            did = f.get("from_number") or f.get("From Number")
            t = max(next_by_num[did], now); next_by_num[did] = t + timedelta(seconds=sec_per_msg)
            _safe_update(drip, r["id"], {"next_send_date": _local_naive_iso(t), "UI": "⏳"})
            updated += 1
            continue

        # resolve market
        market = f.get("Market")
        camp = f.get("Campaign") or []
        if isinstance(camp, list) and camp:
            market = market or cmap.get(camp[0])
        prospect = f.get("Prospect") or []
        if isinstance(prospect, list) and prospect:
            market = market or pmap.get(prospect[0])

        did, _num_id = _pick_number_for_market(market)
        if not did:
            skipped += 1
            continue

        t = max(next_by_num[did], now); next_by_num[did] = t + timedelta(seconds=sec_per_msg)
        _safe_update(drip, r["id"], {
            "from_number": did, "From Number": did,
            "next_send_date": _local_naive_iso(t),
            "UI": "⏳"
        })
        updated += 1

    return {"scanned": scanned, "updated": updated, "skipped": skipped}

def resequence_next_send(per_number_rate: int = 20,
                         only_campaign_id: Optional[str] = None,
                         view: Optional[str] = None,
                         respect_quiet_hours: bool = True) -> Dict[str, int]:
    """
    Recomputes next_send_date for QUEUED items so they go out ASAP,
    paced at N msgs / number / minute. Leaves from_number as-is.
    """
    drip = _tbl(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE)
    rows = drip.all(view=view) if view else drip.all()

    now = utcnow()
    if respect_quiet_hours and _in_quiet_hours(now):
        now = _shift_to_window(now)

    sec_per_msg = max(1, int(math.ceil(60.0 / max(1, per_number_rate))))
    # group by number to pace per DID
    pernum: Dict[str, List[Dict]] = defaultdict(list)
    for r in rows:
        f = r.get("fields", {})
        if (f.get("status") or f.get("Status")) not in ("QUEUED","READY"): 
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

    # deterministic order: created time if present, else record id
    updated = 0
    for did, group in pernum.items():
        group.sort(key=lambda r: (str(r.get("fields", {}).get("created_at") or ""), r["id"]))
        next_t = now
        for r in group:
            _safe_update(drip, r["id"], {"next_send_date": _local_naive_iso(next_t), "UI": "⏳"})
            next_t = next_t + timedelta(seconds=sec_per_msg)
            updated += 1

    return {"groups": len(pernum), "updated": updated}