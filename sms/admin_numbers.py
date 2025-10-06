# sms/admin_numbers.py
from __future__ import annotations

import os
import re
import traceback
from datetime import datetime, timezone, date
from functools import lru_cache
from typing import Any, Dict, Optional, List

from dotenv import load_dotenv
load_dotenv()

from pyairtable import Api

# ---- ENV ----
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
AIRTABLE_API_KEY  = os.getenv("AIRTABLE_API_KEY")

DRIP_QUEUE_TABLE  = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
CAMPAIGNS_TABLE   = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
PROSPECTS_TABLE   = os.getenv("PROSPECTS_TABLE", "Prospects")

NUMBERS_BASE      = os.getenv("NUMBERS_BASE") or os.getenv("CAMPAIGN_CONTROL_BASE") or LEADS_CONVOS_BASE
NUMBERS_TABLE     = os.getenv("NUMBERS_TABLE", "Numbers")

# ---- Quick helpers ----
def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _field(f: Dict, *opts):
    for k in opts:
        if k in f:
            return f[k]
    nf = {_norm(k): k for k in f.keys()}
    for k in opts:
        ak = nf.get(_norm(k))
        if ak:
            return f[ak]
    return None

def _as_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str):  return _norm(v) in {"1","true","yes","active","enabled"}
    if isinstance(v, (int,float)): return v != 0
    return False

def _e164(num: str | None) -> str | None:
    if not isinstance(num, str): return None
    digits = "".join(re.findall(r"\d+", num))
    if not digits: return None
    if len(digits) == 10: return "+1" + digits
    if digits.startswith("1") and len(digits) == 11: return "+" + digits
    if num.startswith("+"): return num
    return "+" + digits

def _today_iso(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(timezone.utc)).date().isoformat()

def _existing_fields(table) -> set[str]:
    try:
        probe = table.all(max_records=1)
        return set((probe[0]["fields"].keys()) if probe else [])
    except Exception:
        return set()

def _safe_update(table, rec_id: str, payload: Dict) -> Optional[Dict]:
    try:
        fields = _existing_fields(table)
        if fields:
            payload = {k: v for k, v in payload.items() if k in fields}
        if payload:
            return table.update(rec_id, payload)
    except Exception:
        traceback.print_exc()
    return None

# ---- Airtable clients ----
@lru_cache(maxsize=None)
def _api(base: str | None) -> Optional[Api]:
    if not (AIRTABLE_API_KEY and base):
        return None
    return Api(AIRTABLE_API_KEY)

@lru_cache(maxsize=None)
def tbl_drip():
    api = _api(LEADS_CONVOS_BASE)
    return api.table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE) if api else None

@lru_cache(maxsize=None)
def tbl_campaigns():
    api = _api(LEADS_CONVOS_BASE)
    return api.table(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE) if api else None

@lru_cache(maxsize=None)
def tbl_prospects():
    api = _api(LEADS_CONVOS_BASE)
    return api.table(LEADS_CONVOS_BASE, PROSPECTS_TABLE) if api else None

@lru_cache(maxsize=None)
def tbl_numbers():
    api = _api(NUMBERS_BASE)
    return api.table(NUMBERS_BASE, NUMBERS_TABLE) if api else None

# ---- Number selection (same logic as runner) ----
def _pick_from_number(market: str | None) -> str | None:
    nums = tbl_numbers()
    if not nums:
        return None
    try:
        rows = nums.all()
    except Exception:
        traceback.print_exc()
        return None

    def row_ok(r, want_market):
        f = r.get("fields", {})
        mk = _field(f, "Market", "market")
        active = _as_bool(_field(f, "Active", "Enabled", "Status"))
        if want_market and mk != want_market:
            return False
        return active

    pool = [r for r in rows if row_ok(r, market)] or [r for r in rows if row_ok(r, None)]
    if not pool:
        return None

    def util(r):
        f = r["fields"]
        cap  = _field(f, "Daily Cap", "Daily Limit", "Daily Quota") or 999999
        used = _field(f, "Sent Today", "Used Today", "Today Sent") or 0
        try: cap = int(cap) or 999999
        except: cap = 999999
        try: used = int(used) or 0
        except: used = 0
        return used / cap

    pool.sort(key=util)
    f = pool[0]["fields"]
    did = _field(f, "From Number", "Number", "Phone", "DID", "Twilio Number")
    return _e164(did)

# ===================================================================
# 1) Backfill missing from_number on QUEUED Drip Queue rows
# ===================================================================
def backfill_drip_from_numbers(max_scan: int = 5000, dry_run: bool = False) -> Dict[str, int]:
    """
    Finds Drip Queue rows with status=QUEUED and missing from_number,
    infers Market (Campaign > Drip row > Prospect), picks a DID from Numbers,
    and writes it back.
    """
    dq = tbl_drip()
    camps = tbl_campaigns()
    pros = tbl_prospects()
    if not dq:
        return {"scanned": 0, "updated": 0, "skipped": 0}

    # cache campaigns & prospects we touch
    camp_cache: Dict[str, Dict] = {}
    pros_cache: Dict[str, Dict] = {}

    def _get_campaign_market(link_ids: List[str] | None) -> Optional[str]:
        if not (camps and link_ids):
            return None
        cid = link_ids[0] if isinstance(link_ids, list) and link_ids else None
        if not cid: return None
        if cid not in camp_cache:
            try: camp_cache[cid] = camps.get(cid)
            except Exception: camp_cache[cid] = {}
        return _field(camp_cache[cid].get("fields", {}), "Market", "market")

    def _get_prospect_market(link_ids: List[str] | None) -> Optional[str]:
        if not (pros and link_ids):
            return None
        pid = link_ids[0] if isinstance(link_ids, list) and link_ids else None
        if not pid: return None
        if pid not in pros_cache:
            try: pros_cache[pid] = pros.get(pid)
            except Exception: pros_cache[pid] = {}
        return _field(pros_cache[pid].get("fields", {}), "Market", "market")

    updated = 0
    skipped = 0
    scanned = 0

    try:
        rows = dq.all(max_records=max_scan)
    except Exception:
        traceback.print_exc()
        return {"scanned": 0, "updated": 0, "skipped": 0}

    for r in rows:
        scanned += 1
        f = r.get("fields", {})
        status = _field(f, "status", "Status")
        has_from = bool(_field(f, "from_number", "From Number"))
        if status != "QUEUED" or has_from:
            skipped += 1
            continue

        # infer market: Campaign > row > Prospect
        mkt = _get_campaign_market(_field(f, "Campaign")) or _field(f, "Market", "market") or _get_prospect_market(_field(f, "Prospect"))
        did = _pick_from_number(mkt)
        if not did:
            skipped += 1
            continue

        if dry_run:
            updated += 1
            continue

        _safe_update(dq, r["id"], {"from_number": did, "From Number": did})
        updated += 1

    return {"scanned": scanned, "updated": updated, "skipped": skipped}

# ===================================================================
# 2) Recalculate Numbers.Sent Today from Drip Queue rows sent today
# ===================================================================
def recalc_numbers_sent_today(for_date: Optional[str] = None) -> Dict[str, int]:
    """
    Looks at Drip Queue rows with status in {'SENT','DELIVERED'}
    whose send/deliver timestamp is today, and sets Numbers.'Sent Today'
    to the count per from_number.
    """
    dq = tbl_drip()
    nums = tbl_numbers()
    if not (dq and nums):
        return {}

    target_day = for_date or _today_iso()
    counts: Dict[str, int] = {}

    def _row_is_today(ff: Dict) -> bool:
        # check common timestamp fields
        for key in ("sent_at", "Sent At", "delivered_at", "Delivered At", "last_updated", "Last Updated", "next_send_date", "Next Send Date"):
            val = _field(ff, key)
            if not isinstance(val, str):
                continue
            # iso date prefix compare
            if val[:10] == target_day:
                return True
        return False

    try:
        rows = dq.all()  # assume manageable size; otherwise iterate by pages
    except Exception:
        traceback.print_exc()
        return {}

    for r in rows:
        f = r.get("fields", {})
        st = _field(f, "status", "Status")
        if st not in {"SENT", "DELIVERED"}:
            continue
        if not _row_is_today(f):
            continue
        did = _field(f, "from_number", "From Number")
        if not did:  # skip if still missing
            continue
        counts[did] = counts.get(did, 0) + 1

    # Write back to Numbers: set (not increment) today's usage
    try:
        nrows = nums.all()
    except Exception:
        traceback.print_exc()
        return counts

    # index numbers by their DID field
    def num_row_did(ff: Dict) -> Optional[str]:
        return _e164(_field(ff, "From Number", "Number", "Phone", "DID", "Twilio Number"))

    for n in nrows:
        nf = n.get("fields", {})
        did = num_row_did(nf)
        used = counts.get(did, 0)
        _safe_update(nums, n["id"], {"Sent Today": used, "Today Sent": used, "Used Today": used})

    return counts

# ===================================================================
# 3) (Optional) Daily reset helper
# ===================================================================
def reset_numbers_daily_counters() -> int:
    """
    Zero out 'Sent Today' on all Numbers (helpful if your scheduler isn't already doing this).
    """
    nums = tbl_numbers()
    if not nums:
        return 0
    try:
        rows = nums.all()
    except Exception:
        traceback.print_exc()
        return 0
    changed = 0
    for r in rows:
        _safe_update(nums, r["id"], {"Sent Today": 0, "Today Sent": 0, "Used Today": 0})
        changed += 1
    return changed