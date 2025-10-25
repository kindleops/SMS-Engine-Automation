# sms/scheduler.py
"""
Bulletproof Campaign Scheduler — unified (vFINAL)

✓ Only Active/Scheduled campaigns with Start Time <= now (CT)
✓ Uses Campaigns.[Prospects] linked records (no market filtering)
✓ Random template per message + link Template
✓ Placeholders: {First}, {Address}, {Property City}
✓ First name = first token (handles commas/initials/suffixes)
✓ DripQ.Market = Prospect.Market (retry without if select mismatch)
✓ TextGrid round-robin by market; prefer Campaign.Market else Prospect.Market; persisted to .tg_state.json
✓ Next Send Date staggered +5–20s cumulatively
✓ Quiet hours (21:00–09:00 CT) skip queueing
✓ Dry-run: TEST_MODE=true or --dryrun
"""

from __future__ import annotations
import os, re, json, random, time, logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pyairtable import Table
from pyairtable.formulas import match

# Pull helpers from your project if available; otherwise do light shims
try:
    from sms.runtime import normalize_phone
except Exception:
    def normalize_phone(s: str) -> Optional[str]:
        if not s: return None
        digits = "".join(ch for ch in str(s) if ch.isdigit())
        if len(digits) == 11 and digits.startswith("1"): digits = digits[1:]
        return digits if len(digits) == 10 else None

# ─────────────────────────── ENV / CONSTANTS ───────────────────────────
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID      = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
TEST_MODE    = os.getenv("TEST_MODE", "false").lower() in ("1","true","yes")

# tz
try:
    import pytz
    CT_TZ = pytz.timezone("America/Chicago")
except Exception:
    CT_TZ = timezone(timedelta(hours=-5))  # crude fallback; prefer pytz

QUIET_START = 21
QUIET_END   = 9
STATE_FILE  = ".tg_state.json"  # TextGrid rotation position

# table names
TBL_CAMPAIGNS = "Campaigns"
TBL_PROSPECTS = "Prospects"
TBL_TEMPLATES = "Templates"
TBL_NUMBERS   = "Numbers"
TBL_DRIPQ     = "Drip Queue"

# fields (exact as you described)
F_CAMPAIGN_NAME    = "Campaign Name"
F_CAMPAIGN_STATUS  = "Status"            # Active | Scheduled | Paused | Completed
F_CAMPAIGN_START   = "Start Time"
F_CAMPAIGN_MARKET  = "Market"            # used for number pool pref
F_CAMPAIGN_PROS    = "Prospects"         # linked → Prospects
F_CAMPAIGN_TMPLS   = "Templates"         # linked → Templates

F_PROS_NAME        = "Owner Name"
F_PROS_PHONE       = "Phone"
F_PROS_ADDR        = "Property Address"
F_PROS_CITY        = "Property City"
F_PROS_MARKET      = "Market"

F_TMPL_MESSAGE     = "Message"

F_DQ_CAMPAIGN      = "Campaign"
F_DQ_PROSPECT      = "Prospect"
F_DQ_TEMPLATE      = "Template"
F_DQ_MESSAGE       = "Message"
F_DQ_TO            = "Seller Phone Number"
F_DQ_FROM          = "TextGrid Phone Number"
F_DQ_MARKET        = "Market"
F_DQ_STATUS        = "Status"            # "Queued"
F_DQ_NEXT          = "Next Send Date"
F_DQ_UI            = "UI"

F_NUM_MARKET       = "Market"
F_NUM_PHONE        = "TextGrid Phone Number"
F_NUM_STATUS       = "Status"            # e.g. Active
F_NUM_ACTIVE       = "Active"            # checkbox/bool

STATUS_QUEUED      = "Queued"

# ───────────────────────────── LOGGING ────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scheduler")

def _req_env():
    missing = [k for k in ("AIRTABLE_API_KEY","LEADS_CONVOS_BASE") if not os.getenv(k) and not os.getenv(k.replace("BASE","_BASE_ID"))]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
_req_env()

camp_tbl = Table(AIRTABLE_KEY, BASE_ID, TBL_CAMPAIGNS)
pros_tbl = Table(AIRTABLE_KEY, BASE_ID, TBL_PROSPECTS)
tmpl_tbl = Table(AIRTABLE_KEY, BASE_ID, TBL_TEMPLATES)
num_tbl  = Table(AIRTABLE_KEY, BASE_ID, TBL_NUMBERS)
drip_tbl = Table(AIRTABLE_KEY, BASE_ID, TBL_DRIPQ)

# ───────────────────────────── TIME HELPERS ───────────────────────────
def ct_now() -> datetime:
    try:
        import pytz
        return datetime.now(pytz.timezone("America/Chicago"))
    except Exception:
        return datetime.now()

def in_quiet_hours() -> bool:
    h = ct_now().hour
    return (h >= QUIET_START) or (h < QUIET_END)

def ct_naive(dt: datetime) -> str:
    """Return CT naive ISO 'YYYY-MM-DDTHH:MM:SS' for Airtable date fields."""
    try:
        import pytz
        local = dt.astimezone(pytz.timezone("America/Chicago"))
    except Exception:
        local = dt
    return local.replace(tzinfo=None).isoformat(timespec="seconds")

# ───────────────────────── FIRST NAME / TEMPLATE ──────────────────────
_SUFFIXES = {"jr","jr.","sr","sr.","iii","ii","iv"}
def first_only(name: Optional[str]) -> str:
    if not name: return ""
    s = str(name).strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        s = parts[1] if len(parts) > 1 else parts[0]
    toks = re.split(r"[^\w']+", s)
    toks = [t for t in toks if t]
    if not toks: return ""
    first = toks[0]
    if len(first) == 1 or first.lower().strip(".") in _SUFFIXES:
        if len(toks) > 1:
            first = toks[1]
    return first.capitalize()

def fill_placeholders(template_msg: str, pf: Dict[str, any]) -> str:
    addr = pf.get(F_PROS_ADDR)
    city = pf.get(F_PROS_CITY)
    msg = (template_msg or "")
    msg = msg.replace("{First}", first_only(pf.get(F_PROS_NAME)))
    msg = msg.replace("{Address}", str(addr or ""))
    msg = msg.replace("{Property City}", str(city or ""))
    return msg

# ───────────────────── TEXTGRID ROUND-ROBIN STATE ─────────────────────
def _load_state() -> Dict[str, int]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_state(state: Dict[str, int]):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"State save failed: {e}")

def _get_numbers_by_market() -> Dict[str, List[str]]:
    by_mkt: Dict[str, List[str]] = {}
    for rec in num_tbl.all():
        f = rec.get("fields", {}) or {}
        if not f.get(F_NUM_ACTIVE):  # must be truthy
            continue
        if str(f.get(F_NUM_STATUS, "")).strip().lower() not in ("active",""):
            # if you mark active via checkbox only, allow blank Status too
            continue
        mkt = f.get(F_NUM_MARKET)
        num = f.get(F_NUM_PHONE)
        if not (mkt and num):
            continue
        by_mkt.setdefault(mkt, []).append(str(num))
    return by_mkt

def pick_tg_number(camp_market: Optional[str], prospect_market: Optional[str], pool: Dict[str, List[str]], state: Dict[str, int]) -> Optional[str]:
    market = camp_market or prospect_market
    if not market: return None
    nums = pool.get(market) or []
    if not nums: return None
    last = state.get(market, -1)
    idx = (last + 1) % len(nums)
    state[market] = idx
    return nums[idx]

# ─────────────────────────── DATA FETCH HELPERS ───────────────────────
def _parse_start(raw: Optional[str]) -> Optional[datetime]:
    if not raw: return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z","+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def fetch_due_campaigns(specific_name: Optional[str] = None) -> List[Dict]:
    if specific_name:
        recs = camp_tbl.all(formula=match({F_CAMPAIGN_NAME: specific_name}))
    else:
        recs = camp_tbl.all(formula="OR({Status}='Active', {Status}='Scheduled')")
    now_ct = ct_now()
    due = []
    for r in recs:
        f = r.get("fields", {}) or {}
        status = str(f.get(F_CAMPAIGN_STATUS,"")).strip().lower()
        if status in ("paused","completed"):
            continue
        st = _parse_start(f.get(F_CAMPAIGN_START))
        if st:
            # compare in CT
            st_ct = ct_now().tzinfo.localize(st.replace(tzinfo=None)) if hasattr(now_ct, "tzinfo") else st
            if st_ct > now_ct:
                continue
        due.append(r)
    return due

def _chunk(ids: List[str], n: int) -> List[List[str]]:
    return [ids[i:i+n] for i in range(0, len(ids), n)]

def fetch_prospects_by_ids(ids: List[str]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for chunk in _chunk(ids, 50):
        formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
        for rec in pros_tbl.all(formula=formula):
            out[rec["id"]] = rec
    return out

def fetch_templates_by_ids(ids: List[str]) -> List[Dict]:
    res: List[Dict] = []
    for chunk in _chunk(ids, 50):
        formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
        for rec in tmpl_tbl.all(formula=formula):
            f = rec.get("fields", {}) or {}
            body = (f.get(F_TMPL_MESSAGE) or "").strip()
            if body:
                res.append(rec)
    return res

# ───────────────────────────── CREATE HELPERS ─────────────────────────
def create_drip(payload: Dict[str, any]) -> bool:
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        msg = str(e)
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg and "Market" in payload:
            log.warning("⚠️ Market select rejected; retrying without Market.")
            safe = dict(payload); safe.pop(F_DQ_MARKET, None)
            try:
                drip_tbl.create(safe)
                return True
            except Exception as e2:
                log.error(f"Create failed after Market retry: {e2}")
                return False
        log.error(f"Create failed: {e}")
        return False

# ───────────────────────────── RUNNER CORE ────────────────────────────
def _queue_one_campaign(camp: Dict, per_limit: Optional[int], numbers_pool: Dict[str, List[str]], tg_state: Dict[str, int], dryrun: bool) -> int:
    cf = camp.get("fields", {}) or {}
    cname = cf.get(F_CAMPAIGN_NAME) or cf.get("Name") or "Unnamed Campaign"
    prospect_ids: List[str] = cf.get(F_CAMPAIGN_PROS) or []
    if not prospect_ids:
        log.warning(f"⚠️ Campaign '{cname}' has no linked prospects.")
        return 0

    pmap = fetch_prospects_by_ids(prospect_ids)

    tmpl_ids: List[str] = cf.get(F_CAMPAIGN_TMPLS) or []
    templates = fetch_templates_by_ids(tmpl_ids) if tmpl_ids else []
    if not templates:
        log.error(f"❌ Campaign '{cname}' has no valid templates.")
        return 0

    # base time: now or Start Time (whichever later), then stagger 5–20s each row
    start = ct_now()
    t = start + timedelta(seconds=random.randint(5, 20))

    total = 0
    ids_iter = prospect_ids[: per_limit] if per_limit else prospect_ids

    for pid in ids_iter:
        prec = pmap.get(pid)
        if not prec:
            continue
        pf = prec.get("fields", {}) or {}

        # message
        tmpl = random.choice(templates)
        tmpl_id = tmpl["id"]
        body = (tmpl.get("fields", {}).get(F_TMPL_MESSAGE) or "").strip()
        msg  = fill_placeholders(body, pf)

        # phones
        to_phone = normalize_phone(pf.get(F_PROS_PHONE))
        if not to_phone:
            continue

        # markets
        prospect_market = pf.get(F_PROS_MARKET)
        campaign_market  = cf.get(F_CAMPAIGN_MARKET)
        from_phone = pick_tg_number(campaign_market, prospect_market, numbers_pool, tg_state)
        next_send  = ct_naive(t)

        payload = {
            F_DQ_CAMPAIGN: [camp["id"]],
            F_DQ_PROSPECT: [pid],
            F_DQ_TEMPLATE: [tmpl_id],
            F_DQ_MESSAGE:  msg,
            F_DQ_TO:       to_phone,
            F_DQ_FROM:     from_phone,       # can be None -> outbound backfill
            F_DQ_MARKET:   prospect_market,  # retry-less if mismatch
            F_DQ_STATUS:   STATUS_QUEUED,
            F_DQ_NEXT:     next_send,
            F_DQ_UI:       "⏳",
        }

        if dryrun or TEST_MODE:
            log.info(f"[dryrun] {cname} :: {pf.get(F_PROS_NAME)} → {msg} :: TG={from_phone} :: {next_send}")
        else:
            if not create_drip(payload):
                t += timedelta(seconds=random.randint(5, 20))
                continue

        total += 1
        t += timedelta(seconds=random.randint(5, 20))
        time.sleep(0.02)

    return total

def run_scheduler(limit: Optional[int] = None, campaign_name: Optional[str] = None, dryrun: bool = False) -> Dict[str, any]:
    if in_quiet_hours():
        log.warning("⏸️ Quiet hours (21:00–09:00 CT). Skipping queueing.")
        return {"ok": True, "queued": 0, "quiet_hours": True}

    log.info(f"🚀 Scheduler start — limit={limit if limit else 'ALL'} dryrun={dryrun or TEST_MODE}")

    try:
        camps = fetch_due_campaigns(campaign_name)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return {"ok": False, "error": str(e)}

    if not camps:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "queued": 0, "note": "No due/active campaigns"}

    numbers_pool = _get_numbers_by_market()
    tg_state = _load_state()

    total = 0
    processed = 0
    for c in camps:
        cf = c.get("fields", {}) or {}
        status = str(cf.get(F_CAMPAIGN_STATUS,"")).lower()
        if status in ("paused","completed"):
            log.info(f"⏭️  Skipping {cf.get(F_CAMPAIGN_NAME) or 'Unnamed'} (status={status})")
            continue

        cname = cf.get(F_CAMPAIGN_NAME) or "Unnamed Campaign"
        log.info(f"➡️ Queuing campaign: {cname}")
        q = _queue_one_campaign(c, limit, numbers_pool, tg_state, dryrun)
        _save_state(tg_state)
        log.info(f"✅ Queued {q} for {cname}")
        processed += 1
        total += q

    log.info(f"🏁 Scheduler done — campaigns={processed}, queued={total}, dryrun={dryrun or TEST_MODE}")
    return {"ok": True, "campaigns": processed, "queued": total, "dryrun": (dryrun or TEST_MODE)}

# ─────────────────────────────── CLI ──────────────────────────────────
if __name__ == "__main__":
    import argparse, json as _json
    ap = argparse.ArgumentParser(description="Bulletproof Campaign Scheduler vFINAL")
    ap.add_argument("--limit", type=int, help="Cap prospects per campaign (default ALL)")
    ap.add_argument("--campaign", help="Run a single campaign by 'Campaign Name'")
    ap.add_argument("--dryrun", action="store_true", help="Log only (no writes)")
    args = ap.parse_args()

    res = run_scheduler(limit=args.limit, campaign_name=args.campaign, dryrun=args.dryrun)
    print(_json.dumps(res, indent=2))
