# sms/campaign_runner.py
"""
Campaign Runner vFINAL

✓ Campaigns: only Active/Scheduled, start time <= now (America/Chicago)
✓ Prospects: uses Campaigns.[Prospects] linked records
✓ Templates: random template per message + link Template -> Drip Queue
✓ Placeholders: {First}, {Address}, {Property City}
✓ First name parsing: only first token, robust against commas/initials/suffixes
✓ Market: copied from Prospect (single select)
✓ TextGrid rotation: round-robin per Market (Numbers table), persisted to .tg_state.json
✓ Next Send Date: staggered 5–20 seconds between rows (accumulating, not all same minute)
✓ Quiet Hours: 9pm–9am America/Chicago → skip entirely
✓ Dry-run: TEST_MODE=true env OR --dryrun flag (logs only, no writes)
✓ Logging: clear per-step logs
✓ Resilience: retries without Market on INVALID_MULTIPLE_CHOICE_OPTIONS, page size <= 100
"""

from __future__ import annotations
import os, re, json, random, time, logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from pyairtable import Table
from pyairtable.formulas import match
import pytz

# ─────────────────────────── ENV / CONSTANTS ───────────────────────────
AIRTABLE_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_BASE   = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
TEST_MODE    = os.getenv("TEST_MODE", "false").lower() in ("1","true","yes")

CT_TZ        = pytz.timezone("America/Chicago")
QUIET_START  = 21   # 9pm
QUIET_END    = 9    # 9am
STATE_FILE   = ".tg_state.json"  # for TextGrid round-robin position

# Tables (names exactly per your schema)
TBL_CAMPAIGNS = "Campaigns"
TBL_PROSPECTS = "Prospects"
TBL_TEMPLATES = "Templates"
TBL_NUMBERS   = "Numbers"
TBL_DRIPQ     = "Drip Queue"

# Drip Queue Status value
DRIPQ_STATUS_QUEUED = "Queued"

# ───────────────────────────── LOGGING ────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("campaign_runner")

def _req_env():
    missing = [k for k in ("AIRTABLE_API_KEY","LEADS_CONVOS_BASE") if not os.getenv(k) and not os.getenv(k.replace("BASE","_BASE_ID"))]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
_req_env()

camp_tbl = Table(AIRTABLE_KEY, LEADS_BASE, TBL_CAMPAIGNS)
pros_tbl = Table(AIRTABLE_KEY, LEADS_BASE, TBL_PROSPECTS)
tmpl_tbl = Table(AIRTABLE_KEY, LEADS_BASE, TBL_TEMPLATES)
num_tbl  = Table(AIRTABLE_KEY, LEADS_BASE, TBL_NUMBERS)
drip_tbl = Table(AIRTABLE_KEY, LEADS_BASE, TBL_DRIPQ)

# ───────────────────────────── TIME HELPERS ───────────────────────────
def ct_now() -> datetime:
    return datetime.now(CT_TZ)

def in_quiet_hours() -> bool:
    h = ct_now().hour
    return (h >= QUIET_START) or (h < QUIET_END)

def ct_naive(dt: datetime) -> str:
    """Return CT naive ISO string 'YYYY-MM-DDTHH:MM:SS' (Airtable-friendly)."""
    local = dt.astimezone(CT_TZ)
    return local.replace(tzinfo=None).isoformat(timespec="seconds")

# ───────────────────────── FIRST NAME / TEMPLATE ──────────────────────
_SUFFIXES = {"jr", "jr.", "sr", "sr.", "iii", "ii", "iv"}
def first_name_only(name: Optional[str]) -> str:
    if not name: return ""
    s = name.strip()
    # Handle "Last, First" → take part after comma
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        s = parts[1] if len(parts) > 1 else parts[0]
    # Split on spaces, ampersands, slashes, etc.
    tokens = re.split(r"[^\w']+", s)
    tokens = [t for t in tokens if t]
    if not tokens: return ""
    first = tokens[0]
    # Drop suffix/initials if the first token is one
    fclean = first.strip(".").lower()
    if len(first) == 1 or fclean in _SUFFIXES:
        # try next token if exists
        if len(tokens) > 1:
            first = tokens[1]
    return first.capitalize()

def fill_placeholders(template_msg: str, pf: Dict[str, any]) -> str:
    msg = template_msg or ""
    repl = {
        "{First}": first_name_only(pf.get("Owner Name")),
        "{Address}": pf.get("Property Address"),
        "{Property City}": pf.get("Property City"),
    }
    for k, v in repl.items():
        msg = msg.replace(k, v if v else k)
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

def get_numbers_by_market() -> Dict[str, List[str]]:
    by_mkt: Dict[str, List[str]] = {}
    # Airtable max page size is 100; .all() paginates under the hood safely
    for rec in num_tbl.all():
        f = rec.get("fields", {})
        mkt = f.get("Market")
        num = f.get("TextGrid Phone Number")
        if not (mkt and num):
            continue
        by_mkt.setdefault(mkt, []).append(str(num))
    return by_mkt

def pick_textgrid_number(market: Optional[str], pool: Dict[str, List[str]], state: Dict[str, int]) -> Optional[str]:
    if not market:  # allow outbound backfill if missing
        return None
    nums = pool.get(market) or []
    if not nums:
        return None
    last = state.get(market, -1)
    idx = (last + 1) % len(nums)
    state[market] = idx
    return nums[idx]

# ─────────────────────────── DATA FETCH HELPERS ───────────────────────
def fetch_due_campaigns(specific_name: Optional[str] = None) -> List[Dict]:
    """Return campaigns that are Active/Scheduled and whose Start Time <= now (or blank)."""
    if specific_name:
        recs = camp_tbl.all(formula=match({"Campaign Name": specific_name}))
    else:
        # Pull Active/Scheduled and filter time in Python to avoid Airtable formula gotchas
        recs = camp_tbl.all(formula="OR({Status}='Active', {Status}='Scheduled')")
    due: List[Dict] = []
    now_ct = ct_now()
    for r in recs:
        f = r.get("fields", {})
        status = (f.get("Status") or "").strip().lower()
        if status in ("paused", "completed"):
            continue
        st_raw = f.get("Start Time")
        if st_raw:
            try:
                # Airtable returns ISO; parse safely
                st = datetime.fromisoformat(st_raw.replace("Z", "+00:00")).astimezone(CT_TZ)
                if st > now_ct:
                    continue
            except Exception:
                # If unparsable, be liberal and include
                pass
        due.append(r)
    return due

def _chunk(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

def fetch_prospects_by_ids(ids: List[str]) -> Dict[str, Dict]:
    """Batch fetch prospects by record IDs; returns id->record map."""
    result: Dict[str, Dict] = {}
    # Keep chunks modest (Airtable formula length); 50 is safe
    for chunk in _chunk(ids, 50):
        formula = "OR(" + ",".join([f"RECORD_ID()='{rid}'" for rid in chunk]) + ")"
        for rec in pros_tbl.all(formula=formula):
            result[rec["id"]] = rec
    return result

def fetch_templates() -> List[Dict]:
    """Return templates with non-empty Message."""
    t = []
    for rec in tmpl_tbl.all():
        f = rec.get("fields", {})
        body = (f.get("Message") or "").strip()
        if body:
            t.append(rec)
    return t

# ───────────────────────────── CREATE HELPERS ─────────────────────────
def create_drip_row(payload: Dict[str, any]) -> bool:
    """Create a Drip Queue row. If Market select mismatches, retry once without Market."""
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        msg = str(e)
        # If single-select option mismatch (e.g., missing comma typo), retry w/o Market
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg and "Market" in payload:
            log.warning("⚠️ Market select rejected; retrying without Market.")
            safe = dict(payload)
            safe.pop("Market", None)
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
    cf = camp.get("fields", {})
    cname = cf.get("Campaign Name") or cf.get("Name") or "Unnamed Campaign"

    prospect_ids: List[str] = cf.get("Prospects") or []
    if not prospect_ids:
        log.warning(f"⚠️ Campaign '{cname}' has no linked prospects.")
        return 0

    # Fetch prospects in batches; map id->record
    pmap = fetch_prospects_by_ids(prospect_ids)
    templates = fetch_templates()
    if not templates:
        log.error("❌ No templates found (Templates.Message is empty).")
        return 0

    total = 0
    # Staggered time: accumulate 5–20s per message
    t = ct_now() + timedelta(seconds=random.randint(5, 20))

    # Respect limit if provided
    ids_iter = prospect_ids[: per_limit] if per_limit else prospect_ids

    for pid in ids_iter:
        prec = pmap.get(pid)
        if not prec:
            continue
        pf = prec.get("fields", {}) or {}

        # Template selection + placeholder fill
        tmpl = random.choice(templates)
        tmpl_id = tmpl["id"]
        tmpl_body = (tmpl.get("fields", {}).get("Message") or "").strip()
        final_msg = fill_placeholders(tmpl_body, pf)

        market = pf.get("Market")
        tg_num  = pick_textgrid_number(market, numbers_pool, tg_state)
        next_send = ct_naive(t)  # CT naive

        payload = {
            "Campaign": [camp["id"]],
            "Prospect": [pid],
            "Template": [tmpl_id],
            "Message": final_msg,
            "Seller Phone Number": pf.get("Phone"),
            "TextGrid Phone Number": tg_num,   # may be None; outbound can backfill if desired
            "Market": market,                  # single select; retried w/o if option mismatch
            "Status": DRIPQ_STATUS_QUEUED,
            "Next Send Date": next_send,
        }

        if dryrun or TEST_MODE:
            log.info(f"[dryrun] {cname} :: {pf.get('Owner Name')} → {final_msg} :: TG={tg_num} :: {next_send}")
        else:
            if not create_drip_row(payload):
                # skip but keep rotating
                t += timedelta(seconds=random.randint(5, 20))
                continue

        total += 1
        # advance stagger time 5–20 seconds to avoid clumping
        t += timedelta(seconds=random.randint(5, 20))
        # tiny sleep to be gentle with API (not a rate cap, just civility)
        time.sleep(0.03)

    return total

def run_campaigns(campaign_name: Optional[str] = None, limit: Optional[int] = None, dryrun: bool = False) -> Dict[str, any]:
    if in_quiet_hours():
        log.warning("⏸️ Quiet hours (21:00–09:00 CT). Skipping queueing.")
        return {"ok": True, "queued": 0, "quiet_hours": True}

    log.info(f"🚀 Campaign Runner — limit={limit if limit else 'ALL'}, dryrun={dryrun or TEST_MODE}")

    try:
        campaigns = fetch_due_campaigns(campaign_name)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return {"ok": False, "error": str(e)}

    if not campaigns:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "queued": 0, "note": "No due/active campaigns"}

    numbers_pool = get_numbers_by_market()
    tg_state = _load_state()

    total = 0
    processed = 0
    for camp in campaigns:
        cf = camp.get("fields", {})
        cname = cf.get("Campaign Name") or cf.get("Name") or "Unnamed Campaign"
        status = (cf.get("Status") or "").lower()
        if status in ("paused", "completed"):
            log.info(f"⏭️  Skipping {cname} (status={status})")
            continue

        log.info(f"➡️ Queuing campaign: {cname}")
        q = _queue_one_campaign(camp, limit, numbers_pool, tg_state, dryrun)
        _save_state(tg_state)
        log.info(f"✅ Queued {q} for {cname}")
        processed += 1
        total += q

    log.info(f"🏁 Finished. Campaigns processed={processed}, total queued={total}")
    return {"ok": True, "campaigns": processed, "queued": total, "dryrun": (dryrun or TEST_MODE)}

# ─────────────────────────────── CLI ──────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Campaign Runner vFINAL")
    ap.add_argument("--campaign", help="Run a single campaign by 'Campaign Name'")
    ap.add_argument("--limit", type=int, help="Prospect cap per campaign (default ALL)")
    ap.add_argument("--dryrun", action="store_true", help="Log only (no Airtable writes)")
    args = ap.parse_args()

    res = run_campaigns(args.campaign, args.limit, args.dryrun)
    print(json.dumps(res, indent=2))
