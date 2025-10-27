# sms/campaign_runner.py
"""
Campaign Runner vHARDENED+FINAL

✓ Campaigns: only Active or (Scheduled AND start<=now, flips to Active)
✓ Prospects: uses Campaigns.[Prospects] linked records
✓ Templates: random template per message + link Template -> Drip Queue
✓ Placeholders: {First}, {Address}, {Property City}
✓ First name parsing: robust
✓ Market: copied from Prospect
✓ TextGrid rotation: round-robin per Market (Numbers table), persisted to .tg_state.json
✓ Next Send Date: staggered 5–20 seconds between rows
✓ Quiet Hours: 9pm–9am America/Chicago → skip writes
✓ Dry-run: TEST_MODE=true env OR --dryrun flag
✓ Logging: clear per-step logs
✓ Resilience: retry without Market on INVALID_MULTIPLE_CHOICE_OPTIONS
✓ Loop guard: skip if campaign already has QUEUED/Retry/Sending… rows
✓ Dedupe: per-run phone set + Airtable check (Campaign+Phone) + optional global phone dedupe
"""

from __future__ import annotations
import argparse, os, random, re, json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")

# ---------- Configurable column names (match your Airtable exactly) ----------
CAMPAIGN_NAME_F = "Campaign Name"
CAMPAIGN_STATUS_F = "Status"                 # single select: Active, Scheduled, Paused, Completed
CAMPAIGN_START_F = "Start Time"              # datetime
CAMPAIGN_MARKET_F = "Market"                 # single select (drives TextGrid number selection)
CAMPAIGN_PROSPECTS_LINK_F = "Prospects"      # link → Prospects
CAMPAIGN_TEMPLATES_LINK_F = "Templates"      # link → Templates

PROSPECT_NAME_KEYS = [
    "Owner Name",
    "Phone 1 Name (Primary) (from Linked Owner)",
]
PROSPECT_MARKET_F = "Market"                 # single select
PROSPECT_PHONE_KEYS = [
    "Phone 1 (from Linked Owner)",
    "Phone",
    "Primary Phone",
    "Mobile",
]
PROSPECT_ADDR_F = "Property Address"
PROSPECT_CITY_F = "Property City"

DRIP_TABLE_NAME = "Drip Queue"
DRIP_CAMPAIGN_LINK_F = "Campaign"
DRIP_PROSPECT_LINK_F = "Prospect"
DRIP_TEMPLATE_LINK_F = "Template"
DRIP_MESSAGE_F = "Message"
DRIP_SELLER_PHONE_F = "Seller Phone Number"
DRIP_FROM_NUMBER_F = "TextGrid Phone Number"
DRIP_MARKET_F = "Market"
DRIP_STATUS_F = "Status"
DRIP_NEXT_SEND_F = "Next Send Date"
DRIP_UI_F = "UI"
DRIP_PROPERTY_ID_F = "Property ID"

TEMPLATE_MESSAGE_F = "Message"

NUMBERS_TABLE_NAME = "Numbers"
NUMBERS_MARKET_F = "Market"
NUMBERS_FROM_KEYS = [
    "TextGrid Phone Number",
    "Number",
    "Phone",
    "From",
    "From Number",
]
NUMBERS_STATUS_F = "Status"                  # expect 'Active' (if present)

# ---------- Behavior toggles ----------
QUIET_TZ = ZoneInfo("America/Chicago")
QUIET_START = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))
QUIET_END   = int(os.getenv("QUIET_END_HOUR_LOCAL",   "9"))
QUIET_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in ("1","true","yes")

TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1","true","yes")
SEND_AFTER_QUEUE_DEFAULT = os.getenv("RUNNER_SEND_AFTER_QUEUE", "true").lower() in ("1","true","yes")

GLOBAL_MAX_DRIPS = int(os.getenv("GLOBAL_MAX_DRIPS", "1000"))  # hard cap per campaign run
GLOBAL_PHONE_DEDUPE = os.getenv("GLOBAL_PHONE_DEDUPE", "true").lower() in ("1","true","yes")

JITTER_MIN_S = 5
JITTER_MAX_S = 20

TG_STATE_FILE = os.getenv("TG_STATE_FILE", ".tg_state.json")   # persists round-robin position per market

STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ---------- Time helpers ----------
def now_ct() -> datetime:
    return datetime.now(QUIET_TZ)

def is_quiet_hours() -> bool:
    if not QUIET_ENFORCED:
        return False
    h = now_ct().hour
    return (h >= QUIET_START) or (h < QUIET_END)

def _escape_quotes(s: str) -> str:
    return str(s).replace("'", "\\'")

# ---------- Campaign metrics and status management ----------
def _update_campaign_progress(camp_tbl, campaign_id: str, queued_count: int, campaign_name: str = "Unknown"):
    """Update campaign metrics in real-time during execution"""
    try:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        
        updates = {
            "Total Sent": queued_count,
            "Last Run At": now_iso,
            "Last Run Result": f"Queued {queued_count} messages"
        }
        
        camp_tbl.update(campaign_id, updates)
        log.info(f"📊 Updated campaign '{campaign_name}' metrics: {queued_count} queued")
        
    except Exception as e:
        log.warning(f"⚠️ Campaign progress update failed for '{campaign_name}': {e}")

def _check_campaign_status(camp_tbl, campaign_id: str) -> str:
    """Check current campaign status (for pause detection during execution)"""
    try:
        current = camp_tbl.get(campaign_id)
        status = current.get("fields", {}).get(CAMPAIGN_STATUS_F, "").strip().lower()
        return status
    except Exception as e:
        log.warning(f"⚠️ Failed to check campaign status: {e}")
        return "unknown"

def _mark_campaign_completed(camp_tbl, campaign_id: str, campaign_name: str, total_processed: int):
    """Mark campaign as Completed when all prospects have been processed"""
    try:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        
        updates = {
            CAMPAIGN_STATUS_F: "Completed",
            "Completed At": now_iso,
            "Last Run Result": f"Completed - processed {total_processed} prospects"
        }
        
        camp_tbl.update(campaign_id, updates)
        log.info(f"✅ Campaign '{campaign_name}' marked as Completed ({total_processed} prospects processed)")
        
    except Exception as e:
        log.warning(f"⚠️ Failed to mark campaign '{campaign_name}' as completed: {e}")

def _sync_to_campaign_control_base(campaign_data: Dict[str, Any]):
    """Sync campaign metrics to Campaign Control Base"""
    try:
        # Use the datastore connector for Campaign Control Base
        control_handle = CONNECTOR.campaign_control_campaigns()
        
        if control_handle.in_memory:
            log.debug("Campaign Control Base not configured - using in-memory fallback")
            return
            
        control_campaigns = control_handle.table
        campaign_name = campaign_data.get("name", "")
        if not campaign_name:
            return
            
        # Search for existing campaign in control base
        formula = f"{{Campaign Name}}='{_escape_quotes(campaign_name)}'"
        existing = control_campaigns.all(formula=formula, max_records=1)
        
        metrics_update = {
            "Total Sent": campaign_data.get("total_sent", 0),
            "Total Replies": campaign_data.get("total_replies", 0),
            "Total Opt Outs": campaign_data.get("total_opt_outs", 0),
            "Last Sync": datetime.now(timezone.utc).isoformat()
        }
        
        if existing:
            # Update existing campaign
            control_campaigns.update(existing[0]["id"], metrics_update)
            log.debug(f"📊 Synced metrics to Campaign Control Base for '{campaign_name}'")
        else:
            log.debug(f"⚠️ Campaign '{campaign_name}' not found in Campaign Control Base")
            
    except Exception as e:
        log.warning(f"⚠️ Campaign Control Base sync failed: {e}")

def _first_link(v: Any) -> Optional[str]:
    if isinstance(v, list) and v:
        return v[0]
    return None

def _first_text(v: Any) -> str:
    if isinstance(v, list):
        return str(v[0]) if v else ""
    return str(v or "")

# ---------- Name, phone, message helpers ----------
def _best_phone(pf: Dict[str, Any]) -> Optional[str]:
    for key in PROSPECT_PHONE_KEYS:
        val = pf.get(key)
        if not val:
            continue
        if isinstance(val, list):
            for it in val:
                p = normalize_phone(str(it))
                if p:
                    return p
        else:
            p = normalize_phone(str(val))
            if p:
                return p
    return None

_first_name_regex = re.compile(r"^[A-Za-z]+(?:'[A-Za-z]+)?$")
def _first_name_from(raw: str) -> str:
    if not raw:
        return ""
    for tok in str(raw).strip().split():
        tok = tok.replace(".", "")
        if _first_name_regex.match(tok):
            return tok
    m = re.match(r"[A-Za-z]+", str(raw))
    return m.group(0) if m else ""

def _render_message(tpl: str, pf: Dict[str, Any]) -> str:
    name = ""
    for k in PROSPECT_NAME_KEYS:
        raw = pf.get(k)
        if raw:
            name = _first_name_from(str(raw))
            if name:
                break
    addr = _first_text(pf.get(PROSPECT_ADDR_F))
    city = _first_text(pf.get(PROSPECT_CITY_F))
    msg = (tpl or "")
    msg = msg.replace("{First}", name)
    msg = msg.replace("{Address}", addr)
    msg = msg.replace("{Property City}", city)
    return msg.strip()

def _ct_future_iso_naive(min_s: int = JITTER_MIN_S, max_s: int = JITTER_MAX_S) -> str:
    dt = now_ct() + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _prospect_property_id(pf: Dict[str, Any]) -> Optional[str]:
    candidates = [
        "Property ID","PropertyId","PropertyID","Property Record ID",
        "Property (Record ID)","Property_ID","Property (from Linked Owner)","Property",
    ]
    for key in candidates:
        v = pf.get(key)
        if isinstance(v, list) and v:
            s = str(v[0]).strip()
            if s:
                return s
        elif isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()
    return None

# ---------- Numbers rotation (persisted) ----------
def _load_tg_state() -> Dict[str, int]:
    try:
        with open(TG_STATE_FILE, "r") as f:
            data = json.load(f)
            return {str(k): int(v) for k, v in (data or {}).items()}
    except Exception:
        return {}

def _save_tg_state(state: Dict[str, int]):
    try:
        with open(TG_STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log.warning(f"Could not persist {TG_STATE_FILE}: {e}")

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _is_active_number(fields: Dict[str, Any]) -> bool:
    # Accept if Active=true (bool or string), or Status='Active' (case-insensitive).
    if "Active" in fields:
        v = fields.get("Active")
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in ("1","true","yes")
    if NUMBERS_STATUS_F in fields:
        return str(fields.get(NUMBERS_STATUS_F) or "").strip().lower() == "active"
    return True

def _extract_number(fields: Dict[str, Any]) -> Optional[str]:
    for k in NUMBERS_FROM_KEYS:
        v = fields.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _get_numbers_for_market(numbers_tbl, market: str) -> List[str]:
    formula = f"LOWER({{{NUMBERS_MARKET_F}}})=LOWER('{_escape_quotes(market)}')"
    try:
        recs = numbers_tbl.all(formula=formula, page_size=100) or []
    except Exception as e:
        log.debug(f"Numbers .all(formula=Market) failed, fallback to client-side: {e}")
        recs = numbers_tbl.all(page_size=100) or []
    pool: List[str] = []
    for r in recs:
        f = r.get("fields", {}) or {}
        mval = f.get(NUMBERS_MARKET_F)
        if mval and _norm(mval) != _norm(market):
            continue
        if not _is_active_number(f):
            continue
        num = _extract_number(f)
        if num:
            pool.append(num)
    if not pool:
        log.warning(f"⚠️ No TextGrid numbers found for market '{market}'.")
    else:
        log.debug(f"📱 Number pool for '{market}': {pool}")
    return pool

def _choose_from_number(numbers_tbl, campaign_market: Optional[str], state: Dict[str, int]) -> Optional[str]:
    if not campaign_market:
        return None
    mk = _norm(campaign_market)
    pool = _get_numbers_for_market(numbers_tbl, campaign_market)
    if not pool:
        return None
    idx = state.get(mk, 0)
    choice = pool[idx % len(pool)]
    state[mk] = idx + 1
    return choice

# ---------- Data fetch ----------
def _fetch_campaign_by_name(tbl, name: str) -> List[Dict[str, Any]]:
    formula = f"{{{CAMPAIGN_NAME_F}}}='{_escape_quotes(name)}'"
    return tbl.all(formula=formula, page_size=100) or []

def _fetch_due_campaigns(tbl) -> List[Dict[str, Any]]:
    # Scheduled & Start <= NOW or Active (exclude Paused/Completed)
    formula = (
        f"AND("
        f"OR({{{CAMPAIGN_STATUS_F}}}='Scheduled',{{{CAMPAIGN_STATUS_F}}}='Active'),"
        f"OR({{{CAMPAIGN_STATUS_F}}}='Active',DATETIME_DIFF(NOW(),{{{CAMPAIGN_START_F}}},'seconds')>=0)"
        f")"
    )
    return tbl.all(formula=formula, page_size=100) or []

def _fetch_records_by_ids(tbl, ids: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(0, len(ids), 90):
        chunk = ids[i:i+90]
        formula = "OR(" + ",".join([f"RECORD_ID()='{_escape_quotes(rid)}'" for rid in chunk]) + ")"
        recs = tbl.all(formula=formula, page_size=100) or []
        out.extend(recs)
    return out

def _fetch_template_messages(templates_tbl, ids: List[str]) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    if not ids:
        return pairs
    recs = _fetch_records_by_ids(templates_tbl, ids)
    for r in recs:
        tid = r.get("id")
        f = r.get("fields", {}) or {}
        msg = f.get(TEMPLATE_MESSAGE_F)
        if tid and isinstance(msg, str) and msg.strip():
            pairs.append((tid, msg.strip()))
    return pairs

# ---------- Duplicate guards ----------
def _already_in_drip_campaign_phone(drip_tbl, campaign_id: str, phone: str) -> bool:
    """Airtable dedupe for (Campaign + Seller Phone) with non-Failed status; robust for linked field."""
    formula = (
        "AND("
        f"SEARCH('{_escape_quotes(campaign_id)}',ARRAYJOIN({{{DRIP_CAMPAIGN_LINK_F}}}))>0,"
        f"{{{DRIP_SELLER_PHONE_F}}}='{_escape_quotes(phone)}',"
        f"NOT({{{DRIP_STATUS_F}}}='Failed')"
        ")"
    )
    try:
        found = drip_tbl.all(formula=formula, max_records=1)
        return bool(found)
    except Exception:
        return False

def _any_active_for_phone(drip_tbl, phone: str) -> bool:
    """Optional global dedupe: skip if phone already has a row not Failed (any campaign)."""
    if not GLOBAL_PHONE_DEDUPE:
        return False
    formula = (
        "AND("
        f"{{{DRIP_SELLER_PHONE_F}}}='{_escape_quotes(phone)}',"
        f"NOT({{{DRIP_STATUS_F}}}='Failed')"
        ")"
    )
    try:
        found = drip_tbl.all(formula=formula, max_records=1)
        return bool(found)
    except Exception:
        return False

def _campaign_has_queued_rows(drip_tbl, campaign_id: str) -> bool:
    """Loop guard: if campaign already has QUEUED/Retry/Sending… rows, skip run."""
    formula = (
        "AND("
        f"SEARCH('{_escape_quotes(campaign_id)}',ARRAYJOIN({{{DRIP_CAMPAIGN_LINK_F}}}))>0,"
        f"OR({{{DRIP_STATUS_F}}}='QUEUED',{{{DRIP_STATUS_F}}}='Retry',{{{DRIP_STATUS_F}}}='Sending…')"
        ")"
    )
    try:
        found = drip_tbl.all(formula=formula, max_records=1)
        return bool(found)
    except Exception:
        return False

# ---------- Core queueing ----------
def _queue_one_campaign(
    camp_tbl,
    campaign: Dict[str, Any],
    limit: Optional[int],
    dryrun: bool,
    preview_limit: int = 5,
) -> Dict[str, Any]:
    cf = (campaign or {}).get("fields", {}) or {}
    cid = campaign.get("id")
    cname = cf.get(CAMPAIGN_NAME_F) or cf.get("Name") or "Unnamed Campaign"
    cstatus = str(cf.get(CAMPAIGN_STATUS_F) or "").strip().lower()
    cstart = cf.get(CAMPAIGN_START_F)
    cmarket = cf.get(CAMPAIGN_MARKET_F)

    # Flip Scheduled → Active if due (only after start time)
    if cstatus == "scheduled":
        if cstart:
            try:
                camp_tbl.update(cid, {CAMPAIGN_STATUS_F: "Active"})
                cstatus = "active"
                log.info(f"▶️ Campaign '{cname}' is now Active (start time reached).")
            except Exception as e:
                log.warning(f"Could not set campaign '{cname}' Active: {e}")

    # Hard stop for Paused/Completed
    if cstatus in ("paused", "completed"):
        log.info(f"⏭️ Campaign {cname} is {cstatus}; skipped.")
        return {"campaign": cname, "queued": 0, "skipped": "status"}

    # Loop guard: don’t requeue if this campaign already has pending rows
    drip_tbl = CONNECTOR.drip_queue().table
    if cid and _campaign_has_queued_rows(drip_tbl, cid):
        log.warning(f"⚠️ Campaign {cname} already has pending drips — skipping duplicate run.")
        return {"campaign": cname, "queued": 0, "skipped": "already_pending"}

    # Linked prospects
    pids = cf.get(CAMPAIGN_PROSPECTS_LINK_F) or []
    if not pids:
        log.info(f"⏭️ Campaign {cname} has no linked Prospects; skipped.")
        return {"campaign": cname, "queued": 0, "skipped": "no_prospects"}

    prospects_tbl = CONNECTOR.prospects().table
    templates_tbl  = CONNECTOR.templates().table
    numbers_tbl    = CONNECTOR.numbers().table

    prospects = _fetch_records_by_ids(prospects_tbl, pids)
    if not prospects:
        log.info(f"⚠️ Campaign {cname} linked Prospects not found; skipped.")
        return {"campaign": cname, "queued": 0, "skipped": "prospects_not_found"}

    # Templates (rotate per message). If none, messages will be blank.
    tmpl_ids = cf.get(CAMPAIGN_TEMPLATES_LINK_F) or []
    templates = _fetch_template_messages(templates_tbl, tmpl_ids)
    if not templates:
        log.warning(f"⚠️ Campaign {cname} has no valid templates; messages will be blank.")
        templates = []

    # Round-robin state (persist across runs)
    tg_state = _load_tg_state()

    # Hard cap per run
    take = len(prospects) if (not limit or limit <= 0) else min(int(limit), len(prospects))
    if take > GLOBAL_MAX_DRIPS:
        log.warning(f"⚠️ Hard cap enforced: truncating from {take} → {GLOBAL_MAX_DRIPS}")
        take = GLOBAL_MAX_DRIPS

    queued = 0
    previews: List[Dict[str, Any]] = []
    reasons = defaultdict(int)
    seen_phones: set[str] = set()  # per-run dedupe
    total_prospects = len(prospects[:take])

    for i, pr in enumerate(prospects[:take]):
        # Check for pause during execution (every 10 prospects for efficiency)
        if i % 10 == 0:
            current_status = _check_campaign_status(camp_tbl, cid)
            if current_status == "paused":
                log.info(f"🛑 Campaign '{cname}' was paused during execution - stopping at prospect {i+1}/{total_prospects}")
                break
        
        pf = (pr or {}).get("fields", {}) or {}

        # pick a deliverable phone for the seller
        phone = _best_phone(pf)
        if not phone:
            reasons["no_phone"] += 1
            continue

        # in-run dedupe (per campaign)
        if phone in seen_phones:
            reasons["dup_in_run"] += 1
            continue
        seen_phones.add(phone)

        # Airtable dedupe (Campaign + Seller Phone)
        if cid and _already_in_drip_campaign_phone(drip_tbl, cid, phone):
            reasons["dup_in_airtable"] += 1
            continue

        # Optional global phone dedupe (any campaign, not Failed)
        if _any_active_for_phone(drip_tbl, phone):
            reasons["dup_global_phone"] += 1
            continue

        # Template & message render
        if templates:
            tmpl_id, body = random.choice(templates)
        else:
            tmpl_id, body = None, ""
        rendered = _render_message(body, pf)

        # Choose From-number by campaign market (fallback: prospect market), persisted round-robin
        drip_market = pf.get(PROSPECT_MARKET_F) or ""
        from_number = _choose_from_number(numbers_tbl, cmarket or drip_market, tg_state)

        # Property ID from prospect
        prop_id = _prospect_property_id(pf)

        payload: Dict[str, Any] = {
            DRIP_CAMPAIGN_LINK_F: [cid] if cid else None,
            DRIP_PROSPECT_LINK_F: [pr.get("id")] if pr.get("id") else None,
            DRIP_TEMPLATE_LINK_F: [tmpl_id] if tmpl_id else None,
            DRIP_SELLER_PHONE_F: phone,
            DRIP_FROM_NUMBER_F: from_number,
            DRIP_MESSAGE_F: rendered,
            DRIP_MARKET_F: drip_market,
            DRIP_STATUS_F: DripStatus.QUEUED.value,
            DRIP_UI_F: STATUS_ICON["QUEUED"],
            DRIP_NEXT_SEND_F: _ct_future_iso_naive(JITTER_MIN_S, JITTER_MAX_S),
            DRIP_PROPERTY_ID_F: prop_id,
        }

        if dryrun:
            queued += 1
            if len(previews) < preview_limit:
                previews.append({
                    "prospect_id": pr.get("id"),
                    "first": _first_name_from(
                        next((pf.get(k) for k in PROSPECT_NAME_KEYS if pf.get(k)), "") or ""
                    ),
                    "from_number": from_number,
                    "market": drip_market,
                    "property_id": prop_id,
                    "message": rendered,
                    "next_send": payload[DRIP_NEXT_SEND_F],
                    "template_linked": bool(tmpl_id),
                })
            continue

        # Real write
        try:
            CONNECTOR.drip_queue().table.create(payload)
            queued += 1
        except Exception as e:
            msg = str(e)
            if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg and DRIP_MARKET_F in payload:
                retry = dict(payload)
                retry.pop(DRIP_MARKET_F, None)
                try:
                    CONNECTOR.drip_queue().table.create(retry)
                    queued += 1
                    log.warning(f"⚠️ Market select rejected ({drip_market}); queued without Market.")
                except Exception as e2:
                    reasons["create_failed"] += 1
                    log.error(f"Airtable create failed [Drip Queue] after Market retry: {e2}")
            else:
                reasons["create_failed"] += 1
                log.error(f"Airtable create failed [Drip Queue]: {e}")

        # Update campaign progress every 25 prospects (for real-time tracking)
        if not dryrun and (i + 1) % 25 == 0:
            _update_campaign_progress(camp_tbl, cid, queued, cname)

    # Final progress update and completion check
    if not dryrun:
        # Update final progress
        _update_campaign_progress(camp_tbl, cid, queued, cname)
        
        # Check if campaign should be marked as completed
        # (all linked prospects have been processed)
        if queued > 0 and queued >= total_prospects:
            _mark_campaign_completed(camp_tbl, cid, cname, total_prospects)
        
        # Sync to Campaign Control Base
        campaign_data = {
            "name": cname,
            "total_sent": queued,
            "total_replies": 0,  # Will be updated by metrics_tracker
            "total_opt_outs": 0  # Will be updated by metrics_tracker
        }
        _sync_to_campaign_control_base(campaign_data)

    # Persist round-robin pointer(s)
    if not dryrun:
        _save_tg_state(tg_state)

    log.info(f"✅ Queued {queued} for {cname}")
    if reasons:
        log.info(f"   Skips: {dict(reasons)}")

    out = {"campaign": cname, "queued": queued}
    if dryrun and previews:
        out["preview"] = previews
    return out

# ---------- Orchestrator ----------
def run_campaigns(limit: Optional[str] = "ALL", send_after_queue: bool = SEND_AFTER_QUEUE_DEFAULT,
                  campaign_name: Optional[str] = None, dryrun: bool = False) -> Dict[str, Any]:
    per_camp_limit = None if (limit is None or str(limit).upper() == "ALL") else max(int(limit), 1)

    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")
    if TEST_MODE or dryrun:
        log.info("⚠️ TEST_MODE active — dry run only.")
        dryrun = True

    # Quiet hours: allow dry-run, block real writes
    if not dryrun and is_quiet_hours():
        log.warning(f"⏸️ Quiet hours ({QUIET_START:02d}:00–{QUIET_END:02d}:00 CT). Skipping queueing.")
        return {"ok": True, "queued": 0, "quiet_hours": True}

    camp_tbl = CONNECTOR.campaigns().table
    if campaign_name:
        camps = _fetch_campaign_by_name(camp_tbl, campaign_name)
        if not camps:
            log.warning(f"⚠️ No campaign found for '{campaign_name}'.")
            return {"ok": True, "queued": 0, "test_mode": dryrun, "campaigns": []}
    else:
        try:
            camps = _fetch_due_campaigns(camp_tbl)
        except Exception as e:
            log.error(f"❌ Failed to fetch campaigns: {e}")
            return {"ok": False, "queued": 0, "error": str(e)}

    results = []
    total = 0
    for camp in camps:
        r = _queue_one_campaign(camp_tbl, camp, per_camp_limit, dryrun)
        results.append(r)
        total += int(r.get("queued", 0))

    # Optional send after queue (only when not dryrun and not quiet)
    if (not dryrun) and send_after_queue and total > 0 and not is_quiet_hours():
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
        except Exception as e:
            log.warning(f"Send after queue failed: {e}")

    return {"ok": True, "queued": total, "test_mode": dryrun,
            "campaigns": [r["campaign"] for r in results], "details": results}

# ---------- CLI ----------
def _parse_args():
    p = argparse.ArgumentParser(description="Campaign Runner")
    p.add_argument("--limit", type=str, default="ALL", help="Cap per campaign (int) or ALL")
    p.add_argument("--campaign", type=str, default=None, help="Exact Campaign Name")
    p.add_argument("--send-after-queue", action="store_true", help="Send immediately after queueing")
    p.add_argument("--no-send-after-queue", action="store_true", help="Do not send after queueing")
    p.add_argument("--dryrun", action="store_true", help="Simulate without writes (overrides TEST_MODE false)")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    send_flag = SEND_AFTER_QUEUE_DEFAULT
    if args.send_after_queue:
        send_flag = True
    if args.no_send_after_queue:
        send_flag = False
    try:
        res = run_campaigns(
            limit=args.limit,
            send_after_queue=send_flag,
            campaign_name=args.campaign,
            dryrun=args.dryrun,
        )
        import json
        print(json.dumps(res, indent=2))
    except Exception as e:
        log.error(f"Campaign run failed: {e}")
        raise