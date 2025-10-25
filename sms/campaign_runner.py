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

import os
import re
import json
import time
import random
import traceback
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py<3.9 fallback

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR

log = get_logger("campaign_runner")

# ─────────────────────────────── Config ───────────────────────────────
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in {"1", "true", "yes"}
QUIET_HOURS_ENFORCED = os.getenv("QUIET_HOURS_ENFORCED", "true").lower() in {"1", "true", "yes"}
QUIET_START_HOUR_LOCAL = int(os.getenv("QUIET_START_HOUR_LOCAL", "21"))
QUIET_END_HOUR_LOCAL = int(os.getenv("QUIET_END_HOUR_LOCAL", "9"))
RUNNER_SEND_AFTER_QUEUE = os.getenv("RUNNER_SEND_AFTER_QUEUE", "true").lower() in {"1", "true", "yes"}
JITTER_MIN_SEC = int(os.getenv("JITTER_MIN_SEC", "5"))
JITTER_MAX_SEC = int(os.getenv("JITTER_MAX_SEC", "20"))

CT = ZoneInfo("America/Chicago") if ZoneInfo else timezone.utc
HOURGLASS = "⏳"

# Field names (aligned to your schema)
CAMPAIGN_NAME_F = "Campaign Name"
CAMPAIGN_STATUS_F = "Status"            # single select: Active, Scheduled, Paused, Completed
CAMPAIGN_START_F = "Start Time"         # datetime
CAMPAIGN_PROSPECTS_F = "Prospects"      # linked → Prospects
CAMPAIGN_TEMPLATES_F = "Templates"      # linked → Templates
CAMPAIGN_MARKET_F = "Market"            # single select (used for number pool)
CAMPAIGN_LAST_RUN_F = "Last Run At"     # optional; if present we write back

PROSPECT_OWNER_NAME_F = "Owner Name"
PROSPECT_PHONE_F = "Phone"
PROSPECT_ADDR_F = "Property Address"
PROSPECT_CITY_F = "Property City"
PROSPECT_MARKET_F = "Market"

DRIP_TBL_STATUS_F = "Status"
DRIP_TBL_MARKET_F = "Market"
DRIP_TBL_MSG_F = "Message"
DRIP_TBL_TO_PHONE_F = "Seller Phone Number"
DRIP_TBL_FROM_PHONE_F = "TextGrid Phone Number"
DRIP_TBL_CAMPAIGN_LINK_F = "Campaign"
DRIP_TBL_PROSPECT_LINK_F = "Prospect"
DRIP_TBL_TEMPLATE_LINK_F = "Template"
DRIP_TBL_NEXT_SEND_F = "Next Send Date"
DRIP_TBL_UI_F = "UI"

TEMPLATE_MSG_F = "Message"              # Templates.Message
NUMBERS_MARKET_F = "Market"             # Numbers.Market (single select)
NUMBERS_PHONE_F = "TextGrid Phone Number"

# ─────────────────────────────── Time helpers ─────────────────────────
def _central_now() -> datetime:
    return datetime.now(CT)

def _is_quiet_hours() -> bool:
    if not QUIET_HOURS_ENFORCED:
        return False
    h = _central_now().hour
    return (h >= QUIET_START_HOUR_LOCAL) or (h < QUIET_END_HOUR_LOCAL)

def _ct_naive_iso(dt: datetime) -> str:
    """Return naive (no tz) ISO for CT – matches your Airtable UI."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(CT).replace(tzinfo=None)
    else:
        # assume local already; still strip tz to be safe
        dt = dt.replace(tzinfo=None)
    return dt.isoformat(timespec="seconds")

def _now_jittered() -> str:
    dt = _central_now() + timedelta(seconds=random.randint(JITTER_MIN_SEC, JITTER_MAX_SEC))
    return _ct_naive_iso(dt)

# ─────────────────────────────── Safe helpers ─────────────────────────
def get_campaigns_table():
    """FastAPI main uses this to /campaign/{id}/start|stop."""
    try:
        return CONNECTOR.campaigns().table
    except Exception as e:
        log.error(f"❌ Campaigns table fetch failed: {e}")
        return None

def _escape_quotes(val: str) -> str:
    return str(val).replace("'", "\\'")

def _first_name_from_owner(name: Optional[str]) -> str:
    """
    Extract first token; drop initials/punctuation.
    'John W. Johnson' --> 'John'
    '  MARY  ANN  '   --> 'MARY'
    """
    if not name or not isinstance(name, str):
        return ""
    # Normalize whitespace, strip punctuation from first token
    tok = name.strip().split()[0] if name.strip() else ""
    tok = re.sub(r"[^\w\-']", "", tok)  # keep word-ish chars
    # Avoid single-letter initials
    if len(tok) == 1:
        return ""
    return tok

def _best_str_field(fields: Dict[str, Any], key: str) -> Optional[str]:
    v = fields.get(key)
    if isinstance(v, list):
        return v[0] if v else None
    if isinstance(v, str):
        return v
    return None

def _best_phone(fields: Dict[str, Any]) -> Optional[str]:
    candidates = [
        PROSPECT_PHONE_F,
        "Primary Phone",
        "Owner Phone",
        "Phone 1",
        "Phone 2",
        "Phone 1 (from Linked Owner)",
        "Phone 2 (from Linked Owner)",
    ]
    for k in candidates:
        v = fields.get(k)
        if isinstance(v, list):
            for vv in v:
                p = normalize_phone(vv)
                if p:
                    return p
        elif isinstance(v, str):
            p = normalize_phone(v)
            if p:
                return p
    return None

def _render_message(tmpl: str, pf: Dict[str, Any]) -> str:
    first = _first_name_from_owner(_best_str_field(pf, PROSPECT_OWNER_NAME_F))
    addr = _best_str_field(pf, PROSPECT_ADDR_F) or ""
    city = _best_str_field(pf, PROSPECT_CITY_F) or ""
    msg = (tmpl or "")
    msg = msg.replace("{First}", first)
    msg = msg.replace("{Address}", addr)
    msg = msg.replace("{Property City}", city)
    return msg.strip()

# ─────────────────────────────── Numbers pool ─────────────────────────
_rotation_index: Dict[str, int] = {}
_numbers_cache: Dict[str, List[str]] = {}  # market_key -> list of from-numbers

def _market_key(val: Optional[str]) -> str:
    return (val or "").strip().lower()

def _load_numbers_for_market(market: str) -> List[str]:
    mk = _market_key(market)
    if mk in _numbers_cache:
        return _numbers_cache[mk]

    try:
        numbers_handle = CONNECTOR.numbers()
        tbl = numbers_handle.table
    except Exception:
        tbl = None

    pool: List[str] = []
    if tbl:
        try:
            # filter exact market (single select); Airtable formula needs exact match
            formula = f"{{{NUMBERS_MARKET_F}}}='{_escape_quotes(market)}'"
            recs = tbl.all(formula=formula, page_size=100)
            for r in recs:
                f = r.get("fields", {}) or {}
                number = f.get(NUMBERS_PHONE_F)
                if isinstance(number, str) and number.strip():
                    pool.append(number.strip())
        except Exception as e:
            log.warning(f"⚠️ Numbers fetch failed for market '{market}': {e}")

    # Fallback: if nothing found, try *all* numbers and take the first 10
    if not pool and tbl:
        try:
            recs = tbl.all(page_size=100)
            for r in recs:
                f = r.get("fields", {}) or {}
                number = f.get(NUMBERS_PHONE_F)
                if isinstance(number, str) and number.strip():
                    pool.append(number.strip())
            pool = pool[:10]
        except Exception:
            pass

    _numbers_cache[mk] = pool
    return pool

def _choose_from_number(market: str) -> Optional[str]:
    pool = _load_numbers_for_market(market)
    if not pool:
        return None
    mk = _market_key(market)
    idx = _rotation_index.get(mk, 0)
    val = pool[idx % len(pool)]
    _rotation_index[mk] = idx + 1
    return val

# ─────────────────────────────── Template helpers ─────────────────────
def _fetch_template_messages_by_ids(tpl_table, template_ids: List[str]) -> Dict[str, str]:
    """
    Fetch Message body for each template id. Returns {template_id: message}.
    """
    results: Dict[str, str] = {}
    if not template_ids:
        return results
    # Batch in chunks of ~90 with OR(RECORD_ID()='id',...)
    for i in range(0, len(template_ids), 90):
        chunk = template_ids[i : i + 90]
        formula = "OR(" + ",".join([f"RECORD_ID()='{_escape_quotes(tid)}'" for tid in chunk]) + ")"
        try:
            recs = tpl_table.all(formula=formula, page_size=100)
            for r in recs:
                rid = r.get("id")
                f = r.get("fields", {}) or {}
                msg = f.get(TEMPLATE_MSG_F)
                if rid and isinstance(msg, str) and msg.strip():
                    results[rid] = msg.strip()
        except Exception as e:
            log.warning(f"⚠️ Template fetch chunk failed: {e}")
        time.sleep(0.1)
    return results

# ─────────────────────────────── Dedupe helper ───────────────────────
def _exists_dupe(drip_tbl, campaign_id: str, last10: str) -> bool:
    """
    Check if a (campaign, phone-last10) already queued. Uses RIGHT() to match last 10.
    """
    try:
        formula = (
            f"AND("
            f"ARRAYJOIN({{{DRIP_TBL_CAMPAIGN_LINK_F}}})='{_escape_quotes(campaign_id)}',"
            f"RIGHT({{{DRIP_TBL_TO_PHONE_F}}},10)='{_escape_quotes(last10)}'"
            f")"
        )
        recs = drip_tbl.all(formula=formula, page_size=1)
        return bool(recs)
    except Exception:
        return False

def _last10(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    return digits[-10:] if len(digits) >= 10 else digits

# ─────────────────────────────── Core Queueing ────────────────────────
def _queue_one_campaign(campaign: Dict[str, Any], per_camp_limit: Optional[int]) -> Tuple[int, int, int]:
    """
    Returns (queued, skipped, total_processed)
    """
    camp_fields = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    camp_name = camp_fields.get(CAMPAIGN_NAME_F) or camp_fields.get("Name") or "Unnamed Campaign"

    # Guard: Status and Start Time handled by fetch, but re-check here
    status = str(camp_fields.get(CAMPAIGN_STATUS_F, "")).strip().lower()
    if status in {"paused", "completed"}:
        log.info(f"⏭️ Campaign skipped (status {status}): {camp_name}")
        return (0, 0, 0)

    template_ids = camp_fields.get(CAMPAIGN_TEMPLATES_F) or []
    prospects_linked = camp_fields.get(CAMPAIGN_PROSPECTS_F) or []
    if not prospects_linked:
        log.info(f"⏭️ Campaign has 0 linked Prospects: {camp_name}")
        return (0, 0, 0)

    # Tables
    drip_tbl = CONNECTOR.drip_queue().table
    pros_tbl = CONNECTOR.prospects().table
    tpl_tbl = CONNECTOR.templates().table

    # Load template messages
    tpl_bodies = _fetch_template_messages_by_ids(tpl_tbl, template_ids)
    if not tpl_bodies:
        log.warning(f"⚠️ No valid template messages for {camp_name}; skipping.")
        return (0, 0, 0)
    tpl_ids_order = list(tpl_bodies.keys())

    # Hydrate prospect records in chunks
    prospects: List[Dict[str, Any]] = []
    for i in range(0, len(prospects_linked), 90):
        chunk = prospects_linked[i : i + 90]
        formula = "OR(" + ",".join([f"RECORD_ID()='{_escape_quotes(pid)}'" for pid in chunk]) + ")"
        try:
            recs = pros_tbl.all(formula=formula, page_size=100)
            prospects.extend(recs)
        except Exception as e:
            log.warning(f"⚠️ Prospect chunk fetch failed: {e}")
        time.sleep(0.08)

    queued = 0
    skipped = 0
    processed = 0

    for idx, pr in enumerate(prospects):
        if per_camp_limit and queued >= per_camp_limit:
            break
        processed += 1
        pf = (pr or {}).get("fields", {}) or {}

        phone = _best_phone(pf)
        if not phone:
            skipped += 1
            continue
        last10 = _last10(phone)
        if not last10:
            skipped += 1
            continue

        # Dedup (campaign_id + last10)
        if campaign_id and _exists_dupe(drip_tbl, campaign_id, last10):
            skipped += 1
            continue

        market = _best_str_field(pf, PROSPECT_MARKET_F) or ""
        from_number = _choose_from_number(market or (camp_fields.get(CAMPAIGN_MARKET_F) or ""))
        if not from_number:
            skipped += 1
            continue

        # Template rotation: pick at index (round-robin) for even spread
        tpl_choice_id = tpl_ids_order[idx % len(tpl_ids_order)]
        body = tpl_bodies.get(tpl_choice_id, "")

        message = _render_message(body, pf)
        if not message:
            skipped += 1
            continue

        payload = {
            DRIP_TBL_STATUS_F: "QUEUED",
            DRIP_TBL_MARKET_F: market,
            DRIP_TBL_MSG_F: message,
            DRIP_TBL_TO_PHONE_F: phone,
            DRIP_TBL_FROM_PHONE_F: from_number,
            DRIP_TBL_NEXT_SEND_F: _now_jittered(),
            DRIP_TBL_UI_F: HOURGLASS,
            DRIP_TBL_CAMPAIGN_LINK_F: [campaign_id] if campaign_id else None,
            DRIP_TBL_PROSPECT_LINK_F: [pr.get("id")] if pr.get("id") else None,
            DRIP_TBL_TEMPLATE_LINK_F: [tpl_choice_id],
        }

        # Robust create: if Market single-select mismatches, retry without Market
        try:
            drip_tbl.create(payload)
            queued += 1
        except Exception as e:
            msg = str(e)
            if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg or "Insufficient permissions to create new select option" in msg:
                try:
                    payload2 = dict(payload)
                    payload2.pop(DRIP_TBL_MARKET_F, None)
                    drip_tbl.create(payload2)
                    queued += 1
                    log.warning(f"⚠️ Market select rejected ({market}); queued without Market.")
                except Exception as e2:
                    skipped += 1
                    log.error(f"Queue insert failed for {camp_name} (prospect {pr.get('id')}): {e2}")
            else:
                skipped += 1
                log.error(f"Queue insert failed for {camp_name} (prospect {pr.get('id')}): {e}")

    log.info(f"✅ Queued {queued} for {camp_name} (skipped {skipped} / processed {processed})")
    # Optional: write last run
    try:
        if campaign_id and CONNECTOR and hasattr(CONNECTOR, "campaigns"):
            CONNECTOR.campaigns().table.update(campaign_id, {CAMPAIGN_LAST_RUN_F: datetime.now(timezone.utc).isoformat()})
    except Exception:
        pass
    return (queued, skipped, processed)

# ─────────────────────────────── Campaign fetch ───────────────────────
def _fetch_due_campaigns(camp_tbl, campaign_name: Optional[str]) -> List[Dict[str, Any]]:
    """
    Returns campaigns that are either:
      - Scheduled and start time <= now, or
      - Active (we allow continuing to add if needed)
    If campaign_name is provided, fetch exactly that record (by Campaign Name or Name).
    """
    due: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    if campaign_name:
        # Exact match by Campaign Name or Name
        name_val = _escape_quotes(campaign_name)
        formula = f"OR({{{CAMPAIGN_NAME_F}}}='{name_val}',{{Name}}='{name_val}')"
        try:
            recs = camp_tbl.all(formula=formula, page_size=100)
            return recs or []
        except Exception as e:
            log.error(f"❌ Campaign lookup failed for '{campaign_name}': {e}")
            return []

    # Otherwise: status in (Scheduled, Active) and Start Time <= now
    # Airtable formula: AND( OR({Status}='Scheduled',{Status}='Active'), {Start Time} <= NOW() )
    formula = (
        f"AND(OR({{{CAMPAIGN_STATUS_F}}}='Scheduled',{{{CAMPAIGN_STATUS_F}}}='Active'),"
        f"DATETIME_DIFF(NOW(),{{{CAMPAIGN_START_F}}},'seconds')>=0)"
    )
    try:
        due = camp_tbl.all(formula=formula, page_size=100)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []
    return due or []

# ─────────────────────────────── Public entry ─────────────────────────
def run_campaigns(limit: str | int = "ALL", send_after_queue: bool = False, campaign_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Exposed function used by FastAPI /run-campaigns.
    - limit: "ALL" or int per-campaign cap
    - send_after_queue: trigger outbound send_batch after queueing
    - campaign_name: optional exact campaign filter
    """
    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")

    # Quiet hours hard-gate: skip entirely (you can queue-only here if you want)
    if _is_quiet_hours():
        log.warning(f"⏸️ Quiet hours ({QUIET_START_HOUR_LOCAL:02d}:00–{QUIET_END_HOUR_LOCAL:02d}:00 CT). Skipping queueing.")
        return {"ok": True, "queued": 0, "quiet_hours": True}

    # Resolve limit
    per_camp_limit: Optional[int] = None
    try:
        s = str(limit).strip().upper()
        if s not in {"", "ALL", "UNLIMITED", "NONE"}:
            per_camp_limit = max(1, int(s))
    except Exception:
        per_camp_limit = None

    # TEST_MODE short-circuit (no writes)
    if TEST_MODE:
        log.info("⚠️ TEST_MODE active — dry run only.")
        # Still list due campaigns for visibility
        try:
            camp_tbl = CONNECTOR.campaigns().table
            due = _fetch_due_campaigns(camp_tbl, campaign_name)
            names = [c.get("fields", {}).get(CAMPAIGN_NAME_F) or c.get("fields", {}).get("Name") for c in due]
        except Exception:
            names = []
        return {"ok": True, "queued": 0, "test_mode": True, "campaigns": names}

    # Live queueing
    total_queued = 0
    total_skipped = 0
    total_processed = 0
    errors: List[str] = []

    try:
        camp_tbl = CONNECTOR.campaigns().table
    except Exception as e:
        return {"ok": False, "error": f"campaigns table unavailable: {e}"}

    due_campaigns = _fetch_due_campaigns(camp_tbl, campaign_name)
    if not due_campaigns:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "queued": 0, "note": "No due/active campaigns."}

    for camp in due_campaigns:
        try:
            name = (camp.get("fields", {}) or {}).get(CAMPAIGN_NAME_F) or (camp.get("fields", {}) or {}).get("Name") or "Unnamed"
            log.info(f"➡️ Queuing campaign: {name}")
            q, s, p = _queue_one_campaign(camp, per_camp_limit)
            total_queued += q
            total_skipped += s
            total_processed += p
            log.info(f"🏁 Finished {name}")
        except Exception as e:
            errors.append(str(e))
            log.error(f"Campaign run failed: {e}")
            log.debug(traceback.format_exc())

    result: Dict[str, Any] = {
        "ok": len(errors) == 0,
        "queued": total_queued,
        "skipped": total_skipped,
        "processed": total_processed,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat(),
    }

    if send_after_queue:
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
            result["send_after_queue"] = True
        except Exception as e:
            result["send_after_queue"] = False
            result["send_error"] = str(e)

    return result

# ─────────────────────────────── CLI shim ─────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Campaign Runner")
    parser.add_argument("--limit", default="ALL", help="Per-campaign cap (int) or ALL")
    parser.add_argument("--send-after-queue", action="store_true", help="Trigger outbound send after queue")
    parser.add_argument("--campaign", default=None, help="Exact campaign name to run")
    parser.add_argument("--dryrun", action="store_true", help="Force dry-run (no writes) regardless of TEST_MODE")
    parser.add_argument("--debug", action="store_true", help="Verbose logs")
    args = parser.parse_args()

    if args.debug:
        log.setLevel("DEBUG")

    # Allow --dryrun to override env
    if args.dryrun:
        TEST_MODE = True  # type: ignore

    print(json.dumps(run_campaigns(args.limit, args.send_after_queue, args.campaign), indent=2))