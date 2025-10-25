# sms/campaign_runner.py
from __future__ import annotations
import os, random, time, hashlib, re, traceback
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional

from sms.runtime import get_logger
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")

# UI icons (unchanged)
STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ============== time helpers ==============
def _ct_future_iso_naive(min_s: int = 3, max_s: int = 25) -> str:
    from zoneinfo import ZoneInfo
    QUIET_TZ = ZoneInfo("America/Chicago")
    dt = datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _now() -> datetime:
    return datetime.now()

# ============== field helpers ==============
def _field_get(fields: dict, *names: str, default=None):
    for n in names:
        if n in fields and fields[n] not in (None, ""):
            return fields[n]
    return default

def _norm_market(s: str | None) -> str | None:
    if not s:
        return None
    s = " ".join(str(s).strip().split())
    s = s.replace(" ,", ",").replace(",,", ",")
    # normalize comma spacing to “City, ST”
    s = re.sub(r"\s*,\s*", ", ", s)
    return s

# ============== name parsing + placeholders ==============
HONORIFICS = {"mr", "mrs", "ms", "dr", "miss", "sir", "madam", "mister"}

def _extract_first_name(full_name: Optional[str]) -> str:
    """
    Robust first-name only:
      - Handles 'LAST, FIRST M.' -> FIRST
      - Handles 'First Middle Last' -> First
      - Strips honorifics and punctuation
      - Handles delimiters: '&', '/', '+'
    """
    if not full_name:
        return ""

    s = str(full_name).strip()
    # Split on household delimiters – take first person
    s = re.split(r"[&/,+]", s, maxsplit=1)[0].strip()

    # If "LAST, FIRST ..." format, take the part after comma
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2:
            s = parts[1]

    # Remove periods in initials (J., M.)
    s = s.replace(".", " ").strip()
    # Collapse whitespace
    s = " ".join(s.split())

    tokens = s.split(" ")
    # Drop honorifics
    tokens = [t for t in tokens if t and t.lower() not in HONORIFICS]
    if not tokens:
        return ""

    first = tokens[0]
    # Strip leftover punctuation/quotes
    first = first.strip(" '\"-")
    return first

def _apply_placeholders(template_body: str, pf: dict, first_name: str) -> str:
    """
    Replace common placeholders without breaking other fields that already work.
    We only force-fill first name tokens; others pass through if not found.
    """
    # Canonical map for fields you already use elsewhere
    mapping = {
        "{First}": first_name,
        "{first}": first_name,
        "{FIRST}": first_name.upper(),
        "{First Name}": first_name,
        "{First_Name}": first_name,
    }

    # Do *only* first-name replacements explicitly
    msg = template_body
    for k, v in mapping.items():
        msg = msg.replace(k, v)

    return msg

# ============== template body ==============
def _get_template_body(templates_table, template_id: str) -> Optional[str]:
    try:
        rec = templates_table.get(template_id)
    except Exception as e:
        log.warning(f"[campaign_runner] Template read failed: {e}")
        return None
    f = (rec or {}).get("fields", {}) or {}
    for key in ("Body", "Message", "Text", "Template", "Content"):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

# ============== textgrid numbers ==============
def _number_from_fields(f: dict) -> Optional[str]:
    return _field_get(
        f,
        "TextGrid Phone Number",
        "Phone Number",
        "Number",
        "From Number",
        default=None,
    )

def _is_active_number(f: dict) -> bool:
    active = f.get("Active")
    return True if active is None else bool(active)

@lru_cache(maxsize=256)
def _load_numbers_for_market(market_value: Optional[str]) -> List[str]:
    mv = _norm_market(market_value)
    if not mv:
        return []
    try:
        numbers_tbl = CONNECTOR.numbers().table
    except Exception as e:
        log.error(f"[numbers] Failed to get Numbers table: {e}")
        return []
    formula = f"{{Market}}='{mv}'"
    try:
        recs = numbers_tbl.all(formula=formula, page_size=100)
    except Exception as e:
        log.error(f"[numbers] Query failed for market {mv}: {e}")
        return []
    pool: List[str] = []
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        if not _is_active_number(f):
            continue
        num = _number_from_fields(f)
        if num:
            pool.append(str(num).strip())
    # dedupe preserve order
    return list(dict.fromkeys(pool))

def _stable_rr_index(prospect_id: str, pool_len: int) -> int:
    if pool_len <= 0:
        return 0
    h = hashlib.sha1(prospect_id.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % pool_len

def _pick_textgrid_number_for_campaign(campaign_fields: dict, prospect_id: str) -> Optional[str]:
    campaign_market = _field_get(campaign_fields, "Market", "Market Name", "market")
    pool = _load_numbers_for_market(campaign_market)
    if not pool:
        return None
    return pool[_stable_rr_index(prospect_id, len(pool))]

# ============== create drip with graceful fallbacks ==============
def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        msg = str(e)
        # If Market single-select value missing in Drip Queue config, retry without Market
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg or "Insufficient permissions" in msg:
            bad_market = payload.pop("Market", None)
            log.warning(f"[campaign_runner] ⚠️ Market select rejected ({bad_market}); retrying without Market.")
            try:
                drip_tbl.create(payload)
                return True
            except Exception as e2:
                log.error(f"[campaign_runner] Retry failed after removing Market: {e2}")
                return False
        log.error(f"[campaign_runner] Airtable create failed [Drip Queue]: {e}")
        return False

# ============== campaign filters ==============
def _eligible_campaigns(camp_tbl) -> List[Dict]:
    now_iso = _now().isoformat()
    # Active OR (Scheduled and Start Time <= now), AND not Paused/Completed
    formula = (
        f"AND("
        f"OR({{Status}}='Active',AND({{Status}}='Scheduled',{{Start Time}}<='{now_iso}')),"
        f"NOT({{Status}}='Paused'),"
        f"NOT({{Status}}='Completed')"
        f")"
    )
    try:
        return camp_tbl.all(formula=formula, page_size=100)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []

# ============== core queueing ==============
def _queue_one_campaign(camp: Dict, limit: int) -> int:
    camp_id = camp.get("id")
    cf = camp.get("fields", {}) or {}
    cname = cf.get("Name") or "Unnamed Campaign"
    log.info(f"➡️ Queuing campaign: {cname}")

    drip_tbl = CONNECTOR.drip_queue().table

    # Linked prospects (REQUIRED)
    pros_link = cf.get("Prospects") or []
    if not pros_link:
        log.warning(f"[campaign_runner] ⚠️ No linked prospects for campaign {cname}")
        return 0

    # Fetch those prospects
    pros_tbl = CONNECTOR.prospects().table
    linked_ids = ",".join([f"RECORD_ID()='{pid}'" for pid in pros_link])
    formula = f"OR({linked_ids})"
    try:
        prospects = pros_tbl.all(formula=formula, page_size=min(max(limit, 1), 100))
    except Exception as e:
        log.error(f"[campaign_runner] Failed to fetch prospects for {cname}: {e}")
        return 0
    if not prospects:
        log.warning(f"[campaign_runner] ⚠️ No prospects returned for {cname}")
        return 0

    # Templates (round-robin)
    tmpl_links = cf.get("Templates") or []
    tmpl_pool = tmpl_links.copy()
    tmpl_total = len(tmpl_pool)
    tmpl_idx = 0
    templates_tbl = CONNECTOR.templates().table if tmpl_total else None

    queued = 0
    for p in prospects:
        pf = (p or {}).get("fields", {}) or {}
        pid = p.get("id") or ""

        # Phones / Market / Names
        phone = _field_get(
            pf,
            "Seller Phone Number",
            "Phone 1 (from Linked Owner)",
            "Phone",
            "Primary Phone",
            "Mobile",
        )
        if not phone:
            continue

        prospect_market = _norm_market(_field_get(pf, "Market", "market", "Market Name"))
        first_name = _extract_first_name(
            _field_get(pf, "First Name", "Owner First Name", "Seller Name", "Owner Name", "Name")
        )

        # Template round robin -> body + placeholders
        message = None
        tmpl_id = None
        if tmpl_total and templates_tbl:
            tmpl_id = tmpl_pool[tmpl_idx % tmpl_total]
            tmpl_idx += 1
            body = _get_template_body(templates_tbl, tmpl_id)
            if body:
                message = _apply_placeholders(body, pf, first_name)

        # Fallback message if no body
        if not message:
            message = f"Hi {first_name}, this is Ryan, a local investor. Are you still the owner? Reply STOP to opt out."

        # TextGrid number chosen per campaign market, evenly distributed by prospect id
        tg_number = _pick_textgrid_number_for_campaign(cf, pid)

        payload: Dict[str, Any] = {
            "Campaign": [camp_id] if camp_id else None,
            "Prospect": [pid] if pid else None,
            "Seller Phone Number": str(phone).strip(),
            "TextGrid Phone Number": tg_number,
            "Message": message,
            "Market": prospect_market,  # will be removed on retry if select mismatch
            "Property ID": _field_get(pf, "Property ID", "Property", "PropertyId"),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _ct_future_iso_naive(3, 25),
            "Template": [tmpl_id] if tmpl_id else None,
        }

        if _robust_create_drip(drip_tbl, payload):
            queued += 1

        # natural spacing inside queue build to reduce stampede
        time.sleep(random.uniform(0.05, 0.15))

    log.info(f"✅ Queued {queued} messages for campaign → {cname}")
    return queued

# ============== public API ==============
def run_campaigns(limit="ALL", send_after_queue=True) -> Dict[str, Any]:
    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")

    camp_tbl = CONNECTOR.campaigns().table
    campaigns = _eligible_campaigns(camp_tbl)
    if not campaigns:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "queued": 0, "processed": 0}

    per_camp_limit = 1_000_000 if str(limit).upper() == "ALL" else max(int(limit), 1)
    total_queued = 0

    for camp in campaigns:
        try:
            q = _queue_one_campaign(camp, per_camp_limit)
            total_queued += q
        except Exception as e:
            log.error(f"[campaign_runner] Campaign queue failed: {e}")
            log.debug(traceback.format_exc())

    if send_after_queue and total_queued > 0:
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
        except Exception as e:
            log.error(f"⚠️ Outbound batch send failed: {e}")

    log.info(f"🏁 Done — {total_queued} queued across {len(campaigns)} campaigns.")
    return {"ok": True, "queued": total_queued, "processed": len(campaigns)}

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
