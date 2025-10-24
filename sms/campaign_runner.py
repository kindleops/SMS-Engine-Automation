# sms/campaign_runner.py
from __future__ import annotations
import re
import random
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")
QUIET_TZ = ZoneInfo("America/Chicago")

STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ------------------------ time helpers ------------------------
def _ct_future_iso_naive(min_s: int = 2, max_s: int = 12) -> str:
    dt = (datetime.now(QUIET_TZ) + timedelta(seconds=random.randint(min_s, max_s)))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

# --------------------- text helpers / placeholders ---------------------
_PLACEHOLDER_RX = re.compile(r"\{([^{}]+)\}")

# map common placeholder keys -> prospect field names
_FIELD_ALIASES = {
    "First": ["First", "First Name", "FirstName"],
    "Last": ["Last", "Last Name", "LastName"],
    "Full Name": ["Full Name", "Name"],
    "Address": ["Property Address", "Address", "PropertyAddress", "Street Address"],
    "Property City": ["Property City", "City", "City (Property)"],
    "State": ["State", "Property State"],
    "Zip": ["Zip", "Zip Code", "Postal Code"],
}

def _lookup_value(p_fields: Dict[str, Any], key: str) -> Optional[str]:
    # Exact key first
    v = p_fields.get(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    # Try aliases
    for alt in _FIELD_ALIASES.get(key, []):
        v = p_fields.get(alt)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def render_message(template_body: str, p_fields: Dict[str, Any]) -> str:
    if not template_body:
        return ""
    def _sub(m: re.Match) -> str:
        key = m.group(1).strip()
        val = _lookup_value(p_fields, key)
        return val if val is not None else ""
    return _PLACEHOLDER_RX.sub(_sub, template_body)

# --------------------- Airtable helpers ---------------------
def _safe_table(fn) -> Optional[Any]:
    try:
        return fn()
    except Exception as e:
        log.error(f"❌ Table fetch failed: {e}")
        return None

def _get_campaigns_table():
    return _safe_table(lambda: CONNECTOR.campaigns().table)

def _get_prospects_table():
    return _safe_table(lambda: CONNECTOR.prospects().table)

def _get_templates_table():
    return _safe_table(lambda: CONNECTOR.templates().table)

def _get_numbers_table():
    return _safe_table(lambda: CONNECTOR.numbers().table)

def _get_drip_table():
    return _safe_table(lambda: CONNECTOR.drip_queue().table)

def _first_id(lst: Optional[List[str]]) -> Optional[str]:
    if isinstance(lst, list) and lst:
        for x in lst:
            if isinstance(x, str) and x.strip():
                return x
    return None

def _get_template_body(templates_table, template_id: Optional[str]) -> Optional[str]:
    if not (templates_table and template_id):
        return None
    try:
        rec = templates_table.get(template_id)
    except Exception as e:
        log.warning(f"Template read failed: {e}")
        return None
    f = (rec or {}).get("fields", {}) or {}
    for key in ("Body", "Message", "Text", "Template", "Content"):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _extract_number(number_fields: Dict[str, Any]) -> Optional[str]:
    for k in ("TextGrid Phone Number", "TextGrid Number", "Number", "Phone Number"):
        v = number_fields.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _get_numbers_for_market(numbers_table, market_value: Optional[str]) -> List[Dict[str, Any]]:
    if not numbers_table:
        return []
    if not market_value:
        # no market on campaign → return all numbers (fallback)
        try:
            return numbers_table.all()
        except Exception as e:
            log.error(f"Read Numbers (all) failed: {e}")
            return []
    # filter numbers by Campaign.Market (this is the pool you said controls sending)
    try:
        # if market contains quotes/commas, Airtable formula still ok with single quotes
        formula = f"{{Market}}='{market_value}'"
        return numbers_table.all(formula=formula)
    except Exception as e:
        log.error(f"Read Numbers by Market failed ({market_value}): {e}")
        return []

# ---------------------- robust drip create ----------------------
def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    """
    Create in Drip Queue with best effort:
      - Try full payload
      - If select option fails for Market, retry without Market
    """
    if not drip_tbl:
        return False

    p = dict(payload)

    # 1) full attempt
    try:
        drip_tbl.create(p)
        return True
    except Exception as e:
        msg = str(e)
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg and "Market" in msg:
            log.warning(f"⚠️ Market select rejected ({payload.get('Market')}); retrying without Market.")
            p2 = dict(p)
            p2.pop("Market", None)
            try:
                drip_tbl.create(p2)
                return True
            except Exception as e2:
                log.error(f"Airtable create failed [Drip Queue] after Market retry: {e2}")
                return False
        elif 'UNKNOWN_FIELD_NAME' in msg and '"Prospects"' in msg:
            # safety: if bad field was included, drop it and retry
            p2 = dict(p)
            p2.pop("Prospects", None)
            try:
                drip_tbl.create(p2)
                return True
            except Exception as e2:
                log.error(f"Airtable create failed [Drip Queue] after Prospects retry: {e2}")
                return False
        else:
            log.error(f"Airtable create failed [Drip Queue]: {e}")
            return False

# ---------------------- fetch active/scheduled campaigns ----------------------
def _fetch_target_campaigns(campaigns_table) -> List[Dict[str, Any]]:
    """
    We want:
      - Status = 'Active'
      - OR Status = 'Scheduled' AND {Start Time} <= NOW()
    """
    if not campaigns_table:
        return []
    formula = "OR({Status}='Active', AND({Status}='Scheduled', {Start Time}<=NOW()))"
    try:
        recs = campaigns_table.all(formula=formula)
        return recs or []
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []

# ---------------------- linked prospects loader ----------------------
def _hydrate_linked_prospects(prospects_table, linked_ids: Optional[List[str]]) -> List[Dict[str, Any]]:
    if not prospects_table or not linked_ids:
        return []
    out: List[Dict[str, Any]] = []
    for pid in linked_ids:
        if not isinstance(pid, str):
            continue
        try:
            rec = prospects_table.get(pid)
            if rec:
                out.append(rec)
        except Exception as e:
            log.debug(f"Prospect read failed ({pid}): {e}")
    return out

# ---------------------- per-campaign queue builder ----------------------
def _build_campaign_queue(campaign: Dict[str, Any], per_camp_limit: Optional[int]) -> Tuple[int, int]:
    """
    Returns: (queued_count, attempted_count)
    """
    drip_tbl = _get_drip_table()
    templates_tbl = _get_templates_table()
    prospects_tbl = _get_prospects_table()
    numbers_tbl = _get_numbers_table()

    cf = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_name = cf.get("Name") or cf.get("Campaign Name") or "Unnamed Campaign"

    # Template
    tmpl_id = _first_id(cf.get("Templates"))
    tmpl_body = _get_template_body(templates_tbl, tmpl_id)
    if not tmpl_body:
        log.warning(f"⚠️ Campaign '{campaign_name}' has no valid Template body; skipping.")
        return (0, 0)

    # Prospects: use ONLY linked prospects on the campaign
    linked_prospects = cf.get("Prospects") or cf.get("Prospect")
    prospects = _hydrate_linked_prospects(prospects_tbl, linked_prospects)
    if not prospects:
        log.info(f"⚠️ Campaign '{campaign_name}' has 0 linked Prospects; skipping.")
        return (0, 0)

    # Numbers: pool controlled by Campaign.Market (as you specified)
    numbers_pool = _get_numbers_for_market(numbers_tbl, cf.get("Market"))
    if not numbers_pool:
        log.warning(f"⚠️ No TextGrid numbers found for Campaign '{campaign_name}' Market='{cf.get('Market')}'.")
        # We still queue with empty TextGrid number (outbound can backfill if you’ve built that).
        # If you prefer to hard-stop, return (0, 0) here.

    max_items = len(prospects) if (per_camp_limit is None or per_camp_limit == "ALL") else max(0, int(per_camp_limit))
    queued = 0
    attempted = 0

    for idx, p in enumerate(prospects[:max_items]):
        attempted += 1
        pf = (p or {}).get("fields", {}) or {}

        # phone
        raw_phone = pf.get("Phone 1 (from Linked Owner)") or pf.get("Phone") or pf.get("Primary Phone") or pf.get("Mobile")
        if not raw_phone:
            continue
        phone_norm = normalize_phone(str(raw_phone)) or str(raw_phone)

        # round-robin TextGrid number from the campaign's number pool
        tg_number = None
        if numbers_pool:
            pick = numbers_pool[idx % len(numbers_pool)]
            tg_number = _extract_number((pick or {}).get("fields", {}) or {})

        # Prospect Market (for Drip Queue; retry without if select mismatch)
        prospect_market = pf.get("Market") or pf.get("market") or pf.get("Market Name")

        # message with placeholders
        message = render_message(tmpl_body, pf)

        payload: Dict[str, Any] = {
            "Campaign": [campaign_id] if campaign_id else None,
            "Prospect": [p.get("id")] if p.get("id") else None,  # singular only; DripQ doesn't have "Prospects"
            "Seller Phone Number": phone_norm,
            "TextGrid Phone Number": tg_number,  # may be None; outbound can backfill if you support it
            "Message": message,
            "Market": prospect_market,           # retry logic will drop it if select option mismatch
            "Property ID": pf.get("Property ID") or pf.get("Property") or pf.get("PropertyId"),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _ct_future_iso_naive(2, 12),
            "Template": [tmpl_id] if tmpl_id else None,
        }

        if _robust_create_drip(drip_tbl, payload):
            queued += 1
        else:
            log.error(f"Queue insert failed for {campaign_name} (prospect {p.get('id')}).")

    log.info(f"✅ Queued {queued}/{attempted} for campaign → {campaign_name}")
    return (queued, attempted)

# ---------------------- main entry ----------------------
def run_campaigns(limit: int | str = "ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")

    campaigns_tbl = _get_campaigns_table()
    targets = _fetch_target_campaigns(campaigns_tbl)

    if not targets:
        log.info("⚠️ No eligible campaigns (Active or Scheduled & due).")
        return {"ok": True, "queued": 0, "attempted": 0, "campaigns": 0, "note": "No eligible campaigns."}

    total_q = 0
    total_a = 0
    processed = 0
    errors: List[str] = []

    # If you pass a numeric limit, apply per-campaign cap; "ALL" means no per-campaign cap
    per_camp_limit = None if (isinstance(limit, str) and str(limit).upper() == "ALL") else int(limit)

    for camp in targets:
        try:
            q, a = _build_campaign_queue(camp, per_camp_limit)
            total_q += q
            total_a += a
            processed += 1
        except Exception as e:
            errors.append(str(e))
            log.error(f"Campaign queue failed: {e}")
            log.debug(traceback.format_exc())

    result = {
        "ok": True,
        "campaigns": processed,
        "queued": total_q,
        "attempted": total_a,
        "errors": errors,
        "timestamp": _now_iso_utc(),
    }

    if send_after_queue:
        try:
            from sms.outbound_batcher import send_batch
            # Send immediately; no campaign_id filter, respect your batcher’s own caps
            send_batch(limit=500)
            result["send_after_queue"] = True
        except Exception as e:
            log.warning(f"Send after queue failed: {e}")
            result["send_after_queue"] = False
            result["send_error"] = str(e)

    log.info(f"✅ Campaign Runner complete → queued {total_q} (attempted {total_a}) across {processed} campaigns")
    return result

async def run_campaigns_main(limit: int | str = "ALL", send_after_queue: bool = True):
    import asyncio
    return await asyncio.to_thread(run_campaigns, limit, send_after_queue)

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
