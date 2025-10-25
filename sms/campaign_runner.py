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
    s = re.sub(r"\s*,\s*", ", ", s)
    return s

# ============== name parsing ==============
HONORIFICS = {"mr", "mrs", "ms", "dr", "miss", "sir", "madam", "mister"}

def _extract_first_name(full_name: Optional[str]) -> str:
    if not full_name:
        return ""
    s = str(full_name).strip()
    s = re.split(r"[&/,+]", s, maxsplit=1)[0].strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2:
            s = parts[1]
    s = s.replace(".", " ").strip()
    s = " ".join(s.split())
    tokens = s.split(" ")
    tokens = [t for t in tokens if t and t.lower() not in HONORIFICS]
    if not tokens:
        return ""
    return tokens[0].strip(" '\"-")

# ============== placeholder replacement ==============
def _apply_placeholders(template_body: str, pf: dict, first_name: str) -> str:
    """
    Smart field-aware placeholder replacement.
    - Replaces {First} variants with extracted first name.
    - Replaces any {FieldName} with value from prospect fields (fuzzy match).
    - Removes unresolved placeholders cleanly.
    """
    msg = template_body or ""

    # First name variants
    first_variants = [
        "{First}", "{first}", "{FIRST}", "{First Name}", "{First_Name}",
    ]
    for key in first_variants:
        msg = msg.replace(key, first_name or "")

    # Match {FieldName} placeholders
    all_placeholders = set(re.findall(r"\{([^{}]+)\}", msg))
    for ph in all_placeholders:
        normalized = ph.lower().replace("_", " ").strip()
        replacement = None
        # exact match
        for k, v in pf.items():
            if not v:
                continue
            if normalized == k.lower().strip():
                replacement = str(v)
                break
        # fuzzy partials (Address → Property Address)
        if not replacement:
            for k, v in pf.items():
                if not v:
                    continue
                if normalized in k.lower():
                    replacement = str(v)
                    break
        msg = msg.replace(f"{{{ph}}}", replacement or "")

    # clean leftovers
    msg = re.sub(r"\{[^{}]+\}", "", msg)
    return " ".join(msg.split())

# ============== template handling ==============
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

# ============== Airtable create with retry ==============
def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        msg = str(e)
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

# ============== queue campaigns ==============
def _queue_one_campaign(camp: Dict, limit: int) -> int:
    camp_id = camp.get("id")
    cf = camp.get("fields", {}) or {}
    cname = cf.get("Name") or "Unnamed Campaign"
    log.info(f"➡️ Queuing campaign: {cname}")

    drip_tbl = CONNECTOR.drip_queue().table
    pros_link = cf.get("Prospects") or []
    if not pros_link:
        log.warning(f"[campaign_runner] ⚠️ No linked prospects for campaign {cname}")
        return 0

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

    tmpl_links = cf.get("Templates") or []
    tmpl_total = len(tmpl_links)
    templates_tbl = CONNECTOR.templates().table if tmpl_total else None
    tmpl_idx = 0

    queued = 0
    for p in prospects:
        pf = (p or {}).get("fields", {}) or {}
        pid = p.get("id") or ""
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

        message = None
        tmpl_id = None
        if tmpl_total and templates_tbl:
            tmpl_id = tmpl_links[tmpl_idx % tmpl_total]
            tmpl_idx += 1
            body = _get_template_body(templates_tbl, tmpl_id)
            if body:
                message = _apply_placeholders(body, pf, first_name)

        if not message:
            message = f"Hi {first_name}, this is Ryan, a local investor. Are you still the owner? Reply STOP to opt out."

        tg_number = _pick_textgrid_number_for_campaign(cf, pid)
        payload = {
            "Campaign": [camp_id] if camp_id else None,
            "Prospect": [pid] if pid else None,
            "Seller Phone Number": str(phone).strip(),
            "TextGrid Phone Number": tg_number,
            "Message": message,
            "Market": prospect_market,
            "Property ID": _field_get(pf, "Property ID", "Property", "PropertyId"),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _ct_future_iso_naive(3, 25),
            "Template": [tmpl_id] if tmpl_id else None,
        }

        if _robust_create_drip(drip_tbl, payload):
            queued += 1
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
    print(run_campaigns(limit=5, send_after_queue=False))
