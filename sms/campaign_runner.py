# sms/campaign_runner.py
from __future__ import annotations

import random
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus  # expects .QUEUED.value == "Queued"

log = get_logger("campaign_runner")
QUIET_TZ = ZoneInfo("America/Chicago")

STATUS_ICON = {"QUEUED": "⏳"}

# ------------------------- time helpers -------------------------
def _now_central() -> datetime:
    return datetime.now(QUIET_TZ)

def _ct_iso_naive(dt: datetime) -> str:
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _next_send_for_index(idx: int, base_gap: float = 7.0, jitter: float = 3.0) -> str:
    delay = idx * base_gap + random.uniform(-jitter, jitter)
    when = _now_central() + timedelta(seconds=max(delay, 0))
    return _ct_iso_naive(when)

# ------------------------- field helpers -------------------------
def _field(fields: Dict[str, Any], *names: str) -> Optional[Any]:
    for n in names:
        if n in fields and fields[n] not in (None, "", []):
            return fields[n]
    return None

def _extract_campaign_start(fields: Dict[str, Any]) -> Optional[datetime]:
    dt_raw = _field(fields, "Start Time", "Start", "Go Live At", "Start Date")
    if not dt_raw:
        return None
    try:
        if isinstance(dt_raw, str):
            dtp = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
        else:
            dtp = dt_raw
        if dtp.tzinfo is None:
            return dtp.replace(tzinfo=QUIET_TZ)
        return dtp.astimezone(QUIET_TZ)
    except Exception:
        return None

def _campaign_status(fields: Dict[str, Any]) -> str:
    return str(_field(fields, "Status") or "").strip()

def _campaign_name(fields: Dict[str, Any]) -> str:
    return str(_field(fields, "Name", "Campaign Name") or "Unnamed Campaign")

def _campaign_templates(fields: Dict[str, Any]) -> List[str]:
    t = _field(fields, "Templates", "Template")
    if isinstance(t, list):
        return [x for x in t if isinstance(x, str)]
    if isinstance(t, str):
        return [t]
    return []

def _campaign_linked_prospect_ids(fields: Dict[str, Any]) -> List[str]:
    p = _field(fields, "Prospects", "Prospect")
    if isinstance(p, list):
        return [x for x in p if isinstance(x, str)]
    if isinstance(p, str):
        return [p]
    return []

def _prospect_phone(p_fields: Dict[str, Any]) -> Optional[str]:
    phone = _field(p_fields, "Phone 1 (from Linked Owner)", "Phone", "Seller Phone Number", "Primary Phone")
    if not phone:
        return None
    return normalize_phone(str(phone)) or str(phone)

def _prospect_first(p_fields: Dict[str, Any]) -> Optional[str]:
    """
    Extracts only the first name from any full-name variant.
    Example: 'John W. Johnson' → 'John'
    """
    name = _field(p_fields, "First", "First Name", "Full Name", "Owner Name", "Owner First", "Owner First Name")
    if not name:
        return None
    name = str(name).strip()
    if " " in name:
        return name.split()[0]
    return name

def _prospect_city(p_fields: Dict[str, Any]) -> Optional[str]:
    return _field(p_fields, "Property City", "City")

def _prospect_address(p_fields: Dict[str, Any]) -> Optional[str]:
    return _field(p_fields, "Address", "Property Address", "Street", "Street Address")

def _prospect_market(p_fields: Dict[str, Any]) -> Optional[str]:
    return _field(p_fields, "Market", "market", "Market Name")

def _prospect_property_id(p_fields: Dict[str, Any]) -> Optional[str]:
    return _field(p_fields, "Property ID", "Property", "PropertyId")

# ------------------------- message templating -------------------------
def _render_message(body: str, pf: Dict[str, Any]) -> str:
    mapping = {
        "First": _prospect_first(pf) or "",
        "Property City": _prospect_city(pf) or "",
        "Address": _prospect_address(pf) or "",
        "City": _prospect_city(pf) or "",
    }
    out = body
    for k, v in mapping.items():
        out = out.replace("{" + k + "}", str(v))
    return out

# ------------------------- template body -------------------------
def _get_template_body(templates_table, template_id: str) -> Optional[str]:
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

# ------------------------- numbers -------------------------
def _numbers_for_market(numbers_tbl, market: Optional[str]) -> List[str]:
    if not numbers_tbl or not market:
        return []
    try:
        recs = numbers_tbl.all(page_size=100)
    except Exception as e:
        log.warning(f"Numbers fetch failed: {e}")
        return []

    out: List[str] = []
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        active = f.get("Active")
        if isinstance(active, bool) and not active:
            continue
        mm = f.get("Market") or f.get("Markets") or f.get("market")
        match = False
        if isinstance(mm, list):
            match = market in mm
        elif isinstance(mm, str):
            match = (mm.strip() == market.strip())
        if not match:
            continue
        number = _field(f, "TextGrid Phone Number", "From Number", "Phone", "Number")
        if number:
            out.append(str(number).strip())
    seen, unique = set(), []
    for n in out:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique

# ------------------------- dedupe check -------------------------
def _has_existing_open_drip(drip_tbl, prospect_id: str, campaign_id: str) -> bool:
    try:
        recs = drip_tbl.all(page_size=100, fields=["Prospect", "Campaign", "Status"])
    except Exception:
        return False
    open_status = {"Queued", "Sending…", "Retry", "Throttled"}
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        p_ids = f.get("Prospect") or []
        c_ids = f.get("Campaign") or []
        if isinstance(p_ids, list) and isinstance(c_ids, list):
            if (prospect_id in p_ids) and (campaign_id in c_ids):
                if str(f.get("Status") or "") in open_status:
                    return True
    return False

# ------------------------- core helpers -------------------------
def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        log.error(f"Airtable create failed [Drip Queue]: {e}")
        return False

# ------------------------- campaign selection -------------------------
ACTIVE_OK = {"Active"}
SCHEDULED = "Scheduled"
BLOCKED = {"Paused", "Completed", "Archived", "Canceled", "Cancelled"}

def _is_campaign_due(fields: Dict[str, Any]) -> bool:
    status = _campaign_status(fields)
    if status in BLOCKED or not status:
        return False
    if status in ACTIVE_OK:
        return True
    if status == SCHEDULED:
        start_dt = _extract_campaign_start(fields)
        if not start_dt:
            return False
        return _now_central() >= start_dt
    return False

# ------------------------- tables -------------------------
def _campaigns_table(): return CONNECTOR.campaigns().table
def _templates_table(): return CONNECTOR.templates().table
def _prospects_table(): return CONNECTOR.prospects().table
def _numbers_table(): return CONNECTOR.numbers().table
def _drip_table(): return CONNECTOR.drip_queue().table

def _fetch_due_campaigns() -> List[Dict[str, Any]]:
    tbl = _campaigns_table()
    try:
        recs = tbl.all(page_size=100)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []
    return [r for r in recs or [] if _is_campaign_due(r.get("fields", {}))]

# ------------------------- queue builder -------------------------
def _queue_one_campaign(campaign: Dict[str, Any], per_camp_limit: Optional[int]) -> int:
    drip_tbl = _drip_table()
    if not drip_tbl: return 0
    c_fields = campaign.get("fields", {}) or {}
    campaign_id = campaign.get("id")
    if not campaign_id: return 0
    campaign_name = _campaign_name(c_fields)
    tmpl_ids = _campaign_templates(c_fields)
    tmpl_tbl = _templates_table()
    tmpl_count = len(tmpl_ids)
    if tmpl_count == 0 or not tmpl_tbl:
        log.warning(f"⚠️ Campaign {campaign_name} has no templates linked.")
        return 0

    p_tbl = _prospects_table()
    prospect_ids = _campaign_linked_prospect_ids(c_fields)
    if not prospect_ids:
        log.info(f"⚠️ Campaign {campaign_name} has 0 linked Prospects; skipping.")
        return 0

    camp_market = str(_field(c_fields, "Market", "market", "Market Name") or "").strip() or None
    numbers_tbl = _numbers_table()
    market_numbers = _numbers_for_market(numbers_tbl, camp_market) if camp_market else []
    num_count = len(market_numbers)

    idx = 0
    queued = 0
    hard_cap = per_camp_limit if per_camp_limit else 1_000_000

    for pid in prospect_ids:
        if queued >= hard_cap: break
        try:
            prec = p_tbl.get(pid)
        except Exception as e:
            log.debug(f"Prospect fetch failed ({pid}): {e}")
            continue
        pf = (prec or {}).get("fields", {}) or {}
        phone = _prospect_phone(pf)
        if not phone: continue
        if _has_existing_open_drip(drip_tbl, pid, campaign_id): continue

        message_template_id = tmpl_ids[idx % tmpl_count]
        body_raw = _get_template_body(tmpl_tbl, message_template_id)
        if not body_raw:
            log.warning(f"⚠️ Template body missing for {message_template_id}; skipping prospect.")
            continue
        message = _render_message(body_raw, pf)

        tg_number = market_numbers[idx % num_count] if num_count else None

        payload: Dict[str, Any] = {
            "Campaign": [campaign_id],
            "Prospect": [pid],
            "Seller Phone Number": phone,
            "TextGrid Phone Number": tg_number,
            "Message": message,
            "Market": _prospect_market(pf),
            "Property ID": _prospect_property_id(pf),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _next_send_for_index(idx, base_gap=7.0, jitter=3.0),
            "Template": [message_template_id],
        }

        if _robust_create_drip(drip_tbl, payload):
            queued += 1
            idx += 1

    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued

# ------------------------- main runner -------------------------
def run_campaigns(limit: int | str = "ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")
    try:
        per_camp_limit = None if str(limit).upper() == "ALL" else int(limit)
    except Exception:
        per_camp_limit = None

    due_campaigns = _fetch_due_campaigns()
    if not due_campaigns:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "processed": 0, "queued": 0, "note": "No due/active campaigns"}

    total_q, processed = 0, 0
    for camp in due_campaigns:
        try:
            q = _queue_one_campaign(camp, per_camp_limit)
            total_q += q
            processed += 1
        except Exception as e:
            log.error(f"Campaign queue failed: {e}")
            log.debug(traceback.format_exc())

    result = {"ok": True, "processed": processed, "queued": total_q, "timestamp": datetime.now(timezone.utc).isoformat()}

    if send_after_queue:
        try:
            from sms.outbound_batcher import send_batch
            send_batch(limit=500)
            result["send_after_queue"] = True
        except Exception as e:
            result["send_after_queue"] = False
            result["send_error"] = str(e)
            log.warning(f"Send after queue failed: {e}")

    log.info(f"✅ Campaign Runner complete → {total_q} queued across {processed} campaigns")
    return result

async def run_campaigns_main(limit: int | str = "ALL", send_after_queue: bool = True):
    import asyncio
    return await asyncio.to_thread(run_campaigns, limit, send_after_queue)

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
