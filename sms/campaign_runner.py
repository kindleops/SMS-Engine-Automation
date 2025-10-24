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

# UI icon used in Drip Queue
STATUS_ICON = {
    "QUEUED": "⏳",
}

# ------------------------- time helpers -------------------------
def _now_central() -> datetime:
    return datetime.now(QUIET_TZ)

def _ct_iso_naive(dt: datetime) -> str:
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _next_send_for_index(idx: int, base_gap: float = 7.0, jitter: float = 3.0) -> str:
    """
    Spread each queued message apart.
    idx: 0-based index in the queue.
    base_gap: average seconds between messages.
    jitter: random +/- to avoid uniform steps.
    """
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
    """
    Accepts common date/time field names and returns a tz-aware Central datetime if present.
    """
    dt_raw = _field(fields,
                    "Start Time", "Start", "Start At", "Go Live At", "Go Live",
                    "Start Date", "Start Datetime")
    if not dt_raw:
        return None
    try:
        # Allow both naive and tz strings from Airtable
        if isinstance(dt_raw, str):
            dtp = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
        else:
            dtp = dt_raw  # already a datetime
        if dtp.tzinfo is None:
            # treat as central local if naive
            return dtp.replace(tzinfo=QUIET_TZ)
        return dtp.astimezone(QUIET_TZ)
    except Exception:
        return None

def _campaign_status(fields: Dict[str, Any]) -> str:
    # Single select typically returns string
    return str(_field(fields, "Status") or "").strip()

def _campaign_name(fields: Dict[str, Any]) -> str:
    return str(_field(fields, "Name", "Campaign Name") or "Unnamed Campaign")

def _campaign_templates(fields: Dict[str, Any]) -> List[str]:
    # Linked templates column; accept "Templates" or "Template"
    t = _field(fields, "Templates", "Template")
    if isinstance(t, list):
        return [x for x in t if isinstance(x, str)]
    if isinstance(t, str):
        return [t]
    return []

def _campaign_linked_prospect_ids(fields: Dict[str, Any]) -> List[str]:
    # Campaigns -> Prospects (linked field). Accept "Prospects" or "Prospect".
    p = _field(fields, "Prospects", "Prospect")
    if isinstance(p, list):
        return [x for x in p if isinstance(x, str)]
    if isinstance(p, str):
        return [p]
    return []

def _prospect_phone(p_fields: Dict[str, Any]) -> Optional[str]:
    # Primary phone precedence; add your aliases here
    phone = _field(p_fields, "Phone 1 (from Linked Owner)", "Phone", "Primary Phone", "Mobile", "Seller Phone Number")
    if not phone:
        return None
    return normalize_phone(str(phone)) or str(phone)

def _prospect_first(p_fields: Dict[str, Any]) -> Optional[str]:
    return _field(p_fields, "First", "First Name", "FirstName", "Owner First", "Owner First Name")

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
    """
    Replace placeholders of the form {First}, {Property City}, {Address} etc.
    Missing values are replaced with empty string.
    """
    mapping = {
        "First": _prospect_first(pf) or "",
        "Property City": _prospect_city(pf) or "",
        "Address": _prospect_address(pf) or "",
        # Common alternates (kept for safety)
        "City": _prospect_city(pf) or "",
        "PropertyID": _prospect_property_id(pf) or "",
        "Property Id": _prospect_property_id(pf) or "",
        "Property": _prospect_property_id(pf) or "",
    }

    out = body
    for k, v in mapping.items():
        out = out.replace("{" + k + "}", str(v))
    return out

# ------------------------- template body -------------------------
def _get_template_body(templates_table, template_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (body, template_id_if_link_ok)
    """
    try:
        rec = templates_table.get(template_id)
    except Exception as e:
        log.warning(f"Template read failed: {e}")
        return None, None
    f = (rec or {}).get("fields", {}) or {}
    for key in ("Body", "Message", "Text", "Template", "Content"):
        v = f.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip(), template_id
    return None, template_id

# ------------------------- numbers (TextGrid) -------------------------
def _numbers_for_market(numbers_tbl, market: Optional[str]) -> List[str]:
    """
    Return a list of TextGrid numbers for a given Market (single select/text match).
    Accepts common field names:
      - "Market" (single select)
      - "Markets" (multi select)
      - "Active" (boolean) – if present, we filter to True
      - number field names: "TextGrid Phone Number", "From Number", "Phone", "Number"
    """
    if not numbers_tbl or not market:
        return []

    # fetch broad; filter in python to avoid formula breakage on commas
    try:
        recs = numbers_tbl.all(page_size=100)
    except Exception as e:
        log.warning(f"Numbers fetch failed: {e}")
        return []

    out: List[str] = []
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        # enforce Active if present
        active = f.get("Active")
        if isinstance(active, bool) and not active:
            continue

        # market match (single- or multi-select or plain text)
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

    # dedupe while preserving order
    seen = set()
    unique = []
    for n in out:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique

# ------------------------- dedupe check -------------------------
def _has_existing_open_drip(drip_tbl, prospect_id: str, campaign_id: str) -> bool:
    """
    Returns True if Drip Queue already has a row for this (Prospect, Campaign)
    in non-terminal states (Queued/Sending…/Retry/Throttled). We allow "Sent"/"Failed"
    to be considered terminal and enqueue again only if explicitly desired later.
    """
    # Filter by exact linked IDs; Airtable formula needs RECORD_ID() in link arrays.
    # Safer approach: fetch small page and check client-side.
    try:
        recs = drip_tbl.all(page_size=100, fields=["Prospect", "Campaign", "Status"])
    except Exception:
        return False
    wanted = {"Queued", "Sending…", "Retry", "Throttled"}
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        p_ids = f.get("Prospect") or []
        c_ids = f.get("Campaign") or []
        if isinstance(p_ids, list) and isinstance(c_ids, list):
            if (prospect_id in p_ids) and (campaign_id in c_ids):
                if str(f.get("Status") or "") in wanted:
                    return True
    return False

# ------------------------- robust create (Market retry) -------------------------
def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    """
    Attempt to create Drip Queue row. If Airtable rejects Market due to select options,
    retry once without "Market".
    """
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e1:
        msg = str(e1)
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in msg or "Insufficient permissions to create new select option" in msg:
            log.warning(f"⚠️ Market select rejected ({payload.get('Market')}); retrying without Market.")
            payload2 = dict(payload)
            payload2.pop("Market", None)
            try:
                drip_tbl.create(payload2)
                return True
            except Exception as e2:
                log.error(f"Airtable create failed [Drip Queue] after Market retry: {e2}")
                return False
        else:
            log.error(f"Airtable create failed [Drip Queue]: {e1}")
            return False

# ------------------------- fetch active/due campaigns -------------------------
ACTIVE_OK = {"Active"}
SCHEDULED = "Scheduled"
BLOCKED = {"Paused", "Completed", "Archived", "Canceled", "Cancelled"}

def _campaigns_table():
    try:
        return CONNECTOR.campaigns().table
    except Exception as e:
        log.error(f"❌ Campaigns table fetch failed: {e}")
        return None

def _templates_table():
    try:
        return CONNECTOR.templates().table
    except Exception:
        return None

def _prospects_table():
    try:
        return CONNECTOR.prospects().table
    except Exception:
        return None

def _numbers_table():
    try:
        return CONNECTOR.numbers().table  # your CONNECTOR should expose Numbers
    except Exception:
        return None

def _drip_table():
    try:
        return CONNECTOR.drip_queue().table
    except Exception:
        return None

def _is_campaign_due(fields: Dict[str, Any]) -> bool:
    status = _campaign_status(fields)
    if status in BLOCKED or not status:
        return False
    if status in ACTIVE_OK:
        return True
    if status == SCHEDULED:
        start_dt = _extract_campaign_start(fields)
        if start_dt is None:
            # If no start time, treat as not yet due (safe)
            return False
        return _now_central() >= start_dt
    return False

def _fetch_due_campaigns() -> List[Dict[str, Any]]:
    tbl = _campaigns_table()
    if not tbl:
        return []
    # Pull a reasonable page and filter in python (simpler than juggling formula variants)
    try:
        recs = tbl.all(page_size=200)
    except Exception as e:
        log.error(f"❌ Failed to fetch campaigns: {e}")
        return []

    due: List[Dict[str, Any]] = []
    for r in recs or []:
        f = (r or {}).get("fields", {}) or {}
        if _is_campaign_due(f):
            due.append(r)
    return due

# ------------------------- core queue builder -------------------------
def _queue_one_campaign(campaign: Dict[str, Any], per_camp_limit: Optional[int]) -> int:
    drip_tbl = _drip_table()
    if not drip_tbl:
        log.error("Drip Queue table not available.")
        return 0

    c_fields = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    if not campaign_id:
        return 0

    campaign_name = _campaign_name(c_fields)
    tmpl_ids = _campaign_templates(c_fields)
    tmpl_id = tmpl_ids[0] if tmpl_ids else None

    tmpl_tbl = _templates_table()
    body_raw, tmpl_id_ok = (None, None)
    if tmpl_id and tmpl_tbl:
        body_raw, tmpl_id_ok = _get_template_body(tmpl_tbl, tmpl_id)
    if not body_raw:
        log.warning(f"⚠️ Campaign {campaign_name} has no usable Template body; skipping.")
        return 0

    p_tbl = _prospects_table()
    if not p_tbl:
        log.error("Prospects table not available.")
        return 0

    prospect_ids = _campaign_linked_prospect_ids(c_fields)
    if not prospect_ids:
        log.info(f"⚠️ Campaign {campaign_name} has 0 linked Prospects; skipping.")
        return 0

    # Numbers by campaign market (used only to pick TextGrid numbers)
    camp_market = str(_field(c_fields, "Market", "market", "Market Name") or "").strip() or None
    numbers_tbl = _numbers_table()
    market_numbers = _numbers_for_market(numbers_tbl, camp_market) if camp_market else []
    # Round-robin cursor (per campaign, per run)
    num_count = len(market_numbers)

    idx = 0
    queued = 0
    hard_cap = per_camp_limit if (per_camp_limit and per_camp_limit > 0) else 1_000_000

    for pid in prospect_ids:
        if queued >= hard_cap:
            break
        try:
            prec = p_tbl.get(pid)
        except Exception as e:
            log.debug(f"Prospect fetch failed ({pid}): {e}")
            continue
        pf = (prec or {}).get("fields", {}) or {}

        phone = _prospect_phone(pf)
        if not phone:
            continue

        # Dedup: if there’s already an open drip for this Prospect+Campaign, skip
        if _has_existing_open_drip(drip_tbl, pid, campaign_id):
            continue

        # Message render w/ placeholders
        message = _render_message(body_raw, pf)

        # Choose market for record display (not required to trigger)
        p_market = _prospect_market(pf)
        market_for_display = p_market or camp_market  # prefer Prospect's Market

        # TextGrid number round-robin from Numbers by Campaign Market
        tg_number = None
        if num_count > 0:
            tg_number = market_numbers[idx % num_count]

        payload: Dict[str, Any] = {
            "Campaign": [campaign_id],
            "Prospect": [pid],  # IMPORTANT: singular link only; "Prospects" does not exist in Drip Queue
            "Seller Phone Number": phone,
            "TextGrid Phone Number": tg_number,  # may be None; outbound batcher can backfill if you allow
            "Message": message,
            "Market": market_for_display,        # retry logic removes this if Airtable rejects the option
            "Property ID": _prospect_property_id(pf),
            "Status": DripStatus.QUEUED.value,
            "UI": STATUS_ICON["QUEUED"],
            "Next Send Date": _next_send_for_index(idx, base_gap=7.0, jitter=3.0),
        }

        # Link Template if your Drip Queue has that field
        if tmpl_id_ok:
            payload["Template"] = [tmpl_id_ok]

        if _robust_create_drip(drip_tbl, payload):
            queued += 1
            idx += 1

    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued

# ------------------------- public API -------------------------
def run_campaigns(limit: int | str = "ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")
    try:
        per_camp_limit = None if (isinstance(limit, str) and str(limit).upper() == "ALL") else int(limit)
    except Exception:
        per_camp_limit = None

    due_campaigns = _fetch_due_campaigns()
    if not due_campaigns:
        log.info("⚠️ No due/active campaigns found.")
        return {"ok": True, "processed": 0, "queued": 0, "note": "No due/active campaigns"}

    total_q = 0
    processed = 0
    for camp in due_campaigns:
        try:
            q = _queue_one_campaign(camp, per_camp_limit)
            total_q += q
            processed += 1
        except Exception as e:
            log.error(f"Campaign queue failed: {e}")
            log.debug(traceback.format_exc())

    result = {
        "ok": True,
        "processed": processed,
        "queued": total_q,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if send_after_queue:
        try:
            from sms.outbound_batcher import send_batch
            # Let your batcher enforce quiet hours & per-minute caps
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
