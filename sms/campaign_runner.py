# sms/campaign_runner.py
from __future__ import annotations
import traceback
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from sms.runtime import get_logger, normalize_phone
from sms.datastore import CONNECTOR
from sms.airtable_schema import DripStatus

log = get_logger("campaign_runner")
CT = ZoneInfo("America/Chicago")

# UI icons (stable)
STATUS_ICON = {
    "QUEUED": "⏳",
    "Sending…": "🔄",
    "Sent": "✅",
    "Retry": "🔁",
    "Throttled": "🕒",
    "Failed": "❌",
    "DNC": "⛔",
}

# ------------------------- utilities -------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ct_future_iso_naive(min_s: int = 2, max_s: int = 12) -> str:
    dt = datetime.now(CT) + timedelta(seconds=random.randint(min_s, max_s))
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def _sanitize_market(s: Optional[str]) -> str:
    """
    Trim spaces and convert NBSP to normal spaces so Airtable single-select
    will match existing options like 'Minneapolis, MN'.
    """
    if not s:
        return ""
    return str(s).replace("\u00A0", " ").strip()

class _SafeMap(dict):
    """Format-safe mapping: missing keys return '{key}' (keeps placeholders)."""
    def __missing__(self, key):
        return "{" + key + "}"

def _fmt_message(body: str, fields: Dict[str, Any]) -> str:
    """
    Fill placeholders like {First}, {Property City}, {Address}.
    We pass the Airtable fields directly so the placeholder names
    can match your column names verbatim.
    """
    try:
        return body.format_map(_SafeMap(fields))
    except Exception:
        # never crash on formatting—just return as-is
        return body

# ------------------------- fetch helpers -------------------------

def _campaigns_table():
    try:
        return CONNECTOR.campaigns().table
    except Exception as e:
        log.error(f"❌ Campaigns table handle failed: {e}")
        return None

def _prospects_table():
    try:
        return CONNECTOR.prospects().table
    except Exception as e:
        log.error(f"❌ Prospects table handle failed: {e}")
        return None

def _templates_table():
    try:
        return CONNECTOR.templates().table
    except Exception as e:
        log.error(f"❌ Templates table handle failed: {e}")
        return None

def _dripq_table():
    try:
        return CONNECTOR.drip_queue().table
    except Exception as e:
        log.error(f"❌ Drip Queue table handle failed: {e}")
        return None

def _numbers_table():
    try:
        return CONNECTOR.numbers().table
    except Exception as e:
        log.error(f"❌ Numbers table handle failed: {e}")
        return None

# ------------------------- template / number -------------------------

def _get_template_body(template_id: Optional[str]) -> Optional[str]:
    if not template_id:
        return None
    tbl = _templates_table()
    if not tbl:
        return None
    try:
        rec = tbl.get(template_id)
        f = (rec or {}).get("fields", {}) or {}
        for key in ("Body", "Message", "Text", "Template", "Content"):
            v = f.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception as e:
        log.warning(f"Template read failed ({template_id}): {e}")
    return None

def _pick_textgrid_number(campaign_market: Optional[str]) -> Optional[str]:
    """
    Choose the first 'Active' number from Numbers where Market == campaign_market.
    (Market on campaign controls from-number selection.)
    """
    market = _sanitize_market(campaign_market)
    if not market:
        return None
    tbl = _numbers_table()
    if not tbl:
        return None
    try:
        # Exact match on Market single-select, Status='Active'
        rows = tbl.all(formula=f"AND({{Market}}='{market}', {{Status}}='Active')")
        if rows:
            return (rows[0].get("fields", {}) or {}).get("Number")
    except Exception as e:
        log.warning(f"TextGrid number lookup failed for market '{market}': {e}")
    return None

# ------------------------- campaign selection -------------------------

def _fetch_due_campaigns() -> List[Dict[str, Any]]:
    """
    Return campaigns that are either:
      - Status = 'Active'
      - OR Status = 'Scheduled' AND Start Time <= now (UTC)
    Never include 'Paused'.
    """
    tbl = _campaigns_table()
    if not tbl:
        return []
    now_iso = _now_utc_iso()
    formula = (
        "AND("
        "  OR({Status}='Active', AND({Status}='Scheduled', OR({Start Time}=BLANK(), {Start Time}<='" + now_iso + "'))),"
        "  {Status}!='Paused'"
        ")"
    )
    try:
        return tbl.all(formula=formula)
    except Exception as e:
        log.error(f"❌ Failed to fetch due campaigns: {e}")
        return []

def _activate_if_scheduled(campaign: Dict[str, Any]) -> None:
    """Flip Scheduled → Active once it's due."""
    try:
        f = (campaign or {}).get("fields", {}) or {}
        if f.get("Status") == "Scheduled":
            tbl = _campaigns_table()
            if tbl:
                tbl.update(campaign["id"], {"Status": "Active"})
                log.info(f"🔓 Campaign activated (Scheduled → Active): {f.get('Name') or campaign.get('id')}")
    except Exception as e:
        log.warning(f"Could not activate campaign: {e}")

# ------------------------- queue dedupe -------------------------

def _already_queued_prospect_ids(drip_tbl, campaign_id: str) -> Set[str]:
    """
    Build a set of Prospect record IDs already present in Drip Queue for this campaign,
    in any status (Queued/Sending/Sent/etc). Prevents duplicates.
    """
    ids: Set[str] = set()
    try:
        # Filter by linked Campaign equality; Airtable supports matching a linked record to its ID.
        existing = drip_tbl.all(formula=f"{{Campaign}}='{campaign_id}'")
        for r in existing or []:
            pf = (r or {}).get("fields", {}) or {}
            # Prospect link could be singular or plural in your base; handle both.
            for key in ("Prospect", "Prospects"):
                link = pf.get(key)
                if isinstance(link, list) and link:
                    for pid in link:
                        if isinstance(pid, str):
                            ids.add(pid)
                elif isinstance(link, str):
                    ids.add(link)
    except Exception as e:
        log.warning(f"Existing queue read failed (campaign {campaign_id}): {e}")
    return ids

# ------------------------- robust create with retries -------------------------

def _robust_create_drip(drip_tbl, payload: Dict[str, Any]) -> bool:
    """
    Create a Drip Queue row, retrying if Airtable rejects single-select 'Market'
    or if the 'Template' field doesn't exist in your base.
    """
    # try with full payload
    try:
        drip_tbl.create(payload)
        return True
    except Exception as e:
        s = str(e)
        # If Market select option is invalid / insufficient permissions → drop Market and retry
        if "INVALID_MULTIPLE_CHOICE_OPTIONS" in s or "INVALID_SINGLE_SELECT_OPTIONS" in s:
            log.warning(f"⚠️ Market select rejected ({payload.get('Market')}); retrying without Market.")
            payload2 = dict(payload)
            payload2.pop("Market", None)
            try:
                drip_tbl.create(payload2)
                return True
            except Exception as e2:
                s2 = str(e2)
                # If Template field is unknown, drop it and retry again
                if "UNKNOWN_FIELD_NAME" in s2 or "NOT_FOUND" in s2:
                    payload3 = dict(payload2)
                    payload3.pop("Template", None)
                    try:
                        drip_tbl.create(payload3)
                        return True
                    except Exception as e3:
                        log.error(f"Airtable create failed [Drip Queue] after retries: {e3}")
                        return False
                log.error(f"Airtable create failed [Drip Queue] after Market retry: {e2}")
                return False

        # If Template field is unknown, drop it and retry (even if Market was fine)
        if "UNKNOWN_FIELD_NAME" in s or "NOT_FOUND" in s:
            payload2 = dict(payload)
            payload2.pop("Template", None)
            try:
                drip_tbl.create(payload2)
                return True
            except Exception as e2:
                log.error(f"Airtable create failed [Drip Queue] after Template retry: {e2}")
                return False

        log.error(f"Airtable create failed [Drip Queue]: {e}")
        return False

# ------------------------- core queue builder -------------------------

def _build_campaign_queue(campaign: Dict[str, Any], limit: int) -> int:
    """
    For a single campaign:
      • load linked Prospects
      • resolve template + format placeholders
      • pick TextGrid number by Campaign.Market
      • set Drip Queue Market from Prospect.Market (view-only)
      • create Drip Queue rows, deduping already queued Prospects
    """
    drip_tbl = _dripq_table()
    pros_tbl = _prospects_table()
    if not (drip_tbl and pros_tbl):
        log.error("❌ Required tables missing (Drip Queue / Prospects).")
        return 0

    cf = (campaign or {}).get("fields", {}) or {}
    campaign_id = campaign.get("id")
    campaign_name = cf.get("Name") or cf.get("Campaign Name") or f"Campaign {campaign_id}"

    # resolve template (first linked)
    tmpl_id = None
    tlinks = cf.get("Templates") or cf.get("Template")
    if isinstance(tlinks, list) and tlinks:
        tmpl_id = tlinks[0]
    elif isinstance(tlinks, str):
        tmpl_id = tlinks
    body = _get_template_body(tmpl_id)
    if not body:
        log.warning(f"⚠️ Campaign '{campaign_name}' has no usable template body; skipping.")
        return 0

    # resolve TextGrid Number by Campaign.Market (controls from-number)
    tg_number = _pick_textgrid_number(cf.get("Market") or cf.get("market") or cf.get("Market Name"))

    # load linked prospects ONLY (no market fallback)
    linked = cf.get("Prospects") or cf.get("Prospect") or []
    if not linked:
        log.info(f"⚠️ Campaign '{campaign_name}' has 0 linked prospects; skipping.")
        return 0

    # dedupe set for this campaign
    already = _already_queued_prospect_ids(drip_tbl, campaign_id)

    queued = 0
    to_process = linked if limit is None else linked[: max(1, int(limit))]

    for pid in to_process:
        try:
            if pid in already:
                continue
            prec = pros_tbl.get(pid)
            pf = (prec or {}).get("fields", {}) or {}

            # phone normalization
            phone = (
                pf.get("Phone 1 (from Linked Owner)")
                or pf.get("Phone")
                or pf.get("Primary Phone")
                or pf.get("Mobile")
            )
            if not phone:
                continue
            phone_norm = normalize_phone(str(phone)) or str(phone)

            # format message with placeholders (never crashes)
            message = _fmt_message(body, pf)

            # view-only market from Prospect
            prospect_market = _sanitize_market(
                pf.get("Market") or pf.get("market") or pf.get("Market Name")
            )

            payload: Dict[str, Any] = {
                "Campaign": [campaign_id] if campaign_id else None,
                # Try both singular/plural; Airtable will accept whichever exists and ignore the other on retry
                "Prospect": [pid],
                "Seller Phone Number": phone_norm,
                "TextGrid Phone Number": tg_number,  # may be None; that's OK—outbound can backfill if needed
                "Message": message,
                "Market": prospect_market,           # may be dropped on retry if select option mismatch
                "Property ID": pf.get("Property ID") or pf.get("Property") or pf.get("PropertyId"),
                "Status": DripStatus.QUEUED.value,   # exact status token your system expects
                "UI": STATUS_ICON["QUEUED"],         # ⏳
                "Next Send Date": _ct_future_iso_naive(2, 12),
                "Template": [tmpl_id] if tmpl_id else None,  # if your base has this link field
            }

            if _robust_create_drip(drip_tbl, payload):
                already.add(pid)
                queued += 1
            else:
                log.error(f"Queue insert failed for {campaign_name} (prospect {pid}).")
        except Exception as e:
            log.error(f"Queue insert failed for {campaign_name} (prospect {pid}): {e}")
            log.debug(traceback.format_exc())

    log.info(f"✅ Queued {queued} messages for campaign → {campaign_name}")
    return queued

# ------------------------- public API -------------------------

def run_campaigns(limit: int | str = "ALL", send_after_queue: bool = True) -> Dict[str, Any]:
    """
    Entrypoint for both CLI and FastAPI endpoint.
    """
    try:
        log.info(f"🚀 Campaign Runner — limit={limit}, send_after_queue={send_after_queue}")
        due = _fetch_due_campaigns()
        if not due:
            log.info("⚠️ No due campaigns found.")
            return {"ok": True, "processed": 0, "queued": 0, "note": "No due campaigns."}

        # flip Scheduled → Active where needed
        for c in due:
            _activate_if_scheduled(c)

        total_q = 0
        processed = 0
        per_camp_limit = None if str(limit).upper() in ("ALL", "", "UNLIMITED", "NONE") else int(limit)

        for camp in due:
            q = _build_campaign_queue(camp, per_camp_limit if per_camp_limit else 1000000)
            total_q += q
            processed += 1

        result = {
            "ok": True,
            "processed": processed,
            "queued": total_q,
            "timestamp": _now_utc_iso(),
        }

        if send_after_queue:
            try:
                from sms.outbound_batcher import send_batch
                # Let outbound batcher own throttling/quiet-hour logic.
                send_batch(limit=500)
                result["send_after_queue"] = True
            except Exception as e:
                result["send_after_queue"] = False
                result["send_error"] = str(e)
                log.warning(f"Send-after-queue failed: {e}")

        log.info(f"✅ Campaign Runner complete → queued={total_q}, campaigns={processed}")
        return result

    except Exception as e:
        err = f"fatal campaign runner error: {e}"
        log.error(err)
        log.debug(traceback.format_exc())
        return {"ok": False, "error": err}

async def run_campaigns_main(limit: int | str = "ALL", send_after_queue: bool = True):
    import asyncio
    return await asyncio.to_thread(run_campaigns, limit, send_after_queue)

if __name__ == "__main__":
    print(run_campaigns("ALL", True))
