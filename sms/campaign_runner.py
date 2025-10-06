# sms/campaign_runner.py
from __future__ import annotations

import os
import re
import json
import random
import traceback
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, Tuple, List, Optional

from dotenv import load_dotenv
load_dotenv()

from pyairtable import Api

from sms.outbound_batcher import send_batch, format_template
from sms.metrics_tracker import update_metrics
from sms.retry_runner import run_retry  # 🔁 retry handler

# ======================
# Airtable config
# ======================
CAMPAIGNS_TABLE    = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE    = os.getenv("TEMPLATES_TABLE", "Templates")
PROSPECTS_TABLE    = os.getenv("PROSPECTS_TABLE", "Prospects")
DRIP_QUEUE_TABLE   = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")

LEADS_CONVOS_BASE  = os.getenv("LEADS_CONVOS_BASE")
PERFORMANCE_BASE   = os.getenv("PERFORMANCE_BASE")

MAIN_KEY           = os.getenv("AIRTABLE_API_KEY")
REPORTING_KEY      = os.getenv("AIRTABLE_REPORTING_KEY", MAIN_KEY)

# Numbers table (for From Number assignment)
NUMBERS_TABLE      = os.getenv("NUMBERS_TABLE", "Numbers")
NUMBERS_BASE       = os.getenv("NUMBERS_BASE") or os.getenv("CAMPAIGN_CONTROL_BASE") or LEADS_CONVOS_BASE

# Send immediately after queueing? (default True)
RUNNER_SEND_AFTER_QUEUE = (os.getenv("RUNNER_SEND_AFTER_QUEUE", "true").strip().lower() == "true")

# Common phone field variants (covers linked-owner variants too)
PHONE_FIELDS = [
    "phone", "Phone", "Mobile", "Cell",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1", "Phone 2", "Phone 3", "Phone Number", "Primary Phone",
    "Phone 1 (from Linked Owner)", "Phone 2 (from Linked Owner)", "Phone 3 (from Linked Owner)",
]

# ======================
# Helpers
# ======================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _auto_field_map(table, sample_record_id: Optional[str] = None) -> Dict[str, str]:
    """normalized_field_name -> actual Airtable field name for this table."""
    try:
        if sample_record_id:
            rec = table.get(sample_record_id)
        else:
            rows = table.all(max_records=1)
            rec = rows[0] if rows else {"fields": {}}
        keys = list(rec.get("fields", {}).keys())
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap_existing_only(table, payload: Dict, sample_record_id: Optional[str] = None) -> Dict:
    """Include only keys that already exist on the table (prevents 422 UNKNOWN_FIELD_NAME)."""
    amap = _auto_field_map(table, sample_record_id)
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ak = amap.get(_norm(k))
        if ak:
            out[ak] = v
    return out

def _safe_create(table, payload: Dict, sample_record_id: Optional[str] = None) -> Optional[Dict]:
    """
    Create with graceful 422 handling:
    - Try mapped payload if schema known
    - On unknown field(s), drop and retry
    - If table is empty/unknown, optimistic create → conservative → minimal
    """
    try:
        amap = _auto_field_map(table, sample_record_id)
        to_send = {amap.get(_norm(k)): v for k, v in payload.items() if amap.get(_norm(k))} if amap else dict(payload)
        if not to_send:
            to_send = dict(payload)

        for _ in range(6):
            try:
                return table.create(to_send)
            except Exception as e:
                msg = str(e)
                bad = re.findall(r'Unknown field name: "([^"]+)"', msg) or re.findall(r"Unknown field name: '([^']+)'", msg)
                if bad:
                    for b in bad:
                        to_send.pop(b, None)
                    continue
                if not amap:
                    conservative_keys = {
                        "prospect","leads","lead","contact",
                        "campaign","template",
                        "phone","to",
                        "messagepreview","message",
                        "status",
                        "fromnumber",
                        "nextsenddate",
                        "propertyid",
                        "market","address","ownername"
                    }
                    conservative = {k: v for k, v in to_send.items() if _norm(k) in conservative_keys}
                    minimal      = {k: v for k, v in to_send.items() if _norm(k) in {"phone","status"}}
                    for candidate in (conservative, minimal):
                        if candidate:
                            try:
                                return table.create(candidate)
                            except Exception:
                                pass
                raise
    except Exception:
        traceback.print_exc()
    return None

def _get(f: Dict, *names) -> Any:
    """Read a field trying multiple variants; supports normalized match."""
    for n in names:
        if n in f:
            return f[n]
    nf = {_norm(k): k for k in f.keys()}
    for n in names:
        key = nf.get(_norm(n))
        if key:
            return f[key]
    return None

def _digits_only(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    ds = "".join(re.findall(r"\d+", s))
    return ds if len(ds) >= 10 else None

def get_phone(f: Dict[str, Any]) -> Optional[str]:
    for k in PHONE_FIELDS:
        v = f.get(k)
        d = _digits_only(v)
        if d:
            return d
    return None

def _compose_owner_name(pf: Dict[str, Any]) -> Optional[str]:
    full = pf.get("Owner Name")
    if full:
        return full
    fn = pf.get("Owner First Name") or ""
    ln = pf.get("Owner Last Name") or ""
    full = (fn + " " + ln).strip()
    return full or None

def _compose_address(pf: Dict[str, Any]) -> Optional[str]:
    return pf.get("Property Address") or pf.get("Address")

def pick_template(template_ids: Any, templates_table) -> Tuple[Optional[str], Optional[str]]:
    """Pick a random template from linked templates in campaign (list or single id)."""
    if not template_ids:
        return None, None
    tid = random.choice(template_ids) if isinstance(template_ids, list) and template_ids else str(template_ids)
    try:
        tmpl = templates_table.get(tid)
    except Exception:
        return None, None
    if not tmpl:
        return None, None
    msg = _get(tmpl.get("fields", {}), "Message", "message")
    return (msg, tid) if msg else (None, None)

# ---- Numbers helpers (From Number assignment) ----
def _field(f: Dict, *opts):
    for k in opts:
        if k in f:
            return f[k]
    nf = {_norm(k): k for k in f.keys()}
    for k in opts:
        ak = nf.get(_norm(k))
        if ak:
            return f[ak]
    return None

def _as_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, str): return _norm(v) in {"1","true","yes","active","enabled"}
    if isinstance(v, (int, float)): return v != 0
    return False

def _e164(num: str | None) -> str | None:
    if not isinstance(num, str): return None
    digits = "".join(re.findall(r"\d+", num))
    if not digits: return None
    if len(digits) == 10: return "+1" + digits
    if digits.startswith("1") and len(digits) == 11: return "+" + digits
    if num.startswith("+"): return num
    return "+" + digits

def _pick_from_number(market: str | None) -> str | None:
    """Choose an active number for the market with quota headroom; fallback to any active."""
    tbl = get_numbers()
    if not tbl:
        return None
    try:
        rows = tbl.all()
    except Exception:
        traceback.print_exc()
        return None

    def row_ok(r, want_market):
        f = r.get("fields", {})
        mk = _field(f, "Market", "market")
        active = _as_bool(_field(f, "Active", "Enabled", "Status"))
        if want_market and mk != want_market:
            return False
        return active

    # Prefer matching market; else any active
    pool = [r for r in rows if row_ok(r, market)] or [r for r in rows if row_ok(r, None)]
    if not pool:
        return None

    # Pick lowest utilization (Sent Today / Daily Cap)
    def util(r):
        f = r["fields"]
        cap  = _field(f, "Daily Cap", "Daily Limit", "Daily Quota") or 999999
        used = _field(f, "Sent Today", "Used Today", "Today Sent") or 0
        try: cap = int(cap) or 999999
        except: cap = 999999
        try: used = int(used) or 0
        except: used = 0
        return used / cap

    pool.sort(key=util)
    f = pool[0]["fields"]
    did = _field(f, "From Number", "Number", "Phone", "DID", "Twilio Number")
    return _e164(did)

# ======================
# Airtable clients (cached)
# ======================
@lru_cache(maxsize=None)
def _api_main():
    return Api(MAIN_KEY) if (MAIN_KEY and LEADS_CONVOS_BASE) else None

@lru_cache(maxsize=None)
def _api_reporting():
    return Api(REPORTING_KEY) if (REPORTING_KEY and PERFORMANCE_BASE) else None

@lru_cache(maxsize=None)
def get_campaigns():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE) if api else None

@lru_cache(maxsize=None)
def get_templates():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, TEMPLATES_TABLE) if api else None

@lru_cache(maxsize=None)
def get_prospects():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, PROSPECTS_TABLE) if api else None

@lru_cache(maxsize=None)
def get_drip():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, DRIP_QUEUE_TABLE) if api else None

@lru_cache(maxsize=None)
def get_runs():
    api = _api_reporting()
    return api.table(PERFORMANCE_BASE, "Runs/Logs") if api else None

@lru_cache(maxsize=None)
def get_kpis():
    api = _api_reporting()
    return api.table(PERFORMANCE_BASE, "KPIs") if api else None

@lru_cache(maxsize=None)
def get_numbers():
    api = Api(MAIN_KEY) if (MAIN_KEY and NUMBERS_BASE) else None
    return api.table(NUMBERS_BASE, NUMBERS_TABLE) if api else None

# ======================
# Main runner
# ======================
def run_campaigns(
    limit: str | int = 1,
    retry_limit: int = 3,
    send_after_queue: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Execute eligible campaigns using the Prospects table:
      • Filters by Campaigns.Go Live + status (Scheduled/Running) and time window
      • Rotates templates per prospect
      • Throttles queueing (~20 msg/min via next_send_date staggering)
      • Assigns from_number from Numbers table (by Market, quota-aware)
      • Optionally sends + retries after queueing (default True)
      • Updates Campaigns status/metrics, Writes Runs/KPIs, and refreshes global metrics
    """
    if send_after_queue is None:
        send_after_queue = RUNNER_SEND_AFTER_QUEUE

    campaigns = get_campaigns()
    templates = get_templates()
    prospects_tbl = get_prospects()
    drip      = get_drip()
    runs      = get_runs()
    kpis      = get_kpis()

    if not (campaigns and templates and prospects_tbl and drip):
        print("⚠️ CampaignRunner: Missing Airtable tables or API env. Check .env / load_dotenv().")
        return {"ok": False, "processed": 0, "results": [], "errors": ["Missing Airtable tables"]}

    now = utcnow()
    now_iso = now.isoformat()

    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 999_999

    # Fetch all Campaigns and filter to eligible
    try:
        all_campaigns = campaigns.all()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "processed": 0, "results": [], "errors": ["Failed to fetch campaigns"]}

    eligible_campaigns = []
    for c in all_campaigns:
        f = c.get("fields", {})
        status_val = str(_get(f, "status", "Status") or "")
        go_live = _get(f, "Go Live", "go_live")
        if status_val in ("Scheduled", "Running") and bool(go_live):
            eligible_campaigns.append(c)

    processed = 0
    results: List[Dict[str, Any]] = []

    for camp in eligible_campaigns:
        if processed >= int(limit):
            break

        f: Dict[str, Any] = camp.get("fields", {})
        cid   = camp["id"]
        name  = _get(f, "Name", "name") or "Unnamed"
        market_pref = _get(f, "Market", "market")

        # Time window
        start_str = _get(f, "start_time", "Start Time")
        end_str   = _get(f, "end_time", "End Time")
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else None
        end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None

        if start_dt and now < start_dt:
            # keep Scheduled; skip for now
            continue
        if end_dt and now > end_dt:
            update_payload = {"status": "Completed", "last_run_at": now_iso}
            mapped = _remap_existing_only(campaigns, update_payload, sample_record_id=cid)
            if mapped:
                try: campaigns.update(cid, mapped)
                except Exception: traceback.print_exc()
            continue

        # View/Segment (filter Prospects by a view if provided)
        view_name = (_get(f, "View/Segment", "view", "View", "Segment") or "").strip() or None

        # Templates (linked records)
        template_ids = _get(f, "templates", "Templates") or []
        if not template_ids:
            print(f"⚠️ Campaign '{name}' missing templates; skipping")
            continue

        # Fetch Prospects
        try:
            prospect_records = prospects_tbl.all(view=view_name) if view_name else prospects_tbl.all()
        except Exception as e:
            print(f"⚠️ Prospect fetch failed for view={view_name!r}. Retrying without view. Error: {e}")
            try:
                prospect_records = prospects_tbl.all()
                view_name = None
            except Exception:
                traceback.print_exc()
                continue

        total_prospects = len(prospect_records)
        if total_prospects == 0:
            # nothing to do; mark as Completed
            update_payload = {"status": "Completed", "last_run_at": now_iso}
            mapped = _remap_existing_only(campaigns, update_payload, sample_record_id=cid)
            if mapped:
                try: campaigns.update(cid, mapped)
                except Exception: traceback.print_exc()
            results.append({"campaign": name, "queued": 0, "sent": 0, "completed": True, "retries": 0, "view": view_name})
            processed += 1
            continue

        # Update to Running (best-effort)
        try:
            mapped = _remap_existing_only(campaigns, {"status": "Running", "last_run_at": now_iso}, sample_record_id=cid)
            if mapped:
                campaigns.update(cid, mapped)
        except Exception:
            traceback.print_exc()

        queued = 0

        # Queue prospects (throttled)
        for idx, prospect in enumerate(prospect_records):
            pf = prospect.get("fields", {})

            phone = get_phone(pf)
            if not phone:
                continue

            template_text, chosen_tid = pick_template(template_ids, templates)
            if not template_text:
                continue

            personalized_text = format_template(template_text, pf)
            next_send = now + timedelta(seconds=idx * 3)  # ~20 msg/min

            prop_id = _get(pf, "Property ID", "property_id")
            owner   = _compose_owner_name(pf)
            address = _compose_address(pf)

            # Market preference: Campaign > Prospect
            market  = market_pref or _get(pf, "Market", "market")
            from_num = _pick_from_number(market)

            try:
                payload = {
                    # linked record fields
                    "Prospect": [prospect["id"]],
                    "Campaign": [cid],
                    "Template": [chosen_tid],

                    # essentials
                    "phone": phone,
                    "Phone": phone,

                    "message_preview": personalized_text,
                    "Message Preview": personalized_text,

                    "status": "QUEUED",
                    "Status": "QUEUED",

                    # from number
                    "from_number": from_num,
                    "From Number": from_num,

                    # schedule
                    "next_send_date": next_send.isoformat(),
                    "Next Send Date": next_send.isoformat(),

                    # helpful context
                    "Property ID": prop_id,
                    "property_id": prop_id,
                    "Market": market,
                    "market": market,
                    "Address": address,
                    "Property Address": address,
                    "Owner Name": owner,
                }

                _safe_create(drip, payload, sample_record_id=None)
                queued += 1

            except Exception:
                print(f"❌ Failed to queue {phone}")
                traceback.print_exc()

        # --- Optionally send a batch (scope to this campaign id) ---
        batch_result: Dict[str, Any] = {"total_sent": 0}
        retry_result: Dict[str, Any] = {}
        if send_after_queue and queued > 0:
            try:
                batch_result = send_batch(campaign_id=cid, limit=500)
            except Exception:
                traceback.print_exc()
                batch_result = {"total_sent": 0}

            # Retry loop for failed sends
            if batch_result.get("total_sent", 0) < queued:
                for _ in range(int(retry_limit)):
                    try:
                        retry_result = run_retry(limit=100, view="Failed Sends")
                    except Exception:
                        retry_result = {}
                    if (retry_result or {}).get("retried", 0) == 0:
                        break

        # Compute progress
        current_sent = _get(f, "messages_sent", "Messages Sent", "total_sent", "Total Sent") or 0
        sent_now = (batch_result.get("total_sent", 0) or 0) + (retry_result.get("retried", 0) or 0)
        sent_so_far = (current_sent or 0) + sent_now
        completed = (total_prospects > 0) and (sent_so_far >= total_prospects)

        # Update campaign record
        update_payload = {
            "status": "Completed" if completed else "Running",
            "total_sent": sent_so_far,
            "Last Run Result": json.dumps(
                {
                    "Queued": queued,
                    "Sent": batch_result.get("total_sent", 0),
                    "Retries": retry_result.get("retried", 0) if retry_result else 0,
                    "Completed": completed,
                    "View": view_name,
                }
            ),
            "last_run_at": now_iso,
        }
        mapped = _remap_existing_only(campaigns, update_payload, sample_record_id=cid)
        if mapped:
            try:
                campaigns.update(cid, mapped)
            except Exception:
                traceback.print_exc()

        # Logs/KPIs
        if runs:
            try:
                _safe_create(
                    runs,
                    {
                        "Type": "CAMPAIGN_RUN",
                        "Campaign": name,
                        "Processed": float(sent_so_far),
                        "Breakdown": json.dumps({"initial": batch_result, "retries": retry_result}),
                        "Timestamp": now_iso,
                    },
                )
            except Exception:
                traceback.print_exc()

        if kpis:
            try:
                _safe_create(
                    kpis,
                    {
                        "Campaign": name,
                        "Metric": "OUTBOUND_SENT" if send_after_queue else "MESSAGES_QUEUED",
                        "Value": float(sent_so_far if send_after_queue else queued),
                        "Date": now.date().isoformat(),
                    },
                )
            except Exception:
                traceback.print_exc()

        results.append(
            {
                "campaign": name,
                "queued": queued,
                "sent": sent_so_far if send_after_queue else 0,
                "completed": completed if send_after_queue else False,
                "retries": (retry_result or {}).get("retried", 0),
                "view": view_name,
            }
        )
        processed += 1

    # Cross-campaign metrics (best-effort)
    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}