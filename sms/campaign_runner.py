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
CAMPAIGNS_TABLE   = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE   = os.getenv("TEMPLATES_TABLE", "Templates")
DRIP_QUEUE_TABLE  = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")
PROSPECTS_TABLE   = os.getenv("PROSPECTS_TABLE", "Prospects")

LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
PERFORMANCE_BASE  = os.getenv("PERFORMANCE_BASE")

MAIN_KEY          = os.getenv("AIRTABLE_API_KEY")
REPORTING_KEY     = os.getenv("AIRTABLE_REPORTING_KEY", MAIN_KEY)

# Toggle immediate sending after queuing (default False → queue-only)
RUNNER_SEND_AFTER_QUEUE = (os.getenv("RUNNER_SEND_AFTER_QUEUE", "false").lower() == "true")

# Phone field variants commonly seen in your Prospects schema
PHONE_FIELDS = [
    "phone", "Phone", "Mobile", "Cell",
    "Owner Phone", "Owner Phone 1", "Owner Phone 2",
    "Phone 1", "Phone 2", "Phone 3",
    "Primary Phone", "Phone Number",
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
    - Try full payload
    - On UNKNOWN_FIELD_NAME, drop just that field and retry (up to a few passes)
    - If table has no discoverable fields yet, try optimistic then conservative/minimal fallbacks
    """
    if not (table and payload):
        return None
    try:
        amap = _auto_field_map(table, sample_record_id)
        if amap:
            to_send = {amap.get(_norm(k)): v for k, v in payload.items() if amap.get(_norm(k))}
            if not to_send:
                to_send = dict(payload)  # optimistic
        else:
            to_send = dict(payload)

        for _ in range(6):
            try:
                return table.create(to_send)
            except Exception as e:
                msg = str(e)
                bad = re.findall(r'Unknown field name: "([^"]+)"', msg) or \
                      re.findall(r"Unknown field name: '([^']+)'", msg)
                if bad:
                    for b in bad:
                        to_send.pop(b, None)
                    continue

                # if first optimistic failed and we have no amap, try conservative/minimal
                if not amap:
                    conservative_keys = {
                        "prospect", "leads", "lead", "contact",
                        "campaign", "template",
                        "phone", "to",
                        "messagepreview", "message",
                        "status",
                        "fromnumber",
                        "nextsenddate",
                        "propertyid",
                        "market", "address", "ownername",
                    }
                    conservative = {k: v for k, v in to_send.items() if _norm(k) in conservative_keys}
                    minimal      = {k: v for k, v in to_send.items() if _norm(k) in {"phone", "status"}}
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


def _safe_update(table, rec_id: str, payload: Dict, sample_record_id: Optional[str] = None) -> Optional[Dict]:
    """Update only fields that exist."""
    if not (table and rec_id and payload):
        return None
    try:
        to_send = _remap_existing_only(table, payload, sample_record_id=rec_id if sample_record_id is None else sample_record_id)
        if to_send:
            return table.update(rec_id, to_send)
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


# ======================
# Main runner
# ======================
def run_campaigns(
    limit: str | int = 1,
    retry_limit: int = 3,
    send_after_queue: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Execute eligible campaigns from the Prospects table:
      • Prospects-only (single source of truth)
      • Rotates templates
      • Throttles outbound to ~20 msgs/minute
      • Optionally sends + retries after queueing
      • Creates Drip Queue rows with linked Campaign/Prospect/Template + Address/Market/Owner
      • Updates Campaign status + counters
      • Best-effort cross-campaign metrics refresh (update_metrics)
    """
    if send_after_queue is None:
        send_after_queue = RUNNER_SEND_AFTER_QUEUE

    campaigns = get_campaigns()
    templates = get_templates()
    drip      = get_drip()
    prospects = get_prospects()
    runs      = get_runs()
    kpis      = get_kpis()

    if not (campaigns and templates and drip and prospects):
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

    # Accept Scheduled/Running AND must have Go Live checked
    eligible_campaigns = []
    for c in all_campaigns:
        f = c.get("fields", {})
        status_val = str(_get(f, "status", "Status") or "")
        go_live = bool(_get(f, "Go Live", "go_live"))
        if go_live and status_val in ("Scheduled", "Running"):
            eligible_campaigns.append(c)

    processed = 0
    results: List[Dict[str, Any]] = []

    for camp in eligible_campaigns:
        if processed >= int(limit):
            break

        cid = camp["id"]
        f: Dict[str, Any] = camp.get("fields", {})
        name = _get(f, "Name", "name") or "Unnamed"

        try:
            # Skip paused/cancelled if they slipped through
            if _get(f, "status", "Status") in ("Paused", "Cancelled"):
                continue

            # Time window
            start_str = _get(f, "Start Time", "start_time")
            end_str   = _get(f, "End Time", "end_time")
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else None
            end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None

            if start_dt and now < start_dt:
                continue
            if end_dt and now > end_dt:
                _safe_update(campaigns, cid, {"status": "Completed", "last_run_at": now_iso})
                continue

            # Templates (linked records)
            template_ids = _get(f, "templates", "Templates") or []
            if not template_ids:
                print(f"⚠️ Campaign '{name}' missing templates; skipping")
                continue

            # Prospects source parsing (Prospects-only)
            view = (_get(f, "View/Segment", "view", "View", "Segment") or "").strip() or None

            # Fetch prospects; if the token lacks permission for the view, try without the view as a fallback
            try:
                prospect_records = prospects.all(view=view) if view else prospects.all()
            except Exception as e:
                print(f"⚠️ Prospects fetch failed (view={view!r}). {e}")
                try:
                    prospect_records = prospects.all()
                    view = None
                except Exception:
                    traceback.print_exc()
                    continue

            total_prospects = len(prospect_records)
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

                # Resolve market/address/owner from campaign first, then prospect
                market = _get(f, "Market", "market") or _get(pf, "Market", "market")
                address = _get(pf, "Property Address", "Address")
                owner = _get(pf, "Owner Name") or " ".join(
                    [x for x in [pf.get("Owner First Name"), pf.get("Owner Last Name")] if x]
                ).strip() or None
                prop_id = _get(pf, "Property ID", "property_id")

                # Include snake + Title variants; _safe_create will prune to what's present
                payload = {
                    # linked record fields
                    "Prospect": [prospect["id"]],
                    "Campaign": [cid],
                    "Template": [chosen_tid],

                    # phone (both)
                    "phone": phone,
                    "Phone": phone,

                    # message preview (both)
                    "message_preview": personalized_text,
                    "Message Preview": personalized_text,

                    # status (both)
                    "status": "QUEUED",
                    "Status": "QUEUED",

                    # from number (both)
                    "from_number": None,
                    "From Number": None,

                    # schedule (both)
                    "next_send_date": next_send.isoformat(),
                    "Next Send Date": next_send.isoformat(),

                    # property id (both)
                    "Property ID": prop_id,
                    "property_id": prop_id,

                    # context (pruned if fields don't exist)
                    "Market": market,
                    "market": market,
                    "Address": address,
                    "Property Address": address,
                    "Owner Name": owner,
                }

                _safe_create(drip, payload)
                queued += 1

            # --- Optionally send a batch (scope to this campaign id) ---
            batch_result: Dict[str, Any] = {"total_sent": 0}
            retry_result: Dict[str, Any] = {}
            if send_after_queue and queued > 0:
                try:
                    batch_result = send_batch(campaign_id=cid, limit=500) or {"total_sent": 0}
                except Exception:
                    traceback.print_exc()
                    batch_result = {"total_sent": 0}

                # Retry loop
                if batch_result.get("total_sent", 0) < queued:
                    for _ in range(int(retry_limit)):
                        try:
                            rr = run_retry(limit=100, view="Failed Sends") or {}
                        except Exception:
                            rr = {}
                        retry_result = rr
                        if rr.get("retried", 0) == 0:
                            break

            # Compute progress
            current_sent = _get(f, "messages_sent", "total_sent", "Messages Sent", "Total Sent") or 0
            sent_so_far = (current_sent or 0) + (batch_result.get("total_sent", 0) or 0) + (retry_result.get("retried", 0) or 0)
            completed = (total_prospects > 0) and (sent_so_far >= total_prospects)

            # Update campaign (mirror messages_sent and total_sent)
            update_payload = {
                "status": "Completed" if completed else "Running",
                "total_sent": sent_so_far,
                "messages_sent": sent_so_far,
                "Last Run Result": json.dumps(
                    {
                        "Queued": queued,
                        "Sent": batch_result.get("total_sent", 0),
                        "Retries": retry_result.get("retried", 0) if retry_result else 0,
                        "Completed": completed,
                        "Table": PROSPECTS_TABLE,
                        "View": view,
                    }
                ),
                "last_run_at": now_iso,
            }
            _safe_update(campaigns, cid, update_payload)

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
                    "table": PROSPECTS_TABLE,
                    "view": view,
                }
            )
            processed += 1

        except Exception as e:
            traceback.print_exc()
            _safe_update(campaigns, cid, {"last_error": str(e)[:500], "last_run_at": now_iso})

    # Cross-campaign metrics (best-effort)
    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}