# sms/campaign_runner.py
import os
import traceback
import json
import random
from datetime import datetime, timezone, timedelta
from functools import lru_cache
import re

from dotenv import load_dotenv
load_dotenv()  # ensure .env is loaded for this process

from pyairtable import Api

from sms.outbound_batcher import send_batch, format_template
from sms.metrics_tracker import update_metrics
from sms.retry_runner import run_retry  # 🔁 retry handler

# --- Airtable Tables (names) ---
CAMPAIGNS_TABLE = os.getenv("CAMPAIGNS_TABLE", "Campaigns")
TEMPLATES_TABLE = "Templates"
DRIP_QUEUE_TABLE = "Drip Queue"

# --- Bases from your .env ---
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")            # appMn2MKocaJ9I3rW
PERFORMANCE_BASE = os.getenv("PERFORMANCE_BASE")              # appzRWrpFggxlRBgL

# --- Keys ---
MAIN_KEY = os.getenv("AIRTABLE_API_KEY")
REPORTING_KEY = os.getenv("AIRTABLE_REPORTING_KEY", MAIN_KEY)

# --- Helpers ---
def utcnow():
    return datetime.now(timezone.utc)

def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', s.strip().lower()) if isinstance(s, str) else s

def _auto_field_map(table, sample_record_id=None):
    """
    Build a map: normalized_field_name -> actual Airtable field name for this table.
    """
    try:
        rec = table.get(sample_record_id) if sample_record_id else (table.all(max_records=1)[0] if table.all(max_records=1) else {"fields": {}})
        keys = list(rec.get("fields", {}).keys())
    except Exception:
        keys = []
    return {_norm(k): k for k in keys}

def _remap_existing_only(table, payload: dict, sample_record_id=None) -> dict:
    """
    Only include keys that exist on the table (by normalized match).
    Prevents 422 UNKNOWN_FIELD_NAME.
    """
    amap = _auto_field_map(table, sample_record_id)
    out = {}
    for k, v in payload.items():
        nk = _norm(k)
        if nk in amap:
            out[amap[nk]] = v
    return out

def _get(f: dict, *names):
    """
    Read a field value trying multiple variants (snake_case / Title Case).
    """
    for n in names:
        if n in f:
            return f[n]
    # try normalized match
    nf = {_norm(k): k for k in f.keys()}
    for n in names:
        key = nf.get(_norm(n))
        if key:
            return f[key]
    return None

def pick_template(template_ids, templates_table):
    """Pick a random template from linked templates in campaign."""
    if not template_ids:
        return None, None
    tid = random.choice(template_ids)
    try:
        tmpl = templates_table.get(tid)
    except Exception:
        return None, None
    if not tmpl:
        return None, None
    return _get(tmpl.get("fields", {}), "Message", "message"), tid

# --- Lazy Airtable Clients ---
@lru_cache(maxsize=None)
def _api_main():
    if not (MAIN_KEY and LEADS_CONVOS_BASE):
        return None
    return Api(MAIN_KEY)

@lru_cache(maxsize=None)
def _api_reporting():
    if not (REPORTING_KEY and PERFORMANCE_BASE):
        return None
    return Api(REPORTING_KEY)

@lru_cache(maxsize=None)
def get_campaigns():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, CAMPAIGNS_TABLE) if api else None

@lru_cache(maxsize=None)
def get_templates():
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, TEMPLATES_TABLE) if api else None

def get_prospects(table_name: str):
    api = _api_main()
    return api.table(LEADS_CONVOS_BASE, table_name) if api else None

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

# --- Main Runner ---
def run_campaigns(limit: str | int = 1, retry_limit: int = 3):
    """
    Runs scheduled campaigns:
      - Supports P1/P2 prospect tables
      - Rotates templates
      - Throttles outbound for compliance
      - Logs to KPIs + Runs
      - Field names tolerant (snake_case / Title Case) and only-updates-existing fields
    """
    campaigns, templates, drip, runs, kpis = (
        get_campaigns(),
        get_templates(),
        get_drip(),
        get_runs(),
        get_kpis(),
    )

    if not (campaigns and templates and drip):
        print("⚠️ CampaignRunner: Missing Airtable tables or API key/base env. Check .env and load_dotenv().")
        return {"ok": False, "processed": 0, "results": [], "errors": ["Missing Airtable tables"]}

    now = utcnow()
    now_iso = now.isoformat()
    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 9999

    # Avoid formula field-name exactness: fetch all, then filter in Python by status
    try:
        all_campaigns = campaigns.all()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "processed": 0, "results": [], "errors": ["Failed to fetch campaigns"]}

    # Keep only 'Scheduled'
    scheduled = []
    for c in all_campaigns:
        status_val = _get(c.get("fields", {}), "status", "Status")
        if status_val == "Scheduled":
            scheduled.append(c)

    processed, results = 0, []

    for camp in scheduled:
        if processed >= limit:
            break

        f, cid = camp.get("fields", {}), camp["id"]
        name = _get(f, "Name", "name") or "Unnamed"

        # Skip paused/cancelled
        if _get(f, "status", "Status") in ["Paused", "Cancelled"]:
            continue

        # --- Start/End Time (support snake_case and Title Case) ---
        start_str = _get(f, "start_time", "Start Time")
        end_str   = _get(f, "end_time", "End Time")
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")) if start_str else None
        end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00")) if end_str else None

        if not start_dt or now < start_dt:
            continue
        if end_dt and now > end_dt:
            payload = {"status": "Completed", "last_run_at": now_iso}
            mapped = _remap_existing_only(campaigns, payload, sample_record_id=cid)
            if mapped:
                try:
                    campaigns.update(cid, mapped)
                except Exception:
                    traceback.print_exc()
            continue

        # --- Templates (link field may be 'templates' or 'Templates') ---
        template_ids = _get(f, "templates", "Templates") or []
        if not template_ids:
            print(f"⚠️ Campaign {name} missing templates, skipping")
            continue

        # --- Prospect Source (P1 / P2) ---
        # View/Segment can be like:
        #   "P1 / My View Name"
        #   "P2 / Ramsey County, MN - Tax Delinquent"
        #   or just a plain view name (uses default table below)
        view_raw = (f.get("View/Segment") or "").strip()

        # Default table from a field (falls back to P1)
        table_name = f.get("Prospect Table", "P1")
        view = None

        m = re.match(r"^(P[12])\s*/\s*(.+)$", view_raw)
        if m:
            table_name = m.group(1)   # "P1" or "P2"
            view = m.group(2)         # actual view name on that table
        elif view_raw:
            view = view_raw           # plain view on the default table

        prospects_table = get_prospects(table_name)
        if not prospects_table:
            print(f"⚠️ Campaign {name} missing prospect table {table_name}")
            continue

        try:
            prospect_records = prospects_table.all(view=view) if view else prospects_table.all()
        except Exception:
            traceback.print_exc()
            continue

        total_prospects, queued = len(prospect_records), 0

        # --- Queue Prospects with Drip Spacing ---
        for idx, prospect in enumerate(prospect_records):
            pf = prospect.get("fields", {})
            phone = _get(pf, "phone", "Phone")
            property_id = _get(pf, "Property ID", "property_id")
            if not phone:
                continue

            # Pick random template
            template_text, chosen_tid = pick_template(template_ids, templates)
            if not template_text:
                continue

            personalized_text = format_template(template_text, pf)
            next_send = now + timedelta(seconds=idx * 3)  # throttle ~20 msgs/minute

            try:
                drip.create(
                    {
                        "Prospect": [prospect["id"]],
                        "Campaign": [cid],
                        "Template": [chosen_tid],
                        "phone": phone,
                        "message_preview": personalized_text,
                        "status": "QUEUED",
                        "from_number": None,  # 🔄 integrate number pools later
                        "next_send_date": next_send.isoformat(),
                        "Property ID": property_id,
                    }
                )
                queued += 1
            except Exception:
                print(f"❌ Failed to queue {phone}")
                traceback.print_exc()

        # --- Batch Send ---
        batch_result = send_batch(limit=500)

        # --- Retry Loop ---
        retry_result = {}
        if batch_result.get("total_sent", 0) < queued:
            for attempt in range(retry_limit):
                retry_result = run_retry(limit=100, view="Failed Sends")
                if retry_result.get("retried", 0) == 0:
                    break

        # Sent-so-far (prefer existing rollup if present)
        current_sent = _get(f, "messages_sent", "Messages Sent") or 0
        sent_so_far = (
            (current_sent or 0)
            + (batch_result.get("total_sent", 0) or 0)
            + (retry_result.get("retried", 0) or 0)
        )
        completed = total_prospects > 0 and (sent_so_far >= total_prospects)

        # --- Update Campaign (only fields that actually exist) ---
        # Use fields that exist in your schema. Your schema includes:
        #   status, last_run_at, Last Run Result, total_sent (numbers)
        payload = {
            "status": "Completed" if completed else "Running",
            "total_sent": sent_so_far,                 # exists in your schema
            "Last Run Result": json.dumps({
                "Queued": queued,
                "Sent": batch_result.get("total_sent", 0),
                "Retries": retry_result.get("retried", 0),
                "Completed": completed
            }),
            "last_run_at": now_iso,
        }
        mapped = _remap_existing_only(campaigns, payload, sample_record_id=cid)
        if mapped:
            try:
                campaigns.update(cid, mapped)
            except Exception:
                traceback.print_exc()

        # --- Log Runs ---
        if runs:
            try:
                runs.create(
                    {
                        "Type": "CAMPAIGN_RUN",
                        "Campaign": name,
                        "Processed": sent_so_far,
                        "Breakdown": json.dumps({"initial": batch_result, "retries": retry_result}),
                        "Timestamp": now_iso,
                    }
                )
            except Exception:
                traceback.print_exc()

        if kpis:
            try:
                kpis.create(
                    {
                        "Campaign": name,
                        "Metric": "OUTBOUND_SENT",
                        "Value": float(sent_so_far),
                        "Date": now.date().isoformat(),
                    }
                )
            except Exception:
                traceback.print_exc()

        results.append({
            "campaign": name,
            "queued": queued,
            "sent": sent_so_far,
            "completed": completed,
            "retries": retry_result.get("retried", 0)
        })
        processed += 1

    # Update cross-campaign metrics (uses its own mapping internally)
    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}
