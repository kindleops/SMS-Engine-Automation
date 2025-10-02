# sms/campaign_runner.py
import os
import traceback
import json
import random
from datetime import datetime, timezone, timedelta
from functools import lru_cache

from sms.outbound_batcher import send_batch, format_template
from sms.metrics_tracker import update_metrics
from sms.retry_runner import run_retry  # 🔁 retry handler

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- Airtable Tables ---
CAMPAIGNS_TABLE = "Campaigns"
TEMPLATES_TABLE = "Templates"
DRIP_QUEUE_TABLE = "Drip Queue"

# --- Lazy Airtable Clients ---
@lru_cache(maxsize=None)
def get_campaigns():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    return Table(api_key, base_id, CAMPAIGNS_TABLE) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_templates():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    return Table(api_key, base_id, TEMPLATES_TABLE) if api_key and base_id and Table else None

def get_prospects(table_name: str):
    """Dynamically return P1/P2 prospects table."""
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    return Table(api_key, base_id, table_name) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_drip():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    return Table(api_key, base_id, DRIP_QUEUE_TABLE) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_runs():
    api_key = os.getenv("AIRTABLE_API_KEY")
    perf_base = os.getenv("PERFORMANCE_BASE")
    return Table(api_key, perf_base, "Runs/Logs") if api_key and perf_base and Table else None

@lru_cache(maxsize=None)
def get_kpis():
    api_key = os.getenv("AIRTABLE_API_KEY")
    perf_base = os.getenv("PERFORMANCE_BASE")
    return Table(api_key, perf_base, "KPIs") if api_key and perf_base and Table else None

# --- Helpers ---
def utcnow():
    return datetime.now(timezone.utc)

def pick_template(template_ids, templates):
    """Pick a random template from linked templates in campaign."""
    if not template_ids:
        return None, None
    tid = random.choice(template_ids)
    tmpl = templates.get(tid)
    if not tmpl:
        return None, None
    return tmpl["fields"].get("Message", ""), tid

# --- Main Runner ---
def run_campaigns(limit: str | int = 1, retry_limit: int = 3):
    """
    Runs scheduled campaigns:
      - Supports P1/P2 prospect tables
      - Rotates templates
      - Throttles outbound for compliance
      - Logs to KPIs + Runs
    """
    campaigns, templates, drip, runs, kpis = (
        get_campaigns(),
        get_templates(),
        get_drip(),
        get_runs(),
        get_kpis(),
    )

    if not (campaigns and templates and drip):
        print("⚠️ CampaignRunner: Missing Airtable tables")
        return {"ok": False, "processed": 0, "results": [], "errors": ["Missing Airtable tables"]}

    now, now_iso = utcnow(), utcnow().isoformat()
    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 9999

    try:
        scheduled = campaigns.all(formula="{Status}='Scheduled'")
    except Exception:
        traceback.print_exc()
        return {"ok": False, "processed": 0, "results": [], "errors": ["Failed to fetch campaigns"]}

    processed, results = 0, []

    for camp in scheduled:
        if processed >= limit:
            break

        f, cid, name = camp["fields"], camp["id"], camp.get("fields", {}).get("Name", "Unnamed")

        # Skip paused/cancelled
        if f.get("Status") in ["Paused", "Cancelled"]:
            continue

        # Handle Start/End Time
        start_dt = datetime.fromisoformat(f["Start Time"].replace("Z", "+00:00")) if f.get("Start Time") else None
        end_dt = datetime.fromisoformat(f["End Time"].replace("Z", "+00:00")) if f.get("End Time") else None
        if not start_dt or now < start_dt:
            continue
        if end_dt and now > end_dt:
            campaigns.update(cid, {"Status": "Completed", "Last Run At": now_iso})
            continue

        # --- Templates ---
        template_ids = f.get("Templates") or []  # multi-select link
        if not template_ids:
            print(f"⚠️ Campaign {name} missing templates, skipping")
            continue

        # --- Prospect Source (P1 / P2) ---
        table_name = f.get("Prospect Table", "P1")  # default P1
        prospects_table = get_prospects(table_name)
        if not prospects_table:
            print(f"⚠️ Campaign {name} missing prospect table {table_name}")
            continue

        view = f.get("View/Segment")
        try:
            prospect_records = prospects_table.all(view=view) if view else prospects_table.all()
        except Exception:
            traceback.print_exc()
            continue

        total_prospects, queued = len(prospect_records), 0

        # --- Queue Prospects with Drip Spacing ---
        for idx, prospect in enumerate(prospect_records):
            pf = prospect["fields"]
            phone, property_id = pf.get("phone"), pf.get("Property ID")
            if not phone:
                continue

            # Pick random template
            template_text, chosen_tid = pick_template(template_ids, templates)
            if not template_text:
                continue

            personalized_text = format_template(template_text, pf)
            next_send = now + timedelta(seconds=idx * 3)  # throttle 20 msgs/minute

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
            except Exception as e:
                print(f"❌ Failed to queue {phone}: {e}")
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

        sent_so_far = (
            f.get("Messages Sent", 0)
            + batch_result.get("total_sent", 0)
            + retry_result.get("retried", 0)
        )
        completed = sent_so_far >= total_prospects

        # --- Update Campaign ---
        try:
            campaigns.update(
                cid,
                {
                    "Status": "Completed" if completed else "Running",
                    "Queued Prospects": queued,
                    "Messages Sent": sent_so_far,
                    "Completion %": round(sent_so_far / total_prospects * 100, 2) if total_prospects else 0,
                    "Last Run Result": json.dumps({"Queued": queued, "Sent": batch_result.get("total_sent", 0),
                                                   "Retries": retry_result.get("retried", 0), "Completed": completed}),
                    "Last Run At": now_iso,
                },
            )
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
                        "Value": sent_so_far,
                        "Date": now.date().isoformat(),
                    }
                )
            except Exception:
                traceback.print_exc()

        results.append({"campaign": name, "queued": queued, "sent": sent_so_far,
                        "completed": completed, "retries": retry_result.get("retried", 0)})
        processed += 1

    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    return {"ok": True, "processed": processed, "results": results, "errors": []}
