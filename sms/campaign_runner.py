# sms/campaign_runner.py
import os, traceback, json
from datetime import datetime, timezone
from functools import lru_cache

from sms.outbound_batcher import send_batch, format_template
from sms.metrics_tracker import update_metrics
from sms.retry_runner import run_retry   # 🔁 retry handler

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- Airtable Tables ---
CAMPAIGNS_TABLE   = "Campaigns"
TEMPLATES_TABLE   = "Templates"
PROSPECTS_TABLE   = "Prospects"      # 🔥 Prospect-level outreach
DRIP_QUEUE_TABLE  = "Drip Queue"


# --- Lazy Airtable Clients ---
@lru_cache(maxsize=None)
def get_campaigns():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    if api_key and base_id and Table:
        try:
            return Table(api_key, base_id, CAMPAIGNS_TABLE)
        except Exception as e:
            print(f"❌ CampaignRunner: failed to init Campaigns table: {e}")
    return None


@lru_cache(maxsize=None)
def get_templates():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    return Table(api_key, base_id, TEMPLATES_TABLE) if api_key and base_id and Table else None


@lru_cache(maxsize=None)
def get_prospects():
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE")
    return Table(api_key, base_id, PROSPECTS_TABLE) if api_key and base_id and Table else None


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


# --- Main Runner ---
def run_campaigns(limit: str | int = 1, retry_limit: int = 3):
    """
    Auto-runs scheduled Prospect campaigns with full lifecycle:
    - Pulls prospects by view/segment
    - Queues into Drip Queue with personalization
    - Sends batch + retries failures
    - Updates Campaigns + logs KPIs
    - Marks Completed once all prospects processed
    """
    campaigns = get_campaigns()
    templates = get_templates()
    prospects = get_prospects()
    drip      = get_drip()
    runs      = get_runs()
    kpis      = get_kpis()

    if not (campaigns and templates and prospects and drip):
        print("⚠️ CampaignRunner: MOCK mode → skipping real Airtable ops")
        return {
            "ok": False,
            "type": "Prospect",
            "processed": 0,
            "results": [],
            "errors": ["Missing Airtable tables"]
        }

    now = utcnow()
    now_iso = now.isoformat()

    if isinstance(limit, str) and limit.upper() == "ALL":
        limit = 9999

    try:
        scheduled = campaigns.all(formula="{Status}='Scheduled'")
    except Exception:
        traceback.print_exc()
        return {
            "ok": False,
            "type": "Prospect",
            "processed": 0,
            "results": [],
            "errors": ["Failed to fetch campaigns"]
        }

    processed, results = 0, []

    for camp in scheduled:
        if processed >= limit:
            break

        f = camp["fields"]
        cid = camp["id"]
        name = f.get("Name", "Unnamed Campaign")

        # --- Handle Start/End Time ---
        start_dt = datetime.fromisoformat(f["Start Time"].replace("Z", "+00:00")) if f.get("Start Time") else None
        end_dt   = datetime.fromisoformat(f["End Time"].replace("Z", "+00:00")) if f.get("End Time") else None

        if not start_dt or now < start_dt:
            continue
        if end_dt and now > end_dt:
            try:
                campaigns.update(cid, {"Status": "Completed", "Last Run At": now_iso})
            except Exception:
                traceback.print_exc()
            continue

        # --- Template ---
        template_id = (f.get("Template") or [None])[0]
        if not template_id:
            print(f"⚠️ Campaign {name} missing template, skipping")
            continue

        tmpl = templates.get(template_id)
        template_text = tmpl["fields"].get("Message", "")
        if not template_text:
            print(f"⚠️ Template {template_id} empty, skipping")
            continue

        # --- Prospects ---
        view = f.get("View/Segment")
        try:
            prospect_records = prospects.all(view=view) if view else prospects.all()
        except Exception:
            traceback.print_exc()
            continue

        total_prospects = len(prospect_records)
        queued = 0

        # --- Queue Prospects ---
        for prospect in prospect_records:
            pf = prospect["fields"]
            phone = pf.get("phone")
            property_id = pf.get("Property ID")
            if not phone:
                continue

            personalized_text = format_template(template_text, pf)

            try:
                drip.create({
                    "Prospect": [prospect["id"]],
                    "Campaign": [cid],
                    "Template": [template_id],
                    "phone": phone,
                    "message_preview": personalized_text,
                    "status": "QUEUED",
                    "from_number": None,  # 🔄 TODO: integrate number pooling
                    "next_send_date": now_iso,
                    "Property ID": property_id
                })
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

        # --- Progress ---
        sent_so_far = f.get("Messages Sent", 0) + batch_result.get("total_sent", 0) + retry_result.get("retried", 0)
        completed = sent_so_far >= total_prospects

        # --- Update Campaign ---
        try:
            campaigns.update(cid, {
                "Status": "Completed" if completed else "Running",
                "Queued Prospects": queued,
                "Messages Sent": sent_so_far,
                "Completion %": round(sent_so_far / total_prospects * 100, 2) if total_prospects else 0,
                "Last Run Result": json.dumps({
                    "Queued": queued,
                    "Sent": batch_result.get("total_sent", 0),
                    "Retries": retry_result.get("retried", 0),
                    "Completed": completed
                }),
                "Last Run At": now_iso
            })
        except Exception:
            traceback.print_exc()

        # --- Log to Performance ---
        if runs:
            try:
                run_record = runs.create({
                    "Type": "CAMPAIGN_RUN",
                    "Campaign": name,
                    "Processed": sent_so_far,
                    "Breakdown": json.dumps({
                        "initial": batch_result,
                        "retries": retry_result
                    }),
                    "Timestamp": now_iso
                })
                batch_result["run_id"] = run_record["id"]
            except Exception:
                traceback.print_exc()

        if kpis:
            try:
                kpis.create({
                    "Campaign": name,
                    "Metric": "OUTBOUND_SENT",
                    "Value": sent_so_far,
                    "Date": now.date().isoformat()
                })
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

    try:
        update_metrics()
    except Exception:
        traceback.print_exc()

    # --- Standardized return ---
    return {
        "ok": True,
        "type": "Prospect",
        "processed": processed,
        "results": results,
        "errors": []
    }