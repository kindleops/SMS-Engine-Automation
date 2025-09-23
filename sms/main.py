import os
import traceback
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Request
from pyairtable import Table

from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.quota_reset import reset_daily_quotas
from sms.metrics_tracker import update_metrics
from sms.inbound_webhook import router as inbound_router
from sms.campaign_runner import run_campaigns
from sms.kpi_aggregator import aggregate_kpis
from sms.retry_runner import run_retry   # ✅ retry runner

# --- Load env ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# --- FastAPI app ---
app = FastAPI()
app.include_router(inbound_router)

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")

# Bases
PERF_BASE = os.getenv("PERFORMANCE_BASE")
PERF_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")

# Leads + Templates
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE")
AIRTABLE_API_KEY  = os.getenv("AIRTABLE_API_KEY")
TEMPLATES_TABLE   = os.getenv("TEMPLATES_TABLE", "Templates")
LEADS_TABLE       = os.getenv("LEADS_TABLE", "Leads")

templates = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, TEMPLATES_TABLE) if AIRTABLE_API_KEY else None
leads     = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, LEADS_TABLE) if AIRTABLE_API_KEY else None


# --- Airtable helpers ---
def get_perf_tables():
    if not PERF_KEY or not PERF_BASE:
        return None, None
    try:
        runs = Table(PERF_KEY, PERF_BASE, "Runs/Logs")
        kpis = Table(PERF_KEY, PERF_BASE, "KPIs")
        return runs, kpis
    except Exception:
        print("⚠️ Failed to init Performance tables:")
        traceback.print_exc()
        return None, None


def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# --- Template KPI Tracker ---
def log_template_kpi(template_id: str | None, metric: str):
    """Increment KPI counters on Templates table."""
    if not templates or not template_id:
        return
    try:
        row = templates.get(template_id)
        fields = row.get("fields", {})
        updates = {}

        if metric == "sent":
            updates["Sends"] = fields.get("Sends", 0) + 1
        elif metric == "delivered":
            updates["Delivered"] = fields.get("Delivered", 0) + 1
        elif metric == "failed":
            updates["Failed Deliveries"] = fields.get("Failed Deliveries", 0) + 1
        elif metric == "replied":
            updates["Replies"] = fields.get("Replies", 0) + 1

        templates.update(template_id, updates)
        print(f"📊 Updated template {template_id}: +1 {metric}")
    except Exception as e:
        print(f"⚠️ Failed to update template KPI {metric}: {e}")


# --- Startup Debug ---
@app.on_event("startup")
def log_env_config():
    print("✅ Environment loaded:")
    print(f"   LEADS_CONVOS_BASE: {os.getenv('LEADS_CONVOS_BASE')}")
    print(f"   CAMPAIGN_CONTROL_BASE: {os.getenv('CAMPAIGN_CONTROL_BASE')}")
    print(f"   PERFORMANCE_BASE: {os.getenv('PERFORMANCE_BASE')}")
    print(f"   NUMBERS_TABLE: {NUMBERS_TABLE}")
    print(f"   CONVERSATIONS_TABLE: {os.getenv('CONVERSATIONS_TABLE', 'Conversations')}")


# --- Health ---
@app.get("/health")
def health():
    return {"ok": True, "timestamp": datetime.now(timezone.utc).isoformat()}


# --- Outbound Batch ---
@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    template: str = Query("intro", description="Which template to use"),
    campaign: str = Query("ALL", description="Campaign label")
):
    check_token(x_cron_token)
    result = send_batch(template=template, campaign=campaign)

    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "OUTBOUND",
                "Processed": result.get("total_sent", 0),
                "Breakdown": str(result.get("results", [])),
                "Campaign": campaign,
                "Template": template,
                "Timestamp": iso_timestamp()
            })
            if kpis and result.get("total_sent", 0) > 0:
                kpis.create({
                    "Campaign": campaign,
                    "Metric": "OUTBOUND_SENT",
                    "Value": result.get("total_sent", 0),
                    "Date": datetime.now(timezone.utc).date().isoformat()
                })
            result["run_id"] = run_record["id"]
        except Exception:
            print("⚠️ Failed to write to Performance base:")
            traceback.print_exc()
    return result


# --- Autoresponder ---
@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None),
    campaign: str = Query("ALL", description="Campaign label")
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Autoresponder"
    result = run_autoresponder(limit=limit, view=view)

    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "AUTORESPONDER",
                "Processed": result.get("processed", 0),
                "Breakdown": str(result.get("breakdown", {})),
                "Campaign": campaign,
                "Timestamp": iso_timestamp()
            })
            result["run_id"] = run_record["id"]

            if kpis:
                for intent, count in (result.get("breakdown") or {}).items():
                    if count > 0:
                        kpis.create({
                            "Campaign": campaign,
                            "Metric": intent,
                            "Value": count,
                            "Date": datetime.now(timezone.utc).date().isoformat()
                        })
        except Exception:
            print("⚠️ Failed to write to Performance base:")
            traceback.print_exc()
    return result


# --- AI Closer ---
@app.post("/ai-closer")
async def ai_closer_endpoint(limit: int = 50, view: str = "Unprocessed Inbounds", x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "AI Closer"
    return run_autoresponder(limit=limit, view=view)


# --- Manual QA ---
@app.post("/manual-qa")
async def manual_qa_endpoint(limit: int = 50, view: str = "Unprocessed Inbounds", x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Manual QA"
    return run_autoresponder(limit=limit, view=view)


# --- Delivery Status Webhook ---
@app.post("/status")
async def delivery_status(request: Request):
    data = await request.json()
    sid         = data.get("MessageSid") or data.get("sid")
    status      = str(data.get("MessageStatus") or data.get("status", "")).lower()
    template_id = data.get("template_id")
    lead_id     = data.get("lead_id")

    print(f"📡 Delivery status for {sid}: {status}")

    if template_id:
        if "delivered" in status:
            log_template_kpi(template_id, "delivered")
        elif "fail" in status or "undeliverable" in status:
            log_template_kpi(template_id, "failed")

    if lead_id and leads:
        try:
            updates = {"Last Delivery Status": status.upper()}
            if "delivered" in status:
                updates["Delivered Count"] = leads.get(lead_id)["fields"].get("Delivered Count", 0) + 1
            elif "fail" in status:
                updates["Failed Count"] = leads.get(lead_id)["fields"].get("Failed Count", 0) + 1
            leads.update(lead_id, updates)
        except Exception as e:
            print(f"⚠️ Failed to update lead delivery metrics: {e}")

    return {"ok": True, "sid": sid, "status": status}


# --- Reset Quotas ---
@app.post("/reset-quotas")
async def reset_quotas_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return reset_daily_quotas()


# --- Metrics ---
@app.post("/metrics")
async def metrics_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return update_metrics()


# --- Retry Worker ---
@app.post("/retry")
async def retry_endpoint(limit: int = 100, view: str | None = None, x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return run_retry(limit=limit, view=view)


# --- KPI Aggregator ---
@app.post("/aggregate-kpis")
async def aggregate_kpis_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    return aggregate_kpis()


# --- Campaign Runner ---
@app.post("/run-campaigns")
async def run_campaigns_endpoint(
    limit: str = Query("ALL", description="Number of campaigns to process, or ALL"),
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)

    # Convert "ALL" → large int
    lim = 9999 if str(limit).upper() == "ALL" else int(limit)

    result = run_campaigns(limit=lim)

    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "CAMPAIGN_RUNNER",
                "Processed": result.get("processed", 0),
                "Breakdown": str(result.get("results", [])),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            result["run_id"] = run_record["id"]
        except Exception:
            print("⚠️ Failed to log campaign runner")
            traceback.print_exc()
    return result