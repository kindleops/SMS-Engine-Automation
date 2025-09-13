import os
import traceback
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request, Query
from pyairtable import Table

from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.quota_reset import reset_daily_quotas
from sms.metrics_tracker import update_metrics

# Load environment variables
load_dotenv()
app = FastAPI()

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")

# Airtable (Conversations base for inbound)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# Performance base (for runs + KPIs)
PERF_BASE = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID") or os.getenv("PERFORMANCE_BASE")
PERF_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")

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

def log_run(run_type: str, processed: int, breakdown: dict | str):
    runs, _ = get_perf_tables()
    if runs:
        try:
            record = runs.create({
                "Type": run_type,
                "Processed": processed,
                "Breakdown": str(breakdown),
                "Timestamp": datetime.now(timezone.utc).isoformat(),
            })
            return record["id"]
        except Exception:
            print(f"⚠️ Failed to log run of type {run_type}")
            traceback.print_exc()
    return None

def log_kpi(metric: str, value: int, campaign: str = "ALL"):
    _, kpis = get_perf_tables()
    if kpis:
        try:
            kpis.create({
                "Campaign": campaign,
                "Metric": metric,
                "Value": value,
                "Date": datetime.now(timezone.utc).date().isoformat()
            })
        except Exception:
            print(f"⚠️ Failed to log KPI: {metric}")
            traceback.print_exc()

def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Health ---
@app.get("/health")
def health():
    return {"ok": True}

# --- Outbound Batch Endpoint ---
@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    template: str = Query("intro", description="Which template to use")
):
    check_token(x_cron_token)
    result = send_batch()

    run_id = log_run("OUTBOUND", result.get("total_sent", 0), result.get("results", []))
    if run_id:
        result["run_id"] = run_id

    if result.get("total_sent", 0) > 0:
        log_kpi("OUTBOUND_SENT", result["total_sent"])

    return result

# --- Autoresponder Endpoint ---
@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Autoresponder"
    result = run_autoresponder(limit=limit, view=view)

    run_id = log_run("AUTORESPONDER", result.get("processed", 0), result.get("breakdown", {}))
    if run_id:
        result["run_id"] = run_id

    for intent, count in (result.get("breakdown") or {}).items():
        if count > 0:
            log_kpi(intent, count)

    return result

# --- AI Closer ---
@app.post("/ai-closer")
async def ai_closer_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "AI Closer"
    result = run_autoresponder(limit=limit, view=view)

    run_id = log_run("AI_CLOSER", result.get("processed", 0), result.get("breakdown", {}))
    if run_id:
        result["run_id"] = run_id
    return result

# --- Manual QA ---
@app.post("/manual-qa")
async def manual_qa_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Manual QA"
    result = run_autoresponder(limit=limit, view=view)

    run_id = log_run("MANUAL_QA", result.get("processed", 0), result.get("breakdown", {}))
    if run_id:
        result["run_id"] = run_id
    return result

# --- Inbound Webhook (TextGrid callback) ---
@app.post("/inbound")
async def inbound_endpoint(request: Request):
    """
    Webhook for TextGrid inbound SMS -> stores in Airtable Conversations.
    """
    try:
        data = await request.json()
        print("📩 Inbound SMS:", data)

        if not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE):
            return {"ok": False, "error": "Airtable not configured"}

        convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
        convos.create({
            "From Number": data.get("from"),
            "To Number": data.get("to"),
            "Message": data.get("message"),
            "Status": "UNPROCESSED",
            "Direction": "IN",
            "TextGrid ID": data.get("id"),
            "Received At": datetime.now(timezone.utc).isoformat()
        })
        return {"ok": True}
    except Exception as e:
        print("❌ Error in inbound handler:", e)
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

# --- Reset Quotas ---
@app.post("/reset-quotas")
async def reset_quotas_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = reset_daily_quotas()

    run_id = log_run("RESET_QUOTAS", result.get("updated", 0), result)
    if run_id:
        result["run_id"] = run_id
    return result