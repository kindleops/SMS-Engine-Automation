import os
import traceback
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query
from pyairtable import Table

from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.quota_reset import reset_daily_quotas
from sms.metrics_tracker import update_metrics
from sms.inbound_webhook import router as inbound_router

# --- Load environment variables from project root ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path=ENV_PATH, override=True)

# --- FastAPI app ---
app = FastAPI()
app.include_router(inbound_router)

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")

# Performance base (for runs + KPIs)
PERF_BASE = os.getenv("PERFORMANCE_BASE")
PERF_KEY = os.getenv("AIRTABLE_REPORTING_KEY") or os.getenv("AIRTABLE_API_KEY")

# Campaign control base (for numbers, quotas, etc.)
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")
NUMBERS_TABLE = os.getenv("NUMBERS_TABLE", "Numbers")


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
    return {"ok": True}


# --- Outbound Batch Endpoint ---
@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    template: str = Query("intro", description="Which template to use")
):
    check_token(x_cron_token)
    result = send_batch()
    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "OUTBOUND",
                "Processed": result.get("total_sent", 0),
                "Breakdown": str(result.get("results", [])),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            if kpis and result.get("total_sent", 0) > 0:
                kpis.create({
                    "Campaign": "ALL",
                    "Metric": "OUTBOUND_SENT",
                    "Value": result.get("total_sent", 0),
                    "Date": datetime.now(timezone.utc).date().isoformat()
                })
            result["run_id"] = run_record["id"]
        except Exception:
            print("⚠️ Failed to write to Performance base:")
            traceback.print_exc()
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

    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "AUTORESPONDER",
                "Processed": result.get("processed", 0),
                "Breakdown": str(result.get("breakdown", {})),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            result["run_id"] = run_record["id"]

            if kpis:
                for intent, count in (result.get("breakdown") or {}).items():
                    if count > 0:
                        kpis.create({
                            "Campaign": "ALL",
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
async def ai_closer_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "AI Closer"
    result = run_autoresponder(limit=limit, view=view)

    runs, _ = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "AI_CLOSER",
                "Processed": result.get("processed", 0),
                "Breakdown": str(result.get("breakdown", {})),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            result["run_id"] = run_record["id"]
        except Exception:
            print("⚠️ Failed to log AI Closer run")
            traceback.print_exc()
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

    runs, _ = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "MANUAL_QA",
                "Processed": result.get("processed", 0),
                "Breakdown": str(result.get("breakdown", {})),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            result["run_id"] = run_record["id"]
        except Exception:
            print("⚠️ Failed to log Manual QA run")
            traceback.print_exc()
    return result


# --- Reset Quotas ---
@app.post("/reset-quotas")
async def reset_quotas_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = reset_daily_quotas()

    runs, _ = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "Type": "RESET_QUOTAS",
                "Processed": result.get("updated", 0),
                "Breakdown": str(result),
                "Timestamp": datetime.now(timezone.utc).isoformat()
            })
            result["run_id"] = run_record["id"]
        except Exception:
            print("⚠️ Failed to log quota reset")
            traceback.print_exc()
    return result