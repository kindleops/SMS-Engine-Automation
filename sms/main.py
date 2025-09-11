# sms/main.py
import os
import traceback
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException, Request, Query
from pyairtable import Table

from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.templates import TEMPLATES
from sms.quota_reset import reset_daily_quotas
from sms import autoresponder

app = FastAPI()

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")

# Airtable (Conversations base for inbound)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# Performance base can be set by either name
PERF_BASE = (
    os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
    or os.getenv("PERFORMANCE_BASE")
)
PERF_KEY = (
    os.getenv("AIRTABLE_REPORTING_KEY")
    or os.getenv("AIRTABLE_API_KEY")
)

# --- Airtable tables ---
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

@app.get("/health")
def health():
    return {"ok": True}

# --- Outbound Batch Endpoint ---
@app.post("/send")
async def send_endpoint(
    x_cron_token: str | None = Header(None),
    template: str = Query("intro", description="Which template to use: intro, followup_yes, followup_no, followup_wrong")
):
    check_token(x_cron_token)
    result = send_batch(template_key=template)   # pass template to batcher

    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "type": "OUTBOUND",
                "processed": result.get("total_sent", 0),
                "breakdown": str(result.get("results", [])),
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            if kpis and result.get("total_sent", 0) > 0:
                kpis.create({
                    "metric": "OUTBOUND_SENT",
                    "count": result.get("total_sent", 0),
                    "date": datetime.now(timezone.utc).date().isoformat()
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

# AI Closer
@app.post("/ai-closer")
async def ai_closer_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "AI Closer"
    return run_autoresponder(limit=limit, view=view)

# Manual QA
@app.post("/manual-qa")
async def manual_qa_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    os.environ["PROCESSED_BY_LABEL"] = "Manual QA"
    return run_autoresponder(limit=limit, view=view)

    runs, kpis = get_perf_tables()
    if runs:
        try:
            run_record = runs.create({
                "type": "AUTORESPONDER",
                "processed": result.get("processed", 0),
                "breakdown": str(result.get("breakdown", {})),
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            result["run_id"] = run_record["id"]

            if kpis:
                for intent, count in (result.get("breakdown") or {}).items():
                    if count > 0:
                        kpis.create({
                            "metric": intent,
                            "count": count,
                            "date": datetime.now(timezone.utc).date().isoformat()
                        })
        except Exception:
            print("⚠️ Failed to write to Performance base:")
            traceback.print_exc()
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

        from_number = data.get("from")
        to_number   = data.get("to")
        message     = data.get("message")
        msg_id      = data.get("id")

        if not (AIRTABLE_API_KEY and LEADS_CONVOS_BASE):
            print("⚠️ Airtable not configured for inbound logging")
            return {"ok": False, "error": "Airtable not configured"}

        convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
        convos.create({
            "From Number": from_number,
            "To Number": to_number,
            "Message": message,
            "Status": "UNPROCESSED",
            "Direction": "IN",
            "TextGrid ID": msg_id,
            "Received At": datetime.now(timezone.utc).isoformat()
        })

        return {"ok": True}
    except Exception as e:
        print("❌ Error in inbound handler:", e)
        traceback.print_exc()
        return {"ok": False, "error": str(e)}
    
    @app.post("/reset-quotas")
    async def reset_quotas_endpoint(x_cron_token: str | None = Header(None)):
        check_token(x_cron_token)
        result = reset_daily_quotas()
        return result