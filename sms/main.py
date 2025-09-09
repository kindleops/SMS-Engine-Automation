# sms/main.py
import os
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.airtable_client import perf_table
from fastapi.responses import JSONResponse

app = FastAPI()

CRON_TOKEN = os.getenv("CRON_TOKEN")

# Performance base tables (use reporting key)
runs = perf_table("Runs/Logs")
kpis = perf_table("KPIs")

def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/env-check")
def env_check():
    return JSONResponse({
        "has_reporting_key": bool(os.getenv("AIRTABLE_REPORTING_KEY")),
        "has_acq_key": bool(os.getenv("AIRTABLE_ACQUISITIONS_KEY")),
        "has_dispo_key": bool(os.getenv("AIRTABLE_DISPO_KEY")),
        "bases": {
            "leads_convos": os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID"),
            "campaign_control": os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID"),
            "performance": os.getenv("AIRTABLE_PERFORMANCE_BASE_ID"),
        }
    })

@app.post("/send")
async def send_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = send_batch()
    run_record = runs.create({
        "type": "OUTBOUND",
        "processed": result.get("total_sent", 0),
        "breakdown": str(result.get("results", [])),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    if result.get("total_sent", 0):
        kpis.create({"metric": "OUTBOUND_SENT", "count": result["total_sent"],
                     "date": datetime.now(timezone.utc).date().isoformat()})
    return {**result, "run_id": run_record["id"]}

@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    result = run_autoresponder(limit=limit, view=view)
    run_record = runs.create({
        "type": "AUTORESPONDER",
        "processed": result.get("processed", 0),
        "breakdown": str(result.get("breakdown", {})),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    for intent, count in (result.get("breakdown") or {}).items():
        if count > 0:
            kpis.create({"metric": intent, "count": count,
                         "date": datetime.now(timezone.utc).date().isoformat()})
    return {**result, "run_id": run_record["id"]}