# sms/main.py
import os
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from pyairtable import Table

app = FastAPI()

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
PERFORMANCE_BASE = os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")

# --- Performance Base tables ---
runs = Table(AIRTABLE_API_KEY, PERFORMANCE_BASE, "Runs/Logs")
kpis = Table(AIRTABLE_API_KEY, PERFORMANCE_BASE, "KPIs")

# --- Token Check ---
def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Outbound Batch Endpoint ---
@app.post("/send")
async def send_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = send_batch()

    # Log outbound run to Performance Base
    run_record = runs.create({
        "type": "OUTBOUND",
        "processed": result.get("total_sent", 0),
        "breakdown": str(result.get("results", [])),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

    # Update KPI counter
    kpis.create({
        "metric": "OUTBOUND_SENT",
        "count": result.get("total_sent", 0),
        "date": datetime.now(timezone.utc).date().isoformat()
    })

    return {**result, "run_id": run_record["id"]}

# --- Autoresponder Endpoint ---
@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    result = run_autoresponder(limit=limit, view=view)

    # Log autoresponder run to Performance Base
    run_record = runs.create({
        "type": "AUTORESPONDER",
        "processed": result.get("processed", 0),
        "breakdown": str(result.get("breakdown", {})),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

    # Update KPI counters
    for intent, count in result.get("breakdown", {}).items():
        if count > 0:
            kpis.create({
                "metric": intent,
                "count": count,
                "date": datetime.now(timezone.utc).date().isoformat()
            })

    return {**result, "run_id": run_record["id"]}