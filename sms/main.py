# sms/main.py
import os
from datetime import datetime, timezone, date
from fastapi import FastAPI, Header, HTTPException
from pyairtable import Table
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder

app = FastAPI()

# --- ENV (match exactly what you set in Render) ---
CRON_TOKEN        = os.getenv("CRON_TOKEN")
AIRTABLE_API_KEY  = os.getenv("AIRTABLE_API_KEY")
PERFORMANCE_BASE  = os.getenv("PERFORMANCE_BASE")  # <- was AIRTABLE_PERFORMANCE_BASE_ID

def _need(name: str, val: str | None):
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

# --- Helpers (no Table at import time) ---
def runs_table() -> Table:
    return Table(_need("AIRTABLE_API_KEY", AIRTABLE_API_KEY),
                 _need("PERFORMANCE_BASE", PERFORMANCE_BASE),
                 "Runs/Logs")

def kpis_table() -> Table:
    return Table(_need("AIRTABLE_API_KEY", AIRTABLE_API_KEY),
                 _need("PERFORMANCE_BASE", PERFORMANCE_BASE),
                 "KPIs")

def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Health / env sanity ---
@app.get("/env-check")
def env_check():
    return {
        "PERFORMANCE_BASE_present": bool(PERFORMANCE_BASE),
        "AIRTABLE_API_KEY_present": bool(AIRTABLE_API_KEY),
        "CRON_TOKEN_present": bool(CRON_TOKEN),
    }

@app.get("/health")
def health():
    return {"ok": True}

# --- Outbound Batch Endpoint ---
@app.post("/send")
async def send_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = send_batch()

    run_record = runs_table().create({
        "type": "OUTBOUND",
        "processed": result.get("total_sent", 0),
        "breakdown": str(result.get("results", [])),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    kpis_table().create({
        "metric": "OUTBOUND_SENT",
        "count": result.get("total_sent", 0),
        "date": date.today().isoformat(),
    })

    return {**result, "run_id": run_record["id"]}

# --- Autoresponder Endpoint ---
@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None),
):
    check_token(x_cron_token)
    result = run_autoresponder(limit=limit, view=view)

    run_record = runs_table().create({
        "type": "AUTORESPONDER",
        "processed": result.get("processed", 0),
        "breakdown": str(result.get("breakdown", {})),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    for intent, count in (result.get("breakdown", {}) or {}).items():
        if count > 0:
            kpis_table().create({
                "metric": intent,
                "count": count,
                "date": date.today().isoformat(),
            })

    return {**result, "run_id": run_record["id"]}