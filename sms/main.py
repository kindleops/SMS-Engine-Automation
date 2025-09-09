# sms/main.py
import os
import traceback
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException
from pyairtable import Table

from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder

app = FastAPI()

# --- ENV CONFIG ---
CRON_TOKEN = os.getenv("CRON_TOKEN")

# Performance base can be set by either name
PERF_BASE = (
    os.getenv("AIRTABLE_PERFORMANCE_BASE_ID")
    or os.getenv("PERFORMANCE_BASE")
)

# Prefer the reporting key for performance (falls back to the general key)
PERF_KEY = (
    os.getenv("AIRTABLE_REPORTING_KEY")
    or os.getenv("AIRTABLE_API_KEY")
)

def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

def get_perf_tables():
    """
    Lazily create Performance tables if envs exist.
    Returns (runs, kpis) or (None, None) if not configured.
    Never raises at import/startup.
    """
    if not PERF_KEY or not PERF_BASE:
        return None, None
    try:
        runs = Table(PERF_KEY, PERF_BASE, "Runs/Logs")
        kpis = Table(PERF_KEY, PERF_BASE, "KPIs")
        return runs, kpis
    except Exception:
        # Log but don't crash the app
        print("⚠️ Failed to init Performance tables:")
        traceback.print_exc()
        return None, None

@app.get("/health")
def health():
    return {"ok": True}

# --- Outbound Batch Endpoint ---
@app.post("/send")
async def send_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)

    result = send_batch()

    # Try to log; if not configured, skip silently
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

    result = run_autoresponder(limit=limit, view=view)

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