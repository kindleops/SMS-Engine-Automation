# ops/webhooks.py
import os
import traceback
import platform
import psutil  # pip install psutil
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pyairtable import Table

app = FastAPI(title="REI DevOps Service")

CRON_TOKEN = os.getenv("CRON_TOKEN")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
DEVOPS_BASE = os.getenv("AIRTABLE_DEVOPS_BASE_ID")

# Airtable tables
logs_table = Table(AIRTABLE_API_KEY, DEVOPS_BASE, "Logs")
metrics_table = Table(AIRTABLE_API_KEY, DEVOPS_BASE, "Metrics")
alerts_table = Table(AIRTABLE_API_KEY, DEVOPS_BASE, "Alerts")


def check_token(token: str):
    if not CRON_TOKEN or token != CRON_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid cron token")


def iso_timestamp():
    return datetime.now(timezone.utc).isoformat()


@app.post("/sync-logs")
async def sync_logs(x_cron_token: str = None):
    try:
        check_token(x_cron_token)
        print("📝 [DevOps] Syncing error logs...")

        # Example: capture latest errors from file/stdout (stubbed here)
        sample_log = {
            "Service": "rei-sms-engine",
            "Level": "ERROR",
            "Message": "Sample error for debugging",
            "Timestamp": iso_timestamp(),
        }

        logs_table.create(sample_log)
        return {"ok": True, "message": "Logs synced", "data": sample_log}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync-metrics")
async def sync_metrics(x_cron_token: str = None):
    try:
        check_token(x_cron_token)
        print("📊 [DevOps] Syncing server metrics...")

        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent

        metric = {
            "Service": "rei-sms-engine",
            "CPU %": cpu,
            "Memory %": mem,
            "Host": platform.node(),
            "Timestamp": iso_timestamp(),
        }

        metrics_table.create(metric)
        return {"ok": True, "message": "Metrics synced", "data": metric}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/alerts")
async def alerts(x_cron_token: str = None):
    try:
        check_token(x_cron_token)
        print("🚨 [DevOps] Checking for alerts...")

        # Example: alert if CPU > 80%
        cpu = psutil.cpu_percent(interval=1)
        if cpu > 80:
            alert = {
                "Service": "rei-sms-engine",
                "Alert": f"High CPU usage detected: {cpu}%",
                "Severity": "HIGH",
                "Timestamp": iso_timestamp(),
            }
            alerts_table.create(alert)
            return {"ok": True, "alert_triggered": True, "data": alert}

        return {"ok": True, "alert_triggered": False}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "rei-devops"}