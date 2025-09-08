from fastapi import FastAPI, Header, HTTPException
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.inbound_webhook import router as inbound_router
from sms.metrics_tracker import update_metrics
import os

app = FastAPI()
CRON_TOKEN = os.getenv("CRON_TOKEN")

app.include_router(inbound_router)

def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.post("/send")
async def send_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = send_batch()
    return result

@app.post("/metrics")
async def metrics_endpoint(x_cron_token: str | None = Header(None)):
    check_token(x_cron_token)
    result = update_metrics()
    return result

@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = "Unprocessed Inbounds",
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    result = run_autoresponder(limit=limit, view=view)
    return result