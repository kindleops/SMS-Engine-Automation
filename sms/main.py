from fastapi import FastAPI, Header, HTTPException, Query
from sms.inbound_webhook import router as inbound_router
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
import os

app = FastAPI()

CRON_TOKEN = os.getenv("CRON_TOKEN")

def check_token(x_cron_token: str | None):
    if CRON_TOKEN and x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

# Include inbound webhook
app.include_router(inbound_router)

@app.post("/send")
async def send_endpoint(
    limit: int = 200,
    view: str = Query(default="Send View – Sniper 750"),
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    send_batch(limit=limit, view=view)
    return {"status": f"✅ Outbound batch sent from {view}"}

@app.post("/autoresponder")
async def autoresponder_endpoint(
    limit: int = 50,
    view: str = Query(default="Unprocessed Inbounds"),
    x_cron_token: str | None = Header(None)
):
    check_token(x_cron_token)
    run_autoresponder(limit=limit, view=view)
    return {"status": f"🤖 Autoresponder ran on {view}"}