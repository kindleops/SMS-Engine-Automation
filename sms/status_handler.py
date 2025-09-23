import os
from fastapi import APIRouter, Request
from pyairtable import Table

# Airtable Templates table
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID          = os.getenv("LEADS_CONVOS_BASE")
TEMPLATES_TABLE  = os.getenv("TEMPLATES_TABLE", "Templates")

templates = Table(AIRTABLE_API_KEY, BASE_ID, TEMPLATES_TABLE)

router = APIRouter()

# --- KPI Logger (shared with autoresponder) ---
def log_template_kpi(template_id: str, event: str):
    if not template_id:
        return
    updates = {}
    if event == "delivered":
        updates["Delivered"] = {"increment": 1}
    elif event == "failed":
        updates["Failed Deliveries"] = {"increment": 1}
    try:
        templates.update(template_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update delivery KPI: {e}")


# --- Delivery Status Endpoint ---
@router.post("/status")
async def delivery_status(req: Request):
    """
    Webhook from TextGrid / carrier.
    Should include: sid, status, template_id
    """
    data = await req.json()
    sid         = data.get("sid")
    status      = data.get("status", "").lower()
    template_id = data.get("template_id")

    print(f"📡 Delivery status update for {sid}: {status}")

    if "delivered" in status:
        log_template_kpi(template_id, "delivered")
    elif "failed" in status or "undeliverable" in status:
        log_template_kpi(template_id, "failed")

    return {"ok": True}