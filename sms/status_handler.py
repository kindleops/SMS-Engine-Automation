# sms/status_handler.py
import os
from fastapi import APIRouter, Request
from pyairtable import Table

router = APIRouter()

# --- Airtable Config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID          = os.getenv("LEADS_CONVOS_BASE")
TEMPLATES_TABLE  = os.getenv("TEMPLATES_TABLE", "Templates")

templates = None
if AIRTABLE_API_KEY and BASE_ID:
    try:
        templates = Table(AIRTABLE_API_KEY, BASE_ID, TEMPLATES_TABLE)
    except Exception as e:
        print(f"⚠️ Failed to init Templates table: {e}")
else:
    print("⚠️ No Airtable config detected → using MOCK Templates table")

# --- KPI Logger ---
def log_template_kpi(template_id: str, event: str):
    """Increment KPI counters on a template record."""
    if not template_id or not templates:
        print(f"[MOCK] log_template_kpi({template_id}, {event})")
        return

    updates = {}
    if event == "delivered":
        updates["Delivered"] = {"increment": 1}
    elif event == "failed":
        updates["Failed Deliveries"] = {"increment": 1}

    if updates:
        try:
            templates.update(template_id, updates)
            print(f"📊 Template {template_id} KPI updated → {event}")
        except Exception as e:
            print(f"⚠️ Failed to update delivery KPI: {e}")

# --- Delivery Status Endpoint ---
@router.post("/status")
async def delivery_status(req: Request):
    """
    Webhook from TextGrid/carrier.
    Expected payload:
      {
        "sid": "SM123...",
        "status": "delivered|failed|undeliverable",
        "template_id": "recXXXX..."
      }
    """
    try:
        data = await req.json()
    except Exception:
        data = {}

    sid         = data.get("sid")
    status      = (data.get("status") or "").lower()
    template_id = data.get("template_id")

    print(f"📡 Delivery status update for {sid or 'unknown SID'} → {status or 'unknown'}")

    if "delivered" in status:
        log_template_kpi(template_id, "delivered")
    elif "failed" in status or "undeliverable" in status:
        log_template_kpi(template_id, "failed")

    return {"ok": True}