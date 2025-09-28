# sms/webhooks/delivery.py
from fastapi import APIRouter, Request
from sms.number_pools import increment_delivered, increment_failed
from datetime import datetime, timezone
import traceback

router = APIRouter(prefix="/delivery", tags=["Delivery"])


@router.post("")
async def delivery_webhook(request: Request):
    """
    Handle delivery receipts from TextGrid/Telco.
    Increments number pool stats and returns ack.
    """
    try:
        payload = await request.json()
    except Exception:
        traceback.print_exc()
        return {"ok": False, "error": "invalid JSON"}

    number = payload.get("from") or payload.get("From")
    status = (payload.get("status") or "").lower()
    msg_id = payload.get("sid") or payload.get("message_sid")

    if not number:
        return {"ok": False, "error": "missing from number"}

    ts = datetime.now(timezone.utc).isoformat()
    print(f"📡 Delivery receipt | {ts} | {number} → {status} | SID={msg_id}")

    if status == "delivered":
        increment_delivered(number)
    elif status in {"failed", "undeliverable"}:
        increment_failed(number)
    else:
        print(f"⚠️ Unhandled delivery status: {status}")

    return {"ok": True, "status": status, "sid": msg_id}
