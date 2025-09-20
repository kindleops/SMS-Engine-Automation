from fastapi import APIRouter, Request
from sms.number_pools import increment_delivered, increment_failed

router = APIRouter(prefix="/delivery", tags=["Delivery"])

@router.post("")
async def delivery_webhook(request: Request):
    payload = await request.json()
    number = payload.get("from") or payload.get("From")
    status = payload.get("status", "").lower()

    if not number:
        return {"ok": False, "error": "missing from number"}

    if status == "delivered":
        increment_delivered(number)
    elif status in ["failed", "undeliverable"]:
        increment_failed(number)

    return {"ok": True, "status": status}