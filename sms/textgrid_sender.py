# sms/textgrid_sender.py
import os
import httpx

TEXTGRID_API_KEY = os.getenv("TEXTGRID_API_KEY")
TEXTGRID_CAMPAIGN_ID = os.getenv("TEXTGRID_CAMPAIGN_ID")

BASE_URL = "https://api.textgrid.com/v1/messages"

def send_message(to: str, body: str) -> dict:
    """Send a single SMS message via TextGrid API."""
    if not TEXTGRID_API_KEY or not TEXTGRID_CAMPAIGN_ID:
        raise RuntimeError("❌ TEXTGRID_API_KEY or TEXTGRID_CAMPAIGN_ID not set")

    payload = {
        "to": to,
        "campaign_id": TEXTGRID_CAMPAIGN_ID,
        "body": body
    }
    headers = {
        "Authorization": f"Bearer {TEXTGRID_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        resp = httpx.post(BASE_URL, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        print(f"📤 Sent SMS → {to}: {body}")
        return resp.json()
    except Exception as e:
        print(f"❌ Failed to send SMS to {to}: {e}")
        return {"error": str(e), "to": to, "body": body}