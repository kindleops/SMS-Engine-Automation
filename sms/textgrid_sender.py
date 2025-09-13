import os
import httpx
import time
from datetime import datetime, timezone
from pyairtable import Table

# --- Env Config ---
TEXTGRID_API_KEY = os.getenv("TEXTGRID_API_KEY")
TEXTGRID_CAMPAIGN_ID = os.getenv("TEXTGRID_CAMPAIGN_ID")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

BASE_URL = "https://api.textgrid.com/v1/messages"


def send_message(to: str, body: str, from_number: str | None = None, retries: int = 3) -> dict:
    """
    Send a single SMS message via TextGrid API.
    Retries on transient failures. Logs outbound into Airtable Conversations.
    """
    if not TEXTGRID_API_KEY or not TEXTGRID_CAMPAIGN_ID:
        raise RuntimeError("❌ TEXTGRID_API_KEY or TEXTGRID_CAMPAIGN_ID not set")

    payload = {
        "to": to,
        "campaign_id": TEXTGRID_CAMPAIGN_ID,
        "body": body
    }
    if from_number:
        payload["from"] = from_number

    headers = {
        "Authorization": f"Bearer {TEXTGRID_API_KEY}",
        "Content-Type": "application/json"
    }

    attempt = 0
    while attempt < retries:
        try:
            resp = httpx.post(BASE_URL, json=payload, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            msg_id = data.get("id")

            print(f"📤 Sent SMS → {to}: {body}")

            # Log to Airtable Conversations
            if AIRTABLE_API_KEY and LEADS_CONVOS_BASE:
                try:
                    convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
                    convos.create({
                        "phone": to,
                        "from_number": from_number or "TEXTGRID",
                        "message": body,
                        "status": "SENT",
                        "direction": "OUT",
                        "textgrid_id": msg_id,
                        "sent_at": datetime.now(timezone.utc).isoformat()
                    })
                except Exception as log_err:
                    print("⚠️ Failed to log outbound to Airtable:", log_err)

            return data

        except Exception as e:
            attempt += 1
            wait_time = 2 ** attempt  # exponential backoff
            print(f"❌ Attempt {attempt} failed to send SMS to {to}: {e}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                return {"error": str(e), "to": to, "body": body, "attempts": retries}