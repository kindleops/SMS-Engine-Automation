import os
import time
import httpx
from datetime import datetime, timezone
from pyairtable import Table

# import your pool logic
from sms.pools import get_from_number   # <-- add this

# --- Env Config ---
ACCOUNT_SID        = os.getenv("TEXTGRID_ACCOUNT_SID")
AUTH_TOKEN         = os.getenv("TEXTGRID_AUTH_TOKEN")

AIRTABLE_API_KEY   = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE  = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# --- Field Mappings ---
FROM_FIELD   = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD    = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD  = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT      = os.getenv("CONV_SENT_AT_FIELD", "sent_at")

# --- Base URL (Breeze API) ---
BASE_URL = f"https://api.textgrid.com/2010-04-01/Accounts/bsmP3M1uzUswpnoGjA8Q2w==/Messages.json"


def send_message(to: str, body: str, from_number: str | None = None, market: str | None = None, retries: int = 3) -> dict:
    """
    Send SMS via TextGrid Breeze API and log into Airtable Conversations.
    - Selects from_number automatically from pools.py if not provided
    """
    if not ACCOUNT_SID or not AUTH_TOKEN:
        raise RuntimeError("❌ TEXTGRID_ACCOUNT_SID or TEXTGRID_AUTH_TOKEN not set")

    # pick number from pool if none passed
    sender = from_number or get_from_number(market=market)

    payload = {
        "To": to,
        "From": sender,
        "Body": body,
    }

    attempt = 0
    while attempt < retries:
        try:
            resp = httpx.post(
                BASE_URL,
                data=payload,
                auth=(ACCOUNT_SID, AUTH_TOKEN),  # Breeze = Basic Auth
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            msg_id = data.get("sid")

            print(f"📤 Sent SMS → {to} (From {sender}): {body}")

            # Airtable log
            if AIRTABLE_API_KEY and LEADS_CONVOS_BASE:
                try:
                    convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
                    convos.create({
                        FROM_FIELD: sender,
                        TO_FIELD: to,
                        MSG_FIELD: body,
                        STATUS_FIELD: "SENT",
                        DIR_FIELD: "OUT",
                        TG_ID_FIELD: msg_id,
                        SENT_AT: datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as log_err:
                    print(f"⚠️ Failed to log outbound SMS to Airtable: {log_err}")

            return data

        except Exception as e:
            attempt += 1
            wait_time = 2 ** attempt
            print(f"❌ Attempt {attempt} failed for {to}: {e}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"🚨 Giving up on {to} after {retries} attempts")
                return {"error": str(e), "to": to, "body": body, "attempts": retries}