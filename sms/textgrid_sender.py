import os
import time
import httpx
from datetime import datetime, timezone
from pyairtable import Table

# --- Env Config ---
TEXTGRID_API_KEY     = os.getenv("TEXTGRID_API_KEY")
TEXTGRID_CAMPAIGN_ID = os.getenv("TEXTGRID_CAMPAIGN_ID")

AIRTABLE_API_KEY     = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE    = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE  = os.getenv("CONVERSATIONS_TABLE", "Conversations")

# --- Field Mappings ---
FROM_FIELD           = os.getenv("CONV_FROM_FIELD", "phone")
TO_FIELD             = os.getenv("CONV_TO_FIELD", "to_number")
MSG_FIELD            = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD         = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD            = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD          = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT              = os.getenv("CONV_SENT_AT_FIELD", "sent_at")

# Retry metadata
RETRY_COUNT_FIELD    = os.getenv("CONV_RETRY_COUNT_FIELD", "retry_count")
RETRY_AFTER_FIELD    = os.getenv("CONV_RETRY_AFTER_FIELD", "retry_after")
LAST_ERROR_FIELD     = os.getenv("CONV_LAST_ERROR_FIELD", "last_retry_error")

# --- API Base ---
BASE_URL = "https://api.textgrid.com/v1/messages"


def send_message(to: str, body: str, from_number: str | None = None, retries: int = 3) -> dict:
    """
    Send SMS via TextGrid and log into Airtable Conversations.
    - Retries with exponential backoff (2s, 4s, 8s…)
    - Logs outbound message with retry metadata for retry workers
    """
    if not TEXTGRID_API_KEY or not TEXTGRID_CAMPAIGN_ID:
        raise RuntimeError("❌ TEXTGRID_API_KEY or TEXTGRID_CAMPAIGN_ID not set")

    payload = {"to": to, "campaign_id": TEXTGRID_CAMPAIGN_ID, "body": body}
    if from_number:
        payload["from"] = from_number

    headers = {"Authorization": f"Bearer {TEXTGRID_API_KEY}", "Content-Type": "application/json"}

    attempt = 0
    while attempt < retries:
        try:
            resp = httpx.post(BASE_URL, json=payload, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            msg_id = data.get("id")

            print(f"📤 Sent SMS → {to}: {body}")

            # Airtable success log
            if AIRTABLE_API_KEY and LEADS_CONVOS_BASE:
                try:
                    convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
                    convos.create({
                        FROM_FIELD: from_number or "TEXTGRID",
                        TO_FIELD: to,
                        MSG_FIELD: body,
                        STATUS_FIELD: "SENT",
                        DIR_FIELD: "OUT",
                        TG_ID_FIELD: msg_id,
                        SENT_AT: datetime.now(timezone.utc).isoformat(),
                        RETRY_COUNT_FIELD: 0,
                    })
                except Exception as log_err:
                    print(f"⚠️ Airtable log failed for {to}: {log_err}")

            return data

        except Exception as e:
            attempt += 1
            wait_time = 2 ** attempt
            err_msg = str(e)
            print(f"❌ Attempt {attempt} failed for {to}: {err_msg}")

            # Airtable failure log
            if AIRTABLE_API_KEY and LEADS_CONVOS_BASE:
                try:
                    convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
                    convos.create({
                        FROM_FIELD: from_number or "TEXTGRID",
                        TO_FIELD: to,
                        MSG_FIELD: body,
                        STATUS_FIELD: "FAILED" if attempt == retries else "RETRY",
                        DIR_FIELD: "OUT",
                        LAST_ERROR_FIELD: err_msg,
                        RETRY_COUNT_FIELD: attempt,
                        RETRY_AFTER_FIELD: (datetime.now(timezone.utc) + timezone.utc.utcoffset(datetime.now()))
                            if attempt == retries else (datetime.now(timezone.utc).isoformat()),
                        SENT_AT: datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as log_err:
                    print(f"⚠️ Airtable failure log failed for {to}: {log_err}")

            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"🚨 Giving up on {to} after {retries} attempts")
                return {"error": err_msg, "to": to, "body": body, "attempts": retries}