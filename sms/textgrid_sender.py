"""Thin TextGrid transport integrated with the schema-aware datastore."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# HTTP Client setup
# ---------------------------------------------------------------------------

try:  # Prefer httpx (async-safe)
    import httpx
except ImportError:
    httpx = None  # type: ignore

try:
    import requests
except ImportError:
    requests = None  # type: ignore

# ---------------------------------------------------------------------------
# Local imports
# ---------------------------------------------------------------------------

from sms.airtable_schema import (
    CONVERSATIONS_TABLE,
    ConversationDeliveryStatus,
    ConversationDirection,
    ConversationProcessor,
    conversations_field_map,
)
from sms.datastore import (
    CONNECTOR,
    create_conversation,
    promote_to_lead,
    touch_lead,
)
from sms.number_pools import get_from_number
from sms.runtime import get_logger, iso_now

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

logger = get_logger(__name__)

ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")
TEXTGRID_CAMPAIGN_ID = os.getenv("TEXTGRID_CAMPAIGN_ID")
BASE_URL = (
    f"https://api.textgrid.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json"
    if ACCOUNT_SID
    else None
)

DEFAULT_PROCESSED_BY = os.getenv(
    "TEXTGRID_PROCESSED_BY_LABEL",
    ConversationProcessor.CAMPAIGN_RUNNER.value,
)
DRY_RUN = os.getenv("TEXTGRID_DRY_RUN", "0").lower() in {"1", "true", "yes"}

# Airtable field mapping
CONV_FIELDS = conversations_field_map()
CONV_FROM_FIELD = CONV_FIELDS["FROM"]              # Seller Phone Number
CONV_TO_FIELD = CONV_FIELDS["TO"]                  # TextGrid Phone Number
CONV_BODY_FIELD = CONV_FIELDS["BODY"]
CONV_DIRECTION_FIELD = CONV_FIELDS["DIRECTION"]
CONV_STATUS_FIELD = CONV_FIELDS["STATUS"]
CONV_SENT_AT_FIELD = CONV_FIELDS["SENT_AT"]
CONV_TEXTGRID_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
CONV_PROCESSED_BY_FIELD = CONV_FIELDS["PROCESSED_BY"]

CONV_FIELD_NAMES = CONVERSATIONS_TABLE.field_names()
CONV_LEAD_LINK_FIELD = CONV_FIELD_NAMES.get("LEAD_LINK", "Lead")
CONV_TEMPLATE_LINK_FIELD = CONV_FIELD_NAMES.get("TEMPLATE_LINK", "Template")
CONV_CAMPAIGN_LINK_FIELD = CONV_FIELD_NAMES.get("CAMPAIGN_LINK", "Campaign")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class TextGridError(Exception):
    """Raised when TextGrid returns a non-success response."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _http_post(url: str, data: Dict[str, Any], auth: Tuple[str, str], timeout: int) -> Dict[str, Any]:
    """HTTP POST wrapper supporting both httpx and requests."""
    if DRY_RUN:
        logger.info("[DRY RUN] POST %s → %s", url, {k: v for k, v in data.items() if k != "Body"})
        return {"sid": "DRY-RUN", "status": "sent"}

    if httpx:
        with httpx.Client(timeout=timeout) as client:
            response = client.post(url, data=data, auth=auth)
            if response.status_code >= 400:
                raise TextGridError(f"HTTP {response.status_code}: {response.text}")
            return response.json()
    elif requests:
        response = requests.post(url, data=data, auth=auth, timeout=timeout)
        if response.status_code >= 400:
            raise TextGridError(f"HTTP {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError as exc:
            raise TextGridError(f"Invalid JSON response: {response.text}") from exc
    else:
        raise TextGridError("No HTTP client available (install httpx or requests)")


def _send_with_retry(url: str, data: Dict[str, Any], timeout: int, retries: int) -> Dict[str, Any]:
    """Retry wrapper for transient network issues."""
    auth = (ACCOUNT_SID or "", AUTH_TOKEN or "")
    for attempt in range(1, retries + 2):
        try:
            return _http_post(url, data=data, auth=auth, timeout=timeout)
        except TextGridError as exc:
            if attempt > retries:
                raise
            wait = 2 ** attempt
            logger.warning(
                "TextGrid send failed (attempt %d/%d): %s; retrying in %ds",
                attempt, retries, exc, wait
            )
            time.sleep(wait)


def _log_conversation(
    seller_phone: str,
    did: str,
    body: str,
    *,
    message_sid: Optional[str],
    lead_id: Optional[str],
    template_id: Optional[str],
    campaign_id: Optional[str],
    status: str,
) -> Optional[str]:
    """Log outbound message to Airtable Conversations."""
    payload = {
        CONV_FROM_FIELD: seller_phone,
        CONV_TO_FIELD: did,
        CONV_BODY_FIELD: body,
        CONV_DIRECTION_FIELD: ConversationDirection.OUTBOUND.value,
        CONV_STATUS_FIELD: status,
        CONV_SENT_AT_FIELD: iso_now(),
        CONV_PROCESSED_BY_FIELD: DEFAULT_PROCESSED_BY,
    }
    if message_sid:
        payload[CONV_TEXTGRID_ID_FIELD] = message_sid
    if lead_id:
        payload[CONV_LEAD_LINK_FIELD] = [lead_id]
    if template_id:
        payload[CONV_TEMPLATE_LINK_FIELD] = [template_id]
    if campaign_id:
        payload[CONV_CAMPAIGN_LINK_FIELD] = [campaign_id]

    try:
        record = create_conversation(message_sid, payload)
        return (record or {}).get("id")
    except Exception as exc:
        logger.exception("Failed to log conversation to Airtable: %s", exc)
        return None


def _ensure_did(from_number: Optional[str], market: Optional[str]) -> str:
    """Resolve a valid sending number (DID)."""
    if from_number:
        return from_number
    if get_from_number:
        try:
            return get_from_number(market or "")  # type: ignore[return-value]
        except Exception as exc:
            logger.warning("Number pool lookup failed: %s", exc)
    raise TextGridError("No from_number available for TextGrid send")


# ---------------------------------------------------------------------------
# Main send_message function
# ---------------------------------------------------------------------------

def send_message(
    to: str,  # Seller Phone Number
    body: str,
    from_number: Optional[str] = None,  # TextGrid DID
    market: Optional[str] = None,
    lead_id: Optional[str] = None,
    property_id: Optional[str] = None,
    template_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    media_urls: Optional[List[str]] = None,
    retries: int = 3,
    timeout: int = 10,
) -> Dict[str, Any]:
    """
    Send an SMS/MMS via TextGrid and log it properly (From=TextGrid DID, To=Seller).
    """

    # Input validation
    if not to or not body:
        return {"status": "failed", "error": "missing to/body", "to": to, "from": from_number}

    if not str(to).startswith("+1"):
        logger.warning("Skipping non-US number: %s", to)
        return {"status": "skipped", "error": "non_us_number", "to": to, "from": from_number}

    if not (ACCOUNT_SID and AUTH_TOKEN and BASE_URL):
        return {"status": "failed", "error": "TextGrid credentials not configured", "to": to, "from": from_number}

    sender = _ensure_did(from_number, market)
    if not lead_id:
        lead_id, property_id = promote_to_lead(to, source=DEFAULT_PROCESSED_BY, conversation_fields=None)

    # ✅ Correct TextGrid API payload
    data: Dict[str, Any] = {
        "To": to,
        "From": sender,
        "Body": body,
        "CampaignId": campaign_id or TEXTGRID_CAMPAIGN_ID,
    }

    if media_urls:
        for i, url in enumerate(media_urls):
            key = "MediaUrl" if i == 0 else f"MediaUrl{i+1}"
            data[key] = url

    logger.info("📤 TextGrid SEND DEBUG → %s", {k: v for k, v in data.items() if k != "Body"})
    print("📨 TEXTGRID PAYLOAD:", data)

    try:
        response = _send_with_retry(BASE_URL, data=data, timeout=timeout, retries=retries)
        sid = response.get("sid") or response.get("message_sid") or response.get("id")
        print("📩 TEXTGRID RESPONSE:", response)

        convo_id = _log_conversation(
            seller_phone=to,
            did=sender,
            body=body,
            message_sid=sid,
            lead_id=lead_id,
            template_id=template_id,
            campaign_id=campaign_id,
            status=ConversationDeliveryStatus.SENT.value,
        )

        if lead_id:
            touch_lead(
                lead_id,
                body=body,
                direction=ConversationDirection.OUTBOUND.value,
                status=ConversationDeliveryStatus.SENT.value,
            )

        return {
            "status": "sent",
            "sid": sid,
            "to": to,
            "from": sender,
            "conversation_id": convo_id,
        }

    except Exception as exc:
        logger.exception("TextGrid send failed for %s", to)
        convo_id = _log_conversation(
            seller_phone=to,
            did=sender,
            body=body,
            message_sid=None,
            lead_id=lead_id,
            template_id=template_id,
            campaign_id=campaign_id,
            status=ConversationDeliveryStatus.FAILED.value,
        )
        if lead_id:
            touch_lead(
                lead_id,
                body=body,
                direction=ConversationDirection.OUTBOUND.value,
                status=ConversationDeliveryStatus.FAILED.value,
            )
        return {
            "status": "failed",
            "error": str(exc),
            "to": to,
            "from": sender,
            "conversation_id": convo_id,
        }
