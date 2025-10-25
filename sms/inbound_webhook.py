# sms/inbound_webhook.py
"""
Inbound SMS + Opt-Out Webhook (schema-aware)
--------------------------------------------
• Accepts TextGrid/Twilio-style JSON or form payloads
• Uses datastore CONNECTOR helpers (schema safe)
• Exports normalize_e164 (used by delivery webhook)
• Marks Conversations + Lead activity consistently
"""

from __future__ import annotations
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request, Header, Query

from sms.runtime import get_logger, iso_now
from sms.datastore import (
    CONNECTOR,
    create_conversation,        # create_conversation(unique_key, fields) — schema-safe
    ensure_prospect_or_lead,    # ensure + return (lead_rec, prospect_rec)
    update_conversation,        # update by record id with fields
    update_record,              # generic table update
    touch_lead,                 # update last-activity trails
    log_message,
)
from sms.airtable_schema import (
    ConversationDirection,
    conversations_field_map,
    leads_field_map,
)

# Optional counters
try:
    from sms.number_pools import increment_delivered, increment_opt_out
except Exception:
    increment_delivered = None  # type: ignore
    increment_opt_out = None    # type: ignore

router = APIRouter(prefix="/inbound", tags=["Inbound"])
logger = get_logger("inbound")

# ---------------------------------------------------------------------------
# Config / Schema maps (avoid hard-coded Airtable column names)
# ---------------------------------------------------------------------------

WEBHOOK_TOKEN = (
    __import__("os").getenv("WEBHOOK_TOKEN")
    or __import__("os").getenv("CRON_TOKEN")
    or None
)

CONV = conversations_field_map()
LEAD = leads_field_map()

CONV_FROM_FIELD       = CONV.get("FROM", "Seller Phone Number")
CONV_TO_FIELD         = CONV.get("TO", "TextGrid Phone Number")
CONV_BODY_FIELD       = CONV.get("BODY", "Message")
CONV_STATUS_FIELD     = CONV.get("STATUS", "Status")
CONV_DIRECTION_FIELD  = CONV.get("DIRECTION", "Direction")
CONV_RECEIVED_AT      = CONV.get("RECEIVED_AT", "Received At")
CONV_PROCESSED_BY     = CONV.get("PROCESSED_BY", "Processed By")
CONV_TEXTGRID_ID      = CONV.get("TEXTGRID_ID", "TextGrid ID")
CONV_LEAD_LINK        = CONV.get("LEAD_LINK", "Lead")
CONV_PROSPECT_LINK    = CONV.get("PROSPECT_LINK", "Prospect")

LEAD_STATUS_FIELD     = LEAD.get("STATUS", "Status")
LEAD_LAST_MESSAGE     = LEAD.get("LAST_MESSAGE", "Last Message")
LEAD_LAST_DIRECTION   = LEAD.get("LAST_DIRECTION", "Last Direction")
LEAD_LAST_ACTIVITY    = LEAD.get("LAST_ACTIVITY", "Last Activity")

STOP_TERMS = {"stop", "unsubscribe", "remove", "opt out", "opt-out", "optout", "quit", "cancel"}

# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _digits(v: str | None) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())

def normalize_e164(v: str | None, *, field: str = "Phone") -> str:
    """Exported for other modules (e.g., delivery webhook)."""
    d = _digits(v)
    if len(d) == 10:
        return "+1" + d
    if len(d) == 11 and d.startswith("1"):
        return "+" + d
    if v and str(v).startswith("+") and d:
        return "+" + d
    raise HTTPException(status_code=422, detail=f"Invalid {field}")

def _sanitize_body(body: Any) -> str:
    txt = str(body or "").strip()
    if not txt:
        raise HTTPException(status_code=422, detail="Empty Body")
    return txt

def _is_stop(msg: str) -> bool:
    return any(t in " ".join(msg.lower().split()) for t in STOP_TERMS)

def _is_authorized(header_token: Optional[str], query_token: Optional[str]) -> bool:
    if not WEBHOOK_TOKEN:
        return True
    return (header_token == WEBHOOK_TOKEN) or (query_token == WEBHOOK_TOKEN)

# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------

async def _parse_payload(request: Request) -> Dict[str, Any]:
    """Parse inbound request to a dict (JSON or Form), case-normalized."""
    ct = request.headers.get("content-type", "").lower()
    if "application/json" in ct:
        try:
            data = await request.json()
        except Exception:
            # Some providers send invalid JSON with correct header; fall back to form
            data = dict(await request.form())
    else:
        data = dict(await request.form())

    # Case-insensitive access
    lower = {str(k).lower(): v for k, v in (data or {}).items()}

    def pick(*keys: str) -> Optional[str]:
        for k in keys:
            v = lower.get(k.lower())
            if v not in (None, ""):
                return str(v)
        return None

    return {
        "from": pick("From", "from", "sender"),
        "to": pick("To", "to", "recipient", "destination"),
        "body": pick("Body", "body", "message", "text"),
        "sid": pick("MessageSid", "messagesid", "sid", "id", "messageid", "TextGridId", "textgridid"),
        "raw": data,
    }

# ---------------------------------------------------------------------------
# Core writers
# ---------------------------------------------------------------------------

def _log_conversation_inbound(
    *,
    from_e164: str,
    to_e164: Optional[str],
    body: str,
    sid: Optional[str],
    lead_id: Optional[str],
    prospect_id: Optional[str],
    status: str = "DELIVERED",
    processed_by: Optional[str] = None,   # IMPORTANT: keep None for normal inbound so AR can process
):
    """Create a Conversations row using schema-safe field names."""
    fields: Dict[str, Any] = {
        CONV_FROM_FIELD: from_e164,
        CONV_TO_FIELD: to_e164,
        CONV_BODY_FIELD: body,
        CONV_DIRECTION_FIELD: ConversationDirection.INBOUND.value,
        CONV_STATUS_FIELD: status,
        CONV_RECEIVED_AT: iso_now(),
    }
    if processed_by:  # only set when we intentionally want to mark ownership (e.g., Opt-Out)
        fields[CONV_PROCESSED_BY] = processed_by
    if sid:
        fields[CONV_TEXTGRID_ID] = sid
    if lead_id:
        fields[CONV_LEAD_LINK] = [lead_id]
    if prospect_id:
        fields[CONV_PROSPECT_LINK] = [prospect_id]

    # create_conversation will upsert by unique key (sid) when provided
    return create_conversation(sid, fields)

def _touch_lead_safe(lead_id: Optional[str], body: str):
    if not lead_id:
        return
    try:
        touch_lead(
            lead_id,
            body=body,
            direction=ConversationDirection.INBOUND.value,
        )
    except Exception:
        logger.warning("Lead touch failed", exc_info=True)

# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _handle_inbound(data: Dict[str, Any]) -> Dict[str, Any]:
    from_e164 = normalize_e164(data.get("from"), field="From")
    to_e164 = normalize_e164(data.get("to"), field="To") if data.get("to") else None
    body = _sanitize_body(data.get("body"))
    sid = data.get("sid")

    # STOP / opt-out → special flow
    if _is_stop(body):
        return _handle_optout(data)

    lead, prospect = ensure_prospect_or_lead(from_e164)
    lead_id = (lead or {}).get("id")
    prospect_id = (prospect or {}).get("id")

    record = _log_conversation_inbound(
        from_e164=from_e164,
        to_e164=to_e164,
        body=body,
        sid=sid,
        lead_id=lead_id,
        prospect_id=prospect_id,
        status="DELIVERED",
        processed_by=None,  # leave blank so the Autoresponder can pick it up
    )
    try:
        log_message(
            CONNECTOR,
            conversation_id=(record or {}).get("id"),
            direction="INBOUND",
            to_phone=to_e164,
            from_phone=from_e164,
            body=body,
            status="RECEIVED",
            provider_sid=sid,
            provider_error=None,
        )
    except Exception:
        logger.warning("Inbound message logging failed", exc_info=True)
    _touch_lead_safe(lead_id, body)

    if to_e164 and increment_delivered:
        try:
            increment_delivered(to_e164)
        except Exception:
            logger.warning("Increment delivered failed", exc_info=True)

    return {
        "status": "ok",
        "conversation_id": (record or {}).get("id"),
        "message_sid": sid,
        "linked": "lead" if lead_id else "prospect",
    }

def _handle_optout(data: Dict[str, Any]) -> Dict[str, Any]:
    from_e164 = normalize_e164(data.get("from"), field="From")
    to_e164 = normalize_e164(data.get("to"), field="To") if data.get("to") else None
    body = _sanitize_body(data.get("body"))
    sid = data.get("sid")

    if to_e164 and increment_opt_out:
        try:
            increment_opt_out(to_e164)
        except Exception:
            logger.warning("Increment opt-out failed", exc_info=True)

    lead, prospect = ensure_prospect_or_lead(from_e164)
    lead_id = (lead or {}).get("id")
    prospect_id = (prospect or {}).get("id")

    # Conversations: mark as OPT OUT (consistent with delivery webhook + reports)
    record = _log_conversation_inbound(
        from_e164=from_e164,
        to_e164=to_e164,
        body=body,
        sid=sid,
        lead_id=lead_id,
        prospect_id=prospect_id,
        status="OPT OUT",
        processed_by="Opt-Out Webhook",
    )

    # Leads: mark DNC (pipeline label), and update activity
    if lead_id:
        try:
            update_record(CONNECTOR.leads(), lead_id, {LEAD_STATUS_FIELD: "DNC"})
        except Exception:
            logger.warning("Lead DNC update failed", exc_info=True)
        _touch_lead_safe(lead_id, body)

    # Ensure the created convo row is flagged consistently (best-effort)
    if record and record.get("id"):
        try:
            update_conversation(record["id"], {CONV_STATUS_FIELD: "OPT OUT", CONV_PROCESSED_BY: "Opt-Out Webhook"})
        except Exception:
            logger.warning("Conversation OPT OUT flag failed", exc_info=True)

    return {
        "status": "optout",
        "conversation_id": (record or {}).get("id"),
        "message_sid": sid,
        "linked": "lead" if lead_id else "prospect",
    }

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("")
async def inbound_entry(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    token: Optional[str] = Query(None),
):
    """Unified webhook for inbound + opt-out detection (JSON or form)."""
    if not _is_authorized(x_webhook_token, token):
        raise HTTPException(status_code=401, detail="Unauthorized")

    data = await _parse_payload(request)
    try:
        result = _handle_inbound(data)
        logger.info("📥 Inbound → %s", result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Inbound failure")
        raise HTTPException(status_code=500, detail=str(e))

# Optional trailing slash for providers that insist on it
@router.post("/")
async def inbound_entry_slash(
    request: Request,
    x_webhook_token: Optional[str] = Header(None, convert_underscores=False),
    token: Optional[str] = Query(None),
):
    return await inbound_entry(request, x_webhook_token, token)
