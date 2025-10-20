# sms/textgrid_sender.py
from __future__ import annotations

import os
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

# --- HTTP client (prefer httpx, fallback to requests) ---
try:
    import httpx  # type: ignore
except Exception:
    httpx = None
try:
    import requests  # type: ignore
except Exception:
    requests = None

# --- Optional: Airtable formula helper for upserts ---
try:
    from pyairtable.formulas import match  # type: ignore
except Exception:
    match = None  # graceful fallback

# --- Pool DID selector ---
try:
    from sms.number_pools import get_from_number
except Exception:
    get_from_number = None

# --- Lazy Airtable table getters (wrappers you already added elsewhere) ---
try:
    from sms.tables import get_convos, get_leads
except Exception:
    get_convos = get_leads = lambda *a, **k: None  # type: ignore

<<<<<<< HEAD
=======
# Centralized Airtable field map + enums
from sms.config import CONV_FIELDS, CONVERSATIONS_FIELDS, LEAD_FIELDS
from sms.airtable_schema import (
    ConversationDirection,
    ConversationDeliveryStatus,
    ConversationProcessor,
    LeadStatus,
)
>>>>>>> codex-refactor-test

# =========================
# ENV / CONFIG
# =========================
ACCOUNT_SID = os.getenv("TEXTGRID_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TEXTGRID_AUTH_TOKEN")
BASE_URL = f"https://api.textgrid.com/2010-04-01/Accounts/{ACCOUNT_SID}/Messages.json" if ACCOUNT_SID else None

# Conversations field mapping (defaults align with your base)
<<<<<<< HEAD
FROM_FIELD = os.getenv("CONV_FROM_FIELD", "phone")          # counterparty phone (recipient)
TO_FIELD = os.getenv("CONV_TO_FIELD", "to_number")          # our DID used to send
MSG_FIELD = os.getenv("CONV_MESSAGE_FIELD", "message")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "status")
DIR_FIELD = os.getenv("CONV_DIRECTION_FIELD", "direction")
TG_ID_FIELD = os.getenv("CONV_TEXTGRID_ID_FIELD", "TextGrid ID")
SENT_AT_FIELD = os.getenv("CONV_SENT_AT_FIELD", "sent_at")
PROCESSED_BY = os.getenv("CONV_PROCESSED_BY_FIELD", "processed_by")

# Optional extras (will only write if fields exist)
LEAD_LINK_FIELD = os.getenv("CONV_LEAD_LINK_FIELD", "lead_id")         # often "Lead"
PROPERTY_ID_FIELD = os.getenv("CONV_PROPERTY_ID_FIELD", "Property ID") # link or text
TEMPLATE_LINK_FLD = os.getenv("CONV_TEMPLATE_LINK_FIELD", "Template")  # linked
CAMPAIGN_LINK_FLD = os.getenv("CONV_CAMPAIGN_LINK_FIELD", "Campaign")  # linked
=======
FROM_FIELD = CONV_FIELDS["FROM"]  # counterparty phone (recipient)
TO_FIELD = CONV_FIELDS["TO"]  # our DID used to send
MSG_FIELD = CONV_FIELDS["BODY"]
STATUS_FIELD = CONV_FIELDS["STATUS"]
DIR_FIELD = CONV_FIELDS["DIRECTION"]
TG_ID_FIELD = CONV_FIELDS["TEXTGRID_ID"]
SENT_AT_FIELD = CONV_FIELDS["SENT_AT"]
PROCESSED_BY_FIELD = CONV_FIELDS["PROCESSED_BY"]

# Optional extras we’ll write only if fields exist
LEAD_LINK_FIELD = CONVERSATIONS_FIELDS.get("LEAD_LINK", "Lead")
PROPERTY_ID_FIELD = CONVERSATIONS_FIELDS.get("PROPERTY_ID", "Property Record ID")
TEMPLATE_LINK_FLD = CONVERSATIONS_FIELDS.get("TEMPLATE_LINK", "Template")
CAMPAIGN_LINK_FLD = CONVERSATIONS_FIELDS.get("CAMPAIGN_LINK", "Campaign")
>>>>>>> codex-refactor-test

LEAD_STATUS_FIELD = LEAD_FIELDS["STATUS"]
LEAD_PHONE_FIELD = LEAD_FIELDS["PHONE"]
LEAD_SOURCE_FIELD = LEAD_FIELDS.get("SOURCE", "Source")
LEAD_REPLY_COUNT_FIELD = LEAD_FIELDS["REPLY_COUNT"]
LEAD_SENT_COUNT_FIELD = LEAD_FIELDS["SENT_COUNT"]
LEAD_DELIVERED_COUNT_FIELD = LEAD_FIELDS["DELIVERED_COUNT"]
LEAD_FAILED_COUNT_FIELD = LEAD_FIELDS["FAILED_COUNT"]
LEAD_LAST_ACTIVITY_FIELD = LEAD_FIELDS["LAST_ACTIVITY"]
LEAD_LAST_DIRECTION_FIELD = LEAD_FIELDS["LAST_DIRECTION"]
LEAD_LAST_MESSAGE_FIELD = LEAD_FIELDS["LAST_MESSAGE"]
LEAD_LAST_OUTBOUND_FIELD = LEAD_FIELDS["LAST_OUTBOUND"]
LEAD_LAST_INBOUND_FIELD = LEAD_FIELDS["LAST_INBOUND"]
LEAD_LAST_DELIVERY_STATUS_FIELD = LEAD_FIELDS["LAST_DELIVERY_STATUS"]
LEAD_PROPERTY_ID_FIELD = LEAD_FIELDS["PROPERTY_ID"]

DEFAULT_PROCESSED_BY = os.getenv(
    "TEXTGRID_PROCESSED_BY_LABEL",
    ConversationProcessor.CAMPAIGN_RUNNER.value,
)
DRY_RUN = os.getenv("TEXTGRID_DRY_RUN", "0").lower() in ("1", "true", "yes")


# =========================
# Small helpers
# =========================
def _now_iso() -> str:
    # seconds precision keeps Airtable tidy and sortable
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _safe_table_create(tbl, payload: Dict) -> Optional[Dict]:
    """Create with 'existing fields only' to avoid 422s on computed/select fields."""
    if not tbl or not payload:
        return None
    try:
        try:
            probe = tbl.all(max_records=1)  # peek schema
            keys = list((probe[0] or {}).get("fields", {}).keys()) if probe else []
        except Exception:
            keys = list(payload.keys())
        norm = {_norm(k): k for k in keys}
        filtered = {}
        for k, v in payload.items():
            mk = norm.get(_norm(k))
            if mk:
                filtered[mk] = v
        return tbl.create(filtered if filtered else {})
    except Exception:
        traceback.print_exc()
        return None


def _safe_table_update(tbl, rec_id: str, patch: Dict) -> Optional[Dict]:
    """Update with 'existing fields only'."""
    if not tbl or not rec_id or not patch:
        return None
    try:
        try:
            probe = tbl.get(rec_id)
            keys = list((probe or {}).get("fields", {}).keys()) if probe else []
        except Exception:
            keys = list(patch.keys())
        norm = {_norm(k): k for k in keys}
        filtered = {}
        for k, v in patch.items():
            mk = norm.get(_norm(k))
            if mk:
                filtered[mk] = v
        return tbl.update(rec_id, filtered if filtered else {})
    except Exception:
        traceback.print_exc()
        return None


def _upsert_convo_by_msgid(tbl, msg_id: Optional[str], payload: Dict) -> Optional[Dict]:
    """
    Create or update a Conversations row uniquely by TextGrid ID (MessageSid).
    Falls back to create if no msg_id or formula helper unavailable.
    """
    if not tbl:
        return None
    try:
        if not msg_id:
            return _safe_table_create(tbl, payload)

        if match:
            existing = tbl.all(formula=match({TG_ID_FIELD: msg_id}))
        else:
            # Fallback formula (simple equality)
            # Note: Airtable formula string escaping is minimal here; TG_ID_FIELD should be a simple column name.
            existing = tbl.all(formula=f"{{{TG_ID_FIELD}}}='{msg_id}'")

        if existing:
            rec_id = existing[0]["id"]
            return _safe_table_update(tbl, rec_id, payload)
        else:
            return _safe_table_create(tbl, payload)
    except Exception:
        traceback.print_exc()
        return None


def _http_post(url: str, data: Dict[str, Any], auth: Tuple[str, str], timeout: int = 10) -> Dict[str, Any]:
    """POST with httpx or requests, returns parsed JSON (or minimal fallback). Handles 429."""
    if DRY_RUN:
        print(f"[DRY RUN] POST {url} data={data}")
        return {"sid": f"SM_fake_{int(time.time())}"}

    client = httpx or requests
    if client is None:
        raise RuntimeError("No HTTP client available (install httpx or requests).")

    if httpx:
        resp = httpx.post(url, data=data, auth=auth, timeout=timeout)
        if resp.status_code == 429:
            raise RuntimeError(f"429 rate limited; retry_after={resp.headers.get('Retry-After')}")
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"raw": resp.text}

    # requests fallback
    resp = requests.post(url, data=data, auth=auth, timeout=timeout)
    if resp.status_code == 429:
        raise RuntimeError(f"429 rate limited; retry_after={resp.headers.get('Retry-After')}")
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


# =========================
# Lead helpers
# =========================
def _find_or_create_lead(phone_number: str, source: str = "Outbound") -> Tuple[Optional[str], Optional[str]]:
    """Ensure every outbound target has a Lead. Returns (lead_id, property_id)."""
    leads_tbl = get_leads()
    if not (leads_tbl and phone_number):
        return None, None
    try:
<<<<<<< HEAD
        recs = leads_tbl.all(formula=f"{{phone}}='{phone_number}'")
=======
        # match on exact 'phone'; extend if you keep last10 elsewhere
        recs = leads_tbl.all(formula=f"{{{LEAD_PHONE_FIELD}}}='{phone_number}'")
>>>>>>> codex-refactor-test
        if recs:
            lf = recs[0].get("fields", {})
            return recs[0]["id"], lf.get(PROPERTY_ID_FIELD) or lf.get("Property ID")

        created = _safe_table_create(
            leads_tbl,
            {
                LEAD_PHONE_FIELD: phone_number,
                LEAD_STATUS_FIELD: LeadStatus.NEW.value,
                LEAD_SOURCE_FIELD: source,
                LEAD_REPLY_COUNT_FIELD: 0,
                LEAD_SENT_COUNT_FIELD: 0,
                LEAD_DELIVERED_COUNT_FIELD: 0,
                LEAD_FAILED_COUNT_FIELD: 0,
                LEAD_LAST_ACTIVITY_FIELD: _now_iso(),
            },
        )
        if created:
            print(f"✨ Created Lead for {phone_number}")
            cf = created.get("fields", {}) if isinstance(created, dict) else {}
            return created.get("id"), cf.get(LEAD_PROPERTY_ID_FIELD) or cf.get("Property ID")
    except Exception:
        traceback.print_exc()
    return None, None


def _update_lead_activity(
    lead_id: Optional[str],
    body: str,
    direction: str,
    property_id: Optional[str] = None,
) -> None:
    if not lead_id:
        return
    leads_tbl = get_leads()
    if not leads_tbl:
        return
    try:
        current = leads_tbl.get(lead_id) or {}
        f = current.get("fields", {}) if isinstance(current, dict) else {}
        patch = {
            LEAD_LAST_ACTIVITY_FIELD: _now_iso(),
            LEAD_LAST_DIRECTION_FIELD: direction,
            LEAD_LAST_MESSAGE_FIELD: (body or "")[:500],
        }
<<<<<<< HEAD
        if direction == "OUT":
            patch["Last Outbound"] = _now_iso()
            patch["Sent Count"] = int(f.get("Sent Count", 0)) + 1
        if property_id:
            patch[PROPERTY_ID_FIELD] = property_id
=======
        if direction in ("OUT", ConversationDirection.OUTBOUND.value):
            patch[LEAD_LAST_OUTBOUND_FIELD] = _now_iso()
        if direction in ("IN", ConversationDirection.INBOUND.value):
            patch[LEAD_LAST_INBOUND_FIELD] = _now_iso()
        if property_id and LEAD_PROPERTY_ID_FIELD:
            patch[LEAD_PROPERTY_ID_FIELD] = property_id
>>>>>>> codex-refactor-test
        _safe_table_update(leads_tbl, lead_id, patch)
    except Exception:
        traceback.print_exc()


# =========================
# Core Sender
# =========================
def send_message(
    to: str,
    body: str,
    from_number: Optional[str] = None,
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
    Send an SMS (or MMS if media_urls provided) via TextGrid and write a Conversations row.
    Returns: {"ok": bool, "sid": str|None, "to": str, "from": str|None, "lead_id": str|None, "property_id": str|None, ...}

    Notes:
      - Conversations mapping uses `phone` (counterparty) and `to_number` (our DID).
      - If Airtable is unavailable, sending still happens; logging is best-effort.
    """
    if not to or not body:
        return {"ok": False, "error": "missing to/body"}

    if not ACCOUNT_SID or not AUTH_TOKEN or not BASE_URL:
        return {"ok": False, "error": "TEXTGRID_ACCOUNT_SID/TEXTGRID_AUTH_TOKEN not configured"}

    # Choose a DID if not provided
    sender = from_number
    if not sender and get_from_number:
        try:
            sender = get_from_number(market or "")
        except Exception as e:
            print(f"⚠️ DID selection failed (market={market}): {e}")
            sender = None
    if not sender:
        return {"ok": False, "error": "no from_number available"}

    # Ensure a Lead exists (optional/best-effort)
    if not lead_id:
        _lead_id, _prop_id = _find_or_create_lead(to, source="Outbound")
        lead_id = lead_id or _lead_id
        property_id = property_id or _prop_id

    # Send with retries (429-aware exponential backoff)
    last_err = None
    msg_id: Optional[str] = None

    for attempt in range(1, max(1, retries) + 1):
        try:
            data: Dict[str, Any] = {"To": to, "From": sender, "Body": body}
            if media_urls:
                # Many gateways accept MediaUrl, MediaUrl2, ...
                for i, m in enumerate(media_urls):
                    data["MediaUrl" + ("" if i == 0 else str(i + 1))] = m

            resp = _http_post(BASE_URL, data=data, auth=(ACCOUNT_SID, AUTH_TOKEN), timeout=timeout)
            msg_id = (resp or {}).get("sid") or (resp or {}).get("message_sid") or (resp or {}).get("id")
            print(f"📤 OUT → {to} (from {sender}) | {body[:120]}")
            last_err = None
            break
        except Exception as e:
            last_err = str(e)
            print(f"❌ Send attempt {attempt}/{retries} failed → {to}: {last_err}")
            if attempt < retries:
                wait = 2 ** attempt
                m = re.search(r"retry_after=(\d+)", last_err or "")
                if m:
                    wait = max(wait, int(m.group(1)))
                print(f"⏳ retrying in {wait}s...")
                time.sleep(wait)

    # Log to Conversations (best effort) with upsert by MessageSid to avoid duplicates
    convos_tbl = get_convos()
    try:
        if convos_tbl:
            rec: Dict[str, Any] = {
                FROM_FIELD: to,               # counterparty phone
                TO_FIELD: sender,             # our DID used to send
                MSG_FIELD: body,
                DIR_FIELD: ConversationDirection.OUTBOUND.value,
                STATUS_FIELD: (
                    ConversationDeliveryStatus.SENT.value
                    if last_err is None
                    else ConversationDeliveryStatus.FAILED.value
                ),
                SENT_AT_FIELD: _now_iso(),
                PROCESSED_BY_FIELD: DEFAULT_PROCESSED_BY,
                TG_ID_FIELD: msg_id,
            }
            if lead_id and LEAD_LINK_FIELD:
                rec[LEAD_LINK_FIELD] = [lead_id]
            if property_id and PROPERTY_ID_FIELD:
                rec[PROPERTY_ID_FIELD] = property_id
            if template_id and TEMPLATE_LINK_FLD:
                rec[TEMPLATE_LINK_FLD] = [template_id]
            if campaign_id and CAMPAIGN_LINK_FLD:
                rec[CAMPAIGN_LINK_FLD] = [campaign_id]

            _upsert_convo_by_msgid(convos_tbl, msg_id, rec)
    except Exception:
        traceback.print_exc()

    # Update lead activity trail (increments Sent Count on OUT)
    try:
        _update_lead_activity(lead_id, body, ConversationDirection.OUTBOUND.value, property_id=property_id)
    except Exception:
        traceback.print_exc()

    if last_err is None:
        return {
            "ok": True,
            "sid": msg_id,
            "to": to,
            "from": sender,
            "lead_id": lead_id,
            "property_id": property_id,
        }
    else:
        return {
            "ok": False,
            "error": last_err,
            "sid": msg_id,
            "to": to,
            "from": sender,
            "lead_id": lead_id,
            "property_id": property_id,
        }
