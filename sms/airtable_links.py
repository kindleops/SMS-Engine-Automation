"""
🚀 Airtable Links Manager
-------------------------
Resolves and links records across Leads, Prospects, and Conversations
based on phone numbers.

Features:
 • Cross-table phone matching (last 10 digits)
 • Safe create/update with schema filtering
 • Automatic Lead upsert from inbound Conversations
 • Structured logging + retry safety
"""

from __future__ import annotations
import os, re, time, traceback
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime, timezone
from functools import lru_cache
from pyairtable import Table
from sms.runtime import get_logger

logger = get_logger("airtable_links")

# ==========================================================
# CONFIG
# ==========================================================
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE = os.getenv("PROSPECTS_TABLE", "Prospects")
CONVOS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")

PHONE_CANDIDATES = [
    "phone",
    "Phone",
    "Mobile",
    "Cell",
    "Phone Number",
    "Primary Phone",
    "Phone 1",
    "Phone 2",
    "Phone 3",
    "Owner Phone",
    "Owner Phone 1",
    "Owner Phone 2",
    "Phone 1 (from Linked Owner)",
    "Phone 2 (from Linked Owner)",
    "Phone 3 (from Linked Owner)",
]


# ==========================================================
# UTILITIES
# ==========================================================
def _digits(s: str) -> str:
    return "".join(re.findall(r"\d+", s or "")) if isinstance(s, str) else ""


def last10(s: str) -> str:
    d = _digits(s)
    return d[-10:] if len(d) >= 10 else ""


# ==========================================================
# TABLE HANDLERS
# ==========================================================
@lru_cache(maxsize=None)
def _safe_tbl(base_id: str, table_name: str) -> Optional[Table]:
    if not (AIRTABLE_API_KEY and base_id and table_name):
        logger.warning(f"⚠️ Missing Airtable config for {table_name}")
        return None
    try:
        return Table(AIRTABLE_API_KEY, base_id, table_name)
    except Exception as e:
        logger.error(f"❌ Failed to init table {table_name}: {e}")
        traceback.print_exc()
        return None


_field_cache: Dict[str, List[str]] = {}


def _existing_cols(tbl: Table) -> List[str]:
    """Cached Airtable field list to prevent repeated .all() calls."""
    tid = getattr(tbl, "name", None) or str(id(tbl))
    if tid in _field_cache:
        return _field_cache[tid]
    try:
        rows = tbl.all(max_records=1) or []
        cols = list((rows[0] or {}).get("fields", {}).keys()) if rows else []
        _field_cache[tid] = cols
        return cols
    except Exception:
        traceback.print_exc()
        return []


# ==========================================================
# SAFE CRUD OPERATIONS (with retries)
# ==========================================================
def _with_retry(fn, *args, retries=3, delay=0.5, **kwargs):
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if "429" in msg or "422" in msg:
                time.sleep(delay)
                delay *= 2
                continue
            traceback.print_exc()
            break
    return None


def _safe_create(tbl: Table, fields: Dict) -> Optional[Dict]:
    if not (tbl and fields):
        return None
    try:
        existing = set(_existing_cols(tbl))
        payload = {k: v for k, v in fields.items() if k in existing}
        return _with_retry(tbl.create, payload if payload else {})
    except Exception:
        traceback.print_exc()
        return None


def _safe_update(tbl: Table, rec_id: str, fields: Dict) -> Optional[Dict]:
    if not (tbl and rec_id and fields):
        return None
    try:
        existing = set(_existing_cols(tbl))
        payload = {k: v for k, v in fields.items() if k in existing}
        return _with_retry(tbl.update, rec_id, payload if payload else {})
    except Exception:
        traceback.print_exc()
        return None


# ==========================================================
# SCANNERS
# ==========================================================
def _scan_by_last10(tbl: Table, phone: str) -> Optional[Dict]:
    """Find record where any known phone column matches last10."""
    want = last10(phone)
    if not (tbl and want):
        return None
    cols = [c for c in PHONE_CANDIDATES if c in set(_existing_cols(tbl))]
    if not cols:
        return None
    try:
        # Airtable formula filter (faster than scanning all)
        formula = "OR(" + ",".join([f"FIND('{want}', {{{c}}})" for c in cols]) + ")"
        results = tbl.all(filterByFormula=formula)
        if results:
            return results[0]
    except Exception:
        traceback.print_exc()
    return None


# ==========================================================
# CORE FUNCTIONS
# ==========================================================
def upsert_lead_for_phone(from_number: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (lead_id, property_id).
    If no Lead exists:
        → try to find matching Prospect (carry fields)
        → create new Lead record
    """
    if not last10(from_number):
        return None, None

    leads_tbl = _safe_tbl(BASE_ID, LEADS_TABLE)
    prospects_tbl = _safe_tbl(BASE_ID, PROSPECTS_TABLE)
    if not leads_tbl:
        return None, None

    # Already a lead?
    lead = _scan_by_last10(leads_tbl, from_number)
    if lead:
        f = lead.get("fields", {})
        return lead["id"], f.get("Property ID")

    # Try to find a matching Prospect
    carry: Dict[str, Any] = {}
    property_id = None
    if prospects_tbl:
        pr = _scan_by_last10(prospects_tbl, from_number)
        if pr:
            pf = pr.get("fields", {}) or {}
            carry = {
                k: pf.get(k) for k in ("Owner Name", "Address", "Market", "Sync Source", "List", "Source List", "Property Type") if k in pf
            }
            property_id = pf.get("Property ID")

    # Create new Lead
    new_fields = {
        **carry,
        "Phone": from_number,
        "Lead Status": "New",
        "Source": "Inbound",
        "Reply Count": 0,
        "Last Inbound": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "Property ID": property_id,
    }

    created = _safe_create(leads_tbl, new_fields)
    if created:
        cf = created.get("fields", {}) or {}
        logger.info(f"✅ Created new Lead {created.get('id')} for {from_number}")
        return created.get("id"), cf.get("Property ID") or property_id

    logger.warning(f"⚠️ Failed to upsert Lead for {from_number}")
    return None, None


def link_conversation_to_lead(convos_tbl: Table, conversation_id: str, lead_id: str, lead_link_field: str = "Lead"):
    """Ensures Conversations.<lead_link_field> = [lead_id]."""
    if not (convos_tbl and conversation_id and lead_id):
        return
    _safe_update(convos_tbl, conversation_id, {lead_link_field: [lead_id]})
    logger.debug(f"🔗 Linked Conversation {conversation_id} → Lead {lead_id}")


def find_lead_id_by_to_number(to_number: str) -> Optional[str]:
    leads_tbl = _safe_tbl(BASE_ID, LEADS_TABLE)
    if not leads_tbl:
        return None
    r = _scan_by_last10(leads_tbl, to_number)
    return r["id"] if r else None
