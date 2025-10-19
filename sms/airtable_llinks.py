# sms/airtable_links.py
from __future__ import annotations
import re, traceback, os
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timezone
from pyairtable import Table

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")

LEADS_TABLE      = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE  = os.getenv("PROSPECTS_TABLE", "Prospects")
CONVOS_TABLE     = os.getenv("CONVERSATIONS_TABLE", "Conversations")

PHONE_CANDIDATES = [
    "phone","Phone","Mobile","Cell","Phone Number","Primary Phone",
    "Phone 1","Phone 2","Phone 3",
    "Owner Phone","Owner Phone 1","Owner Phone 2",
    "Phone 1 (from Linked Owner)","Phone 2 (from Linked Owner)","Phone 3 (from Linked Owner)",
]

def _digits(s): 
    return "".join(re.findall(r"\d+", s or "")) if isinstance(s, str) else ""

def last10(s) -> str:
    d = _digits(s);  return d[-10:] if len(d) >= 10 else ""

def _safe_tbl(base_id: str, table_name: str) -> Optional[Table]:
    if not (AIRTABLE_API_KEY and base_id and table_name):
        return None
    try:
        return Table(AIRTABLE_API_KEY, base_id, table_name)
    except Exception:
        traceback.print_exc(); return None

def _existing_cols(tbl: Table) -> List[str]:
    try:
        rows = tbl.all(max_records=1) or []
        return list((rows[0] or {}).get("fields", {}).keys()) if rows else []
    except Exception:
        return []

def _scan_by_last10(tbl: Table, phone: str) -> Optional[Dict]:
    """Find the first record where any candidate phone column matches last10."""
    want = last10(phone)
    if not (tbl and want):
        return None
    cols = [c for c in PHONE_CANDIDATES if c in set(_existing_cols(tbl))]
    try:
        for r in tbl.all():
            f = r.get("fields", {}) or {}
            for c in cols:
                if last10(f.get(c)) == want:
                    return r
    except Exception:
        traceback.print_exc()
    return None

def _safe_create(tbl: Table, fields: Dict) -> Optional[Dict]:
    if not (tbl and fields): return None
    try:
        # filter to existing columns to avoid 422s
        existing = set(_existing_cols(tbl))
        payload = {k: v for k, v in fields.items() if k in existing}
        return tbl.create(payload if payload else {})
    except Exception:
        traceback.print_exc(); return None

def _safe_update(tbl: Table, rec_id: str, fields: Dict) -> Optional[Dict]:
    if not (tbl and rec_id and fields): return None
    try:
        existing = set(_existing_cols(tbl))
        payload = {k: v for k, v in fields.items() if k in existing}
        return tbl.update(rec_id, payload if payload else {})
    except Exception:
        traceback.print_exc(); return None

def upsert_lead_for_phone(from_number: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (lead_id, property_id). If lead doesn’t exist:
      - copy mapped fields from first matching Prospect by last10
      - create a Lead, and return its record ID
    """
    if not last10(from_number):
        return None, None

    leads_tbl     = _safe_tbl(BASE_ID, LEADS_TABLE)
    prospects_tbl = _safe_tbl(BASE_ID, PROSPECTS_TABLE)
    if not leads_tbl:
        return None, None

    # already a Lead?
    lead = _scan_by_last10(leads_tbl, from_number)
    if lead:
        f = lead.get("fields", {})
        return lead["id"], f.get("Property ID")

    # find Prospect for field carryover
    carry: Dict = {}
    property_id = None
    if prospects_tbl:
        pr = _scan_by_last10(prospects_tbl, from_number)
        if pr:
            pf = pr.get("fields", {}) or {}
            carry = {
                k: pf.get(k) for k in (
                    "Owner Name","Address","Market","Sync Source","List","Source List","Property Type"
                ) if k in pf
            }
            property_id = pf.get("Property ID")

    # create Lead
    created = _safe_create(leads_tbl, {
        **carry,
        "phone": from_number,             # if the Leads table uses different casing, _safe_create filters
        "Lead Status": "New",
        "Source": "Inbound",
        "Reply Count": 0,
        "Last Inbound": datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "Property ID": property_id,
    })
    if created:
        cf = created.get("fields", {}) or {}
        return created.get("id"), cf.get("Property ID") or property_id
    return None, None

def link_conversation_to_lead(convos_tbl: Table, conversation_id: str, lead_id: str, lead_link_field: str = "lead_id"):
    """
    Ensures Conversations.<lead_link_field> = [lead_id]
    (Linked fields must be arrays of Airtable record IDs.)
    """
    if not (convos_tbl and conversation_id and lead_id):
        return
    _safe_update(convos_tbl, conversation_id, {lead_link_field: [lead_id]})

def find_lead_id_by_to_number(to_number: str) -> Optional[str]:
    leads_tbl = _safe_tbl(BASE_ID, LEADS_TABLE)
    if not leads_tbl:
        return None
    r = _scan_by_last10(leads_tbl, to_number)
    return r["id"] if r else None