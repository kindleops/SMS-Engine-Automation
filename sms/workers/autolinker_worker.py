<<<<<<< HEAD
# sms/workers/autolinker_worker.py
import asyncio, os, re, time, traceback
from datetime import datetime, timezone
from sms.tables import get_table

SLEEP_SEC = int(os.getenv("AUTOLINKER_INTERVAL_SEC", "90"))
BATCH = int(os.getenv("AUTOLINKER_BATCH", "200"))

# common helpers
def last10(s: str | None) -> str:
    if not s: return ""
    return re.sub(r"[^0-9]", "", str(s))[-10:]

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

LEAD_PHONE_FIELDS = ["phone","Phone","Phone (Raw)","Phone E164","Primary Phone","Mobile","Owner Phone",
                     "Phone 1 (from Linked Owner)","Phone 2 (from Linked Owner)","Phone 3 (from Linked Owner)"]

CONV_FROM = ["from_number","From","from","phone"]
CONV_TO   = ["to_number","To","to","recipient","phone"]
CONV_DIR  = ["direction","Direction","DIR","Dir"]

def pick_phone10(cf: dict) -> str:
    direction = (cf.get("direction") or cf.get("Direction") or "").upper()
    fields = (CONV_TO + CONV_FROM) if direction in ("OUT","OUTBOUND") else (CONV_FROM + CONV_TO)
    for k in fields:
        if cf.get(k):
            p = last10(cf.get(k))
            if p: return p
    # fallback scan
    for k in set(CONV_FROM + CONV_TO):
        if cf.get(k):
            p = last10(cf.get(k))
            if p: return p
    return ""

def build_map(tbl, phone_fields=LEAD_PHONE_FIELDS):
    m_phone, m_rec = {}, {}
    for r in tbl.all():
        rid = r["id"]; f = r.get("fields", {})
        m_rec[rid] = rid
        for k in phone_fields:
            v = f.get(k)
            p = last10(v)
            if p and p not in m_phone: m_phone[p] = rid
    return m_phone, m_rec

async def run_once():
    C = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","CONVERSATIONS_TABLE","Conversations")
    P = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","PROSPECTS_TABLE","Prospects")
    L = get_table("AIRTABLE_API_KEY","LEADS_CONVOS_BASE","LEADS_TABLE","Leads")

    p_by_phone, _ = build_map(P)
    l_by_phone, _ = build_map(L)

    rows = C.all()
    changed = 0
    for r in rows[:BATCH]:
        cid = r["id"]; f = r.get("fields", {})
        updates = {}
        # mirror conversation record id
        if f.get("Conversation Record ID") != cid:
            updates["Conversation Record ID"] = cid

        # if Prospect link missing, try phone10
        if not f.get("Prospect"):
            p10 = pick_phone10(f)
            if p10 and p10 in p_by_phone:
                updates["Prospect"] = [p_by_phone[p10]]
                updates["Prospect Record ID"] = p_by_phone[p10]

        # if Lead link missing, try phone10
        if not f.get("Lead"):
            p10 = pick_phone10(f)
            if p10 and p10 in l_by_phone:
                updates["Lead"] = [l_by_phone[p10]]
                updates["Lead Record ID"] = l_by_phone[p10]

        if updates:
            try:
                C.update(cid, updates)
                changed += 1
                time.sleep(0.15)
            except Exception:
                traceback.print_exc()

    print(f"[autolinker] {utcnow_iso()} updated={changed} scanned={min(BATCH,len(rows))}")

async def main():
    while True:
        try:
            await run_once()
        except Exception:
            traceback.print_exc()
        await asyncio.sleep(SLEEP_SEC)

if __name__ == "__main__":
    asyncio.run(main())
=======
"""Worker that backfills Conversation ↔ Prospect/Lead links."""

from __future__ import annotations

from typing import Dict, Optional

from .. import spec
from ..datastore import ensure_prospect_or_lead, update_conversation, CONNECTOR


def run(limit: Optional[int] = None) -> Dict[str, int]:
    handle = CONNECTOR.conversations()
    records = handle.table.all()
    linked = 0
    for record in records:
        fields = record.get("fields", {})
        if fields.get(spec.CONVERSATION_FIELDS.lead_link) or fields.get(spec.CONVERSATION_FIELDS.prospect_link):
            continue
        phone = fields.get(spec.CONVERSATION_FIELDS.seller_phone)
        if not phone:
            continue
        lead, prospect = ensure_prospect_or_lead(str(phone))
        updates = {}
        if lead:
            updates[spec.CONVERSATION_FIELDS.lead_link] = [lead["id"]]
        elif prospect:
            updates[spec.CONVERSATION_FIELDS.prospect_link] = [prospect["id"]]
        if updates:
            update_conversation(record["id"], updates)
            linked += 1
            if limit is not None and linked >= limit:
                break
    return {"linked": linked}


if __name__ == "__main__":
    print(run())

>>>>>>> codex-refactor-test
