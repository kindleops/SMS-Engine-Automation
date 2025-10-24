#!/usr/bin/env python3
from __future__ import annotations
import os, re
from typing import Dict, List, Optional, Tuple, Any

# optional .env
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from sms.tables import get_table as _get_table

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


# ---------------- env helpers ----------------
def _req(env: str) -> str:
    v = os.getenv(env)
    if not v:
        raise RuntimeError(f"Missing required env var: {env}")
    return v


def _choose_env(cands: List[str]) -> str:
    for c in cands:
        if os.getenv(c):
            return c
    raise RuntimeError(f"Missing required env var (any of): {', '.join(cands)}")


BASE_ENV = _choose_env(["LEADS_CONVOS_BASE", "LEADS_CONVO_BASE"])

TABLES = {
    "Leads": ([BASE_ENV], "LEADS_TABLE"),
    "Conversations": ([BASE_ENV], "CONVERSATIONS_TABLE"),
    "Campaigns": ([BASE_ENV], "CAMPAIGNS_TABLE"),
    "Templates": ([BASE_ENV], "TEMPLATES_TABLE"),
    "Prospects": ([BASE_ENV], "PROSPECTS_TABLE"),
    "Drip Queue": ([BASE_ENV], "DRIP_QUEUE_TABLE"),
    "Opt-Outs": ([BASE_ENV], "OPTOUTS_TABLE"),
}

LINK_FIELDS = {
    "Lead": "Leads",
    "Template": "Templates",
    "Campaign": "Campaigns",
    "Prospect": "Prospects",
}

# single-text “* Record ID” fields to mirror into
TEXT_RECORD_ID_FIELDS = {
    "Lead": "Lead Record ID",
    "Template": "Template Record ID",
    "Campaign": "Campaign Record ID",
    "Prospect": "Prospect Record ID",
    "Conversation": "Conversation Record ID",
}


def get_table(name: str):
    _req("AIRTABLE_API_KEY")
    base_envs, table_env = TABLES[name]
    base_env = _choose_env(base_envs)
    _req(table_env)
    return _get_table("AIRTABLE_API_KEY", base_env, table_env, name)


# ---------------- utils ----------------
def last10(s: Optional[str]) -> str:
    if s is None:
        return ""
    return re.sub(r"[^0-9]", "", str(s))[-10:]


def is_rec_id(v: Any) -> bool:
    return isinstance(v, str) and v.startswith("rec")


def safe_update(tbl, rec_id: str, fields: dict):
    # drop Nones & empty
    payload = {k: v for k, v in fields.items() if v not in (None, "", [], {})}
    if not payload:
        return
    if DRY_RUN:
        print(f"[DRY] UPDATE {tbl._table_url} :: {rec_id} <- {payload}")
        return
    try:
        tbl.update(rec_id, payload)
    except Exception as e:
        print(f"⚠️  Update failed for {rec_id}: {e}")


# ---------------- maps for lookups ----------------
LEAD_PHONE_FIELDS = ["phone", "Phone", "Phone (Raw)", "Phone E164", "Primary Phone", "Mobile", "Owner Phone"]


def build_lead_maps() -> Tuple[Dict[str, str], Dict[str, str], Dict[int, str]]:
    """Returns:
    phone10 -> lead_rec
    leadId_str -> lead_rec
    leadId_int -> lead_rec
    """
    tbl = get_table("Leads")
    phone_map: Dict[str, str] = {}
    leadid_str: Dict[str, str] = {}
    leadid_int: Dict[int, str] = {}
    rows = tbl.all()
    for r in rows:
        rid = r.get("id")
        f = r.get("fields", {})
        if not rid:
            continue
        # phone
        for k in LEAD_PHONE_FIELDS:
            v = f.get(k)
            if v:
                p10 = last10(v)
                if p10:
                    phone_map.setdefault(p10, rid)
        # Lead ID (could be int or str)
        lid = f.get("Lead ID")
        if lid is not None and lid != "":
            if isinstance(lid, int):
                leadid_int.setdefault(lid, rid)
                leadid_str.setdefault(str(lid), rid)
            else:
                s = str(lid).strip()
                if s:
                    leadid_str.setdefault(s, rid)
                    try:
                        leadid_int.setdefault(int(s), rid)
                    except Exception:
                        pass
    print(f"ℹ️  Lead maps: phone={len(phone_map)} leadId_str={len(leadid_str)} leadId_int={len(leadid_int)}")
    return phone_map, leadid_str, leadid_int


def build_template_map() -> Dict[str, str]:
    """Prefer explicit Template record IDs if already stored on Templates; otherwise map by Message text."""
    tbl = get_table("Templates")
    msg_to_rec: Dict[str, str] = {}
    for r in tbl.all():
        rid = r.get("id")
        f = r.get("fields", {})
        if not rid:
            continue
        body = (f.get("Message") or f.get("Body") or f.get("Text") or f.get("Content") or "").strip()
        if body:
            msg_to_rec.setdefault(body, rid)
    print(f"ℹ️  Template message map: {len(msg_to_rec)}")
    return msg_to_rec


def build_campaign_map() -> Dict[str, str]:
    """Map by Campaign ID and Campaign Name (if unique)."""
    tbl = get_table("Campaigns")
    m: Dict[str, str] = {}
    for r in tbl.all():
        rid = r.get("id")
        f = r.get("fields", {})
        if not rid:
            continue
        for k in ("Campaign ID", "Campaign Name"):
            v = f.get(k)
            if v not in (None, ""):
                m.setdefault(str(v).strip(), rid)
    print(f"ℹ️  Campaign map: {len(m)}")
    return m


def build_prospect_maps() -> Tuple[Dict[str, str], Dict[str, str], Dict[int, str]]:
    """phone10 -> rec, LeadID str/int -> rec (if Prospects carries them)."""
    tbl = get_table("Prospects")
    phone_map: Dict[str, str] = {}
    lid_str: Dict[str, str] = {}
    lid_int: Dict[int, str] = {}
    for r in tbl.all():
        rid = r.get("id")
        f = r.get("fields", {})
        if not rid:
            continue
        for k in LEAD_PHONE_FIELDS:
            v = f.get(k)
            if v:
                p10 = last10(v)
                if p10:
                    phone_map.setdefault(p10, rid)
        lid = f.get("Lead ID")
        if lid not in (None, ""):
            if isinstance(lid, int):
                lid_int.setdefault(lid, rid)
                lid_str.setdefault(str(lid), rid)
            else:
                s = str(lid).strip()
                if s:
                    lid_str.setdefault(s, rid)
                    try:
                        lid_int.setdefault(int(s), rid)
                    except Exception:
                        pass
    print(f"ℹ️  Prospect maps: phone={len(phone_map)} leadId_str={len(lid_str)} leadId_int={len(lid_int)}")
    return phone_map, lid_str, lid_int


# ---------------- conversation helpers ----------------
CONV_TO_FIELDS = ["to_number", "To", "to", "recipient", "phone"]
CONV_FROM_FIELDS = ["from_number", "From", "from", "sender", "phone"]
CONV_DIR_FIELDS = ["direction", "Direction"]


def extract_conv_phone10(cf: dict) -> str:
    direction = ""
    for k in CONV_DIR_FIELDS:
        if cf.get(k):
            direction = str(cf.get(k)).upper()
            break
    order = (
        (CONV_TO_FIELDS + CONV_FROM_FIELDS)
        if direction == "OUTBOUND"
        else (CONV_FROM_FIELDS + CONV_TO_FIELDS)
        if direction == "INBOUND"
        else (CONV_TO_FIELDS + CONV_FROM_FIELDS)
    )
    for k in order:
        if cf.get(k):
            p = last10(cf.get(k))
            if p:
                return p
    return ""


# ---------------- main fix ----------------
def main():
    conv_tbl = get_table("Conversations")
    leads_phone, leads_id_str, leads_id_int = build_lead_maps()
    tmpl_by_msg = build_template_map()
    camp_map = build_campaign_map()
    pros_phone, pros_id_str, pros_id_int = build_prospect_maps()

    convs = conv_tbl.all()
    fixed = 0
    already_ok = 0
    scanned = 0

    for r in convs:
        scanned += 1
        cid = r.get("id")
        f = r.get("fields", {})

        updates: Dict[str, Any] = {}

        # --- Always mirror Conversation's own rec into Conversation Record ID (text) if present ---
        conv_text_field = TEXT_RECORD_ID_FIELDS["Conversation"]
        if conv_text_field in f and f.get(conv_text_field) != cid:
            updates[conv_text_field] = cid

        # ---------- Lead ----------
        lead_text_field = TEXT_RECORD_ID_FIELDS["Lead"]  # 'Lead Record ID'
        current_link = f.get("Lead")
        current_text = f.get(lead_text_field)

        lead_rec_to_set: Optional[str] = None

        # 1) prefer already-filled text rec id
        if is_rec_id(current_text):
            lead_rec_to_set = current_text
        # 2) else prefer existing link (if any)
        elif isinstance(current_link, list) and current_link and is_rec_id(current_link[0]):
            lead_rec_to_set = current_link[0]
        else:
            # 3) derive by phone or Lead ID present in conversation
            p10 = extract_conv_phone10(f)
            if p10 and p10 in leads_phone:
                lead_rec_to_set = leads_phone[p10]
            else:
                # scan common lead-id-ish fields
                for k in ("Lead ID", "lead_id", "leadId", "lead", "Lead"):
                    v = f.get(k)
                    if v in (None, ""):
                        continue
                    if isinstance(v, int) and v in leads_id_int:
                        lead_rec_to_set = leads_id_int[v]
                        break
                    s = str(v).strip()
                    if s and s in leads_id_str:
                        lead_rec_to_set = leads_id_str[s]
                        break

        if lead_rec_to_set:
            # write both link + text
            if not (isinstance(current_link, list) and current_link and current_link[0] == lead_rec_to_set):
                updates["Lead"] = [lead_rec_to_set]
            if f.get(lead_text_field) != lead_rec_to_set:
                updates[lead_text_field] = lead_rec_to_set

        # ---------- Template ----------
        tmpl_text_field = TEXT_RECORD_ID_FIELDS["Template"]  # 'Template Record ID'
        curr_tmpl_link = f.get("Template")
        curr_tmpl_text = f.get(tmpl_text_field)
        tmpl_rec_to_set: Optional[str] = None

        if is_rec_id(curr_tmpl_text):
            tmpl_rec_to_set = curr_tmpl_text
        elif isinstance(curr_tmpl_link, list) and curr_tmpl_link and is_rec_id(curr_tmpl_link[0]):
            tmpl_rec_to_set = curr_tmpl_link[0]
        else:
            # try to match by message body stored on conversation
            for body_key in ("Body", "body", "Message", "text", "message"):
                body = (f.get(body_key) or "").strip()
                if body and body in tmpl_by_msg:
                    tmpl_rec_to_set = tmpl_by_msg[body]
                    break

        if tmpl_rec_to_set:
            if not (isinstance(curr_tmpl_link, list) and curr_tmpl_link and curr_tmpl_link[0] == tmpl_rec_to_set):
                updates["Template"] = [tmpl_rec_to_set]
            if f.get(tmpl_text_field) != tmpl_rec_to_set:
                updates[tmpl_text_field] = tmpl_rec_to_set

        # ---------- Campaign ----------
        camp_text_field = TEXT_RECORD_ID_FIELDS["Campaign"]  # 'Campaign Record ID'
        curr_camp_link = f.get("Campaign")
        curr_camp_text = f.get(camp_text_field)
        camp_rec_to_set: Optional[str] = None

        if is_rec_id(curr_camp_text):
            camp_rec_to_set = curr_camp_text
        elif isinstance(curr_camp_link, list) and curr_camp_link and is_rec_id(curr_camp_link[0]):
            camp_rec_to_set = curr_camp_link[0]
        else:
            # try Campaign ID / Campaign Name on conversation if you store one
            for k in ("Campaign ID", "campaign_id", "Campaign", "campaign"):
                v = f.get(k)
                if v in (None, ""):
                    continue
                key = str(v).strip()
                if key in camp_map:
                    camp_rec_to_set = camp_map[key]
                    break

        if camp_rec_to_set:
            if not (isinstance(curr_camp_link, list) and curr_camp_link and curr_camp_link[0] == camp_rec_to_set):
                updates["Campaign"] = [camp_rec_to_set]
            if f.get(camp_text_field) != camp_rec_to_set:
                updates[camp_text_field] = camp_rec_to_set

        # ---------- Prospect ----------
        pros_text_field = TEXT_RECORD_ID_FIELDS["Prospect"]  # 'Prospect Record ID'
        curr_pros_link = f.get("Prospect")
        curr_pros_text = f.get(pros_text_field)
        pros_rec_to_set: Optional[str] = None

        if is_rec_id(curr_pros_text):
            pros_rec_to_set = curr_pros_text
        elif isinstance(curr_pros_link, list) and curr_pros_link and is_rec_id(curr_pros_link[0]):
            pros_rec_to_set = curr_pros_link[0]
        else:
            # derive from phone or Lead ID (if Prospects carry them)
            p10 = extract_conv_phone10(f)
            if p10 and p10 in pros_phone:
                pros_rec_to_set = pros_phone[p10]
            else:
                for k in ("Lead ID", "lead_id", "leadId"):
                    v = f.get(k)
                    if v in (None, ""):
                        continue
                    if isinstance(v, int) and v in pros_id_int:
                        pros_rec_to_set = pros_id_int[v]
                        break
                    s = str(v).strip()
                    if s and s in pros_id_str:
                        pros_rec_to_set = pros_id_str[s]
                        break

        if pros_rec_to_set:
            if not (isinstance(curr_pros_link, list) and curr_pros_link and curr_pros_link[0] == pros_rec_to_set):
                updates["Prospect"] = [pros_rec_to_set]
            if f.get(pros_text_field) != pros_rec_to_set:
                updates[pros_text_field] = pros_rec_to_set

        # ---------- commit ----------
        if not updates:
            already_ok += 1
        else:
            safe_update(conv_tbl, cid, updates)
            fixed += 1

    print("\n=== Record-ID Link Backfill ===")
    print(f"Scanned Conversations: {scanned}")
    print(f"Updated rows:          {fixed}")
    print(f"No changes needed:     {already_ok}")
    print("✅ Done.")


# ------------- run -------------
if __name__ == "__main__":
    print(f"=== fix_links_by_record_id start (DRY_RUN={str(DRY_RUN).upper()}) ===")
    main()
