#!/usr/bin/env python3
from __future__ import annotations
import os, re
from typing import Dict, List, Optional, Tuple, Any

# optional: load .env automatically
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from sms.tables import get_table as _get_table

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# ---------------- ENV + TABLE HELPERS ----------------
def _req(env: str) -> str:
    v = os.getenv(env)
    if not v:
        raise RuntimeError(f"Missing required env var: {env}")
    return v

def _choose_env(candidates: List[str]) -> str:
    for c in candidates:
        if os.getenv(c):
            return c
    raise RuntimeError(f"Missing required env var (any of): {', '.join(candidates)}")

# accept either spelling
LEADS_BASE_ENV = _choose_env(["LEADS_CONVOS_BASE", "LEADS_CONVO_BASE"])

# all tables in same base (your setup)
TABLE_TO_ENV: Dict[str, tuple[List[str], str]] = {
    "Leads":         ([LEADS_BASE_ENV], "LEADS_TABLE"),
    "Conversations": ([LEADS_BASE_ENV], "CONVERSATIONS_TABLE"),
    "Campaigns":     ([LEADS_BASE_ENV], "CAMPAIGNS_TABLE"),
    "Templates":     ([LEADS_BASE_ENV], "TEMPLATES_TABLE"),
    "Prospects":     ([LEADS_BASE_ENV], "PROSPECTS_TABLE"),
    "Drip Queue":    ([LEADS_BASE_ENV], "DRIP_QUEUE_TABLE"),
}

def get_table_env(name: str):
    if name not in TABLE_TO_ENV:
        raise RuntimeError(f"TABLE_TO_ENV missing mapping for '{name}'")
    base_env_candidates, table_env = TABLE_TO_ENV[name]
    _req("AIRTABLE_API_KEY")
    chosen_base_env = _choose_env(base_env_candidates)
    _req(table_env)
    try:
        return _get_table("AIRTABLE_API_KEY", chosen_base_env, table_env, name)
    except Exception as e:
        print(f"❌ get_table_env('{name}') failed: {e}")
        return None

# ---------------- UTILS ----------------
def last10(s: Optional[str]) -> str:
    if s is None: return ""
    return re.sub(r"[^0-9]", "", str(s))[-10:]

def link_record(table_name: str, rec_id: str, field: str, link_ids: List[str]):
    if DRY_RUN:
        print(f"[DRY] update {table_name}:{rec_id} -> {field}={link_ids}")
        return
    tbl = get_table_env(table_name)
    if not tbl:
        print(f"❌ Cannot link; table '{table_name}' unavailable")
        return
    try:
        tbl.update(rec_id, {field: link_ids})
    except Exception as e:
        print(f"⚠️  Failed linking {table_name}:{rec_id} → {field}: {e}")

# ---------------- LEADS MAPS ----------------
LEAD_PHONE_FIELDS = ["phone","Phone","Phone (Raw)","Phone E164","Primary Phone","Mobile","Owner Phone"]

def build_leads_phone_map() -> Dict[str,str]:
    """{ last10(phone) -> Lead.RecordID }"""
    tbl = get_table_env("Leads")
    mapping: Dict[str,str] = {}
    if not tbl: return mapping
    try:
        for r in tbl.all():
            f = r.get("fields",{})
            rid = f.get("Record ID")
            if not rid: continue
            for k in LEAD_PHONE_FIELDS:
                val = f.get(k)
                if val:
                    p10 = last10(val)
                    if p10:
                        mapping[p10] = rid
                        break
        print(f"ℹ️  Leads phone map size: {len(mapping)} (fields tried: {LEAD_PHONE_FIELDS})")
    except Exception as e:
        print(f"⚠️  Leads read error: {e}")
    return mapping

def build_leads_leadid_map() -> Tuple[Dict[str,str], set[str], set[int]]:
    """
    Returns:
      - mapping_str: { <LeadID as str> -> <Lead.RecordID> }
      - id_str_set:  set of LeadID as strings
      - id_int_set:  set of LeadID as ints (for numeric storage)
    """
    tbl = get_table_env("Leads")
    mapping_str: Dict[str,str] = {}
    id_str_set: set[str] = set()
    id_int_set: set[int] = set()
    if not tbl: 
        print("⚠️  Leads table unavailable")
        return mapping_str, id_str_set, id_int_set
    try:
        for r in tbl.all():
            f = r.get("fields",{})
            rid = f.get("Record ID")
            lead_id_val = f.get("Lead ID")
            if not rid or lead_id_val in (None, ""): 
                continue
            # normalize both int and str forms
            if isinstance(lead_id_val, int):
                id_int_set.add(lead_id_val)
                s = str(lead_id_val)
            else:
                s = str(lead_id_val).strip()
                # best-effort int capture for cross-type compare
                try:
                    id_int_set.add(int(s))
                except Exception:
                    pass
            mapping_str[s] = rid
            id_str_set.add(s)
        print(f"ℹ️  Leads LeadID map size: {len(mapping_str)} (supports str & int matching)")
    except Exception as e:
        print(f"⚠️  Leads LeadID read error: {e}")
    return mapping_str, id_str_set, id_int_set

# ---------------- CONVERSATION PHONE EXTRACTION ----------------
CONV_TO_FIELDS   = ["to_number","To","to","recipient","phone"]
CONV_FROM_FIELDS = ["from_number","From","from","sender","phone"]
CONV_DIRECTION_FIELDS = ["direction","Direction"]

def extract_conv_phone(cf: dict) -> str:
    direction_val = ""
    for d in CONV_DIRECTION_FIELDS:
        if cf.get(d):
            direction_val = str(cf.get(d)).upper()
            break
    candidates = []
    if direction_val == "OUTBOUND":
        candidates = CONV_TO_FIELDS + CONV_FROM_FIELDS
    elif direction_val == "INBOUND":
        candidates = CONV_FROM_FIELDS + CONV_TO_FIELDS
    else:
        candidates = CONV_TO_FIELDS + CONV_FROM_FIELDS
    for k in candidates:
        v = cf.get(k)
        if v:
            p10 = last10(v)
            if p10: return p10
    return ""

# ---------------- BACKFILLS ----------------
CONV_LEAD_ID_FIELDS = ["Lead ID","lead_id","leadId","lead","Lead","lead_ref","lead_ref_id"]

def conversations_to_leads() -> Tuple[int,int,int]:
    conv_tbl = get_table_env("Conversations")
    if not conv_tbl: return (0,0,0)

    lead_map_phone  = build_leads_phone_map()
    lead_map_leadid, lead_ids_str, lead_ids_int = build_leads_leadid_map()

    linked = already = scanned = phone_hits = id_hits = auto_id_hits = 0
    try:
        rows = conv_tbl.all()
    except Exception as e:
        print(f"⚠️  Conversations read error: {e}"); return (0,0,0)

    for c in rows:
        scanned += 1
        f = c.get("fields",{})
        if f.get("Lead"):
            already += 1
            continue

        # 1) phone-based
        p10 = extract_conv_phone(f)
        rid = lead_map_phone.get(p10) if p10 else None
        if rid:
            link_record("Conversations", c["id"], "Lead", [rid])
            linked += 1; phone_hits += 1; continue

        # 2) explicit lead-id fields in Conversations
        matched = False
        for k in CONV_LEAD_ID_FIELDS:
            val = f.get(k)
            if val in (None, ""): 
                continue
            # support both str and int values on the conv side
            if isinstance(val, int) and val in lead_ids_int:
                rid2 = lead_map_leadid.get(str(val))
                if rid2:
                    link_record("Conversations", c["id"], "Lead", [rid2])
                    linked += 1; id_hits += 1; matched = True
                    break
            else:
                s = str(val).strip()
                rid2 = lead_map_leadid.get(s)
                if rid2:
                    link_record("Conversations", c["id"], "Lead", [rid2])
                    linked += 1; id_hits += 1; matched = True
                    break
        if matched:
            continue

        # 3) auto-discover: scan all simple field values for exact LeadID matches
        for key, val in f.items():
            if val in (None, ""): 
                continue
            # strings
            if isinstance(val, str) and val in lead_ids_str:
                rid3 = lead_map_leadid.get(val)
                if rid3:
                    link_record("Conversations", c["id"], "Lead", [rid3])
                    linked += 1; auto_id_hits += 1; matched = True
                    break
            # ints
            if isinstance(val, int) and val in lead_ids_int:
                rid3 = lead_map_leadid.get(str(val))
                if rid3:
                    link_record("Conversations", c["id"], "Lead", [rid3])
                    linked += 1; auto_id_hits += 1; matched = True
                    break
            # simple lists of scalars
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, int) and item in lead_ids_int:
                        rid3 = lead_map_leadid.get(str(item))
                        if rid3:
                            link_record("Conversations", c["id"], "Lead", [rid3])
                            linked += 1; auto_id_hits += 1; matched = True
                            break
                    if isinstance(item, str) and item in lead_ids_str:
                        rid3 = lead_map_leadid.get(item)
                        if rid3:
                            link_record("Conversations", c["id"], "Lead", [rid3])
                            linked += 1; auto_id_hits += 1; matched = True
                            break
                if matched: break

    print(
        "Conversations→Leads "
        f"linked={linked} (phone={phone_hits}, lead_id_fields={id_hits}, auto_id={auto_id_hits}), "
        f"already_linked={already}, scanned={scanned}"
    )
    return linked, already, scanned

def conversations_to_templates() -> Tuple[int,int]:
    conv_tbl = get_table_env("Conversations")
    tmpl_tbl = get_table_env("Templates")
    if not conv_tbl or not tmpl_tbl:
        print("⚠️  Skip Templates linking: table unavailable")
        return (0,0)

    TEMPLATE_BODY_FIELDS = ["Message","Body","Text","Content"]  # your Templates use 'Message'
    tmpl_map: Dict[str,str] = {}
    try:
        for r in tmpl_tbl.all():
            f = r.get("fields",{})
            rid = f.get("Record ID")
            if not rid: continue
            for col in TEMPLATE_BODY_FIELDS:
                body = (f.get(col) or "").strip()
                if body:
                    tmpl_map[body] = rid
                    break
        print(f"ℹ️  Templates body map size: {len(tmpl_map)} (fields tried: {TEMPLATE_BODY_FIELDS})")
    except Exception as e:
        print(f"⚠️  Templates read error: {e}")
        return (0,0)

    # Guess conversation “body” fields. Your sample didn’t show any; this will likely be 0 until
    # a body/message column exists in Conversations.
    BODY_FIELDS_GUESS = ["Body","body","Message","text","message"]
    linked = scanned = 0
    try:
        convs = conv_tbl.all()
    except Exception as e:
        print(f"⚠️  Conversations read error: {e}")
        return (0,0)

    for c in convs:
        scanned += 1
        f = c.get("fields",{})
        dir_val = (f.get("direction") or f.get("Direction") or "").upper()
        if dir_val and dir_val != "OUTBOUND": 
            continue
        if f.get("Template"): 
            continue
        body = ""
        for col in BODY_FIELDS_GUESS:
            val = (f.get(col) or "").strip()
            if val:
                body = val; break
        if not body: 
            continue
        rid = tmpl_map.get(body)
        if rid:
            link_record("Conversations", c["id"], "Template", [rid])
            linked += 1
    print(f"Conversations→Templates linked={linked}, scanned={scanned}")
    return linked, scanned

def conversations_to_campaigns() -> Tuple[int,int]:
    print("⏭️  Conversations→Campaigns skipped (no join key detected).")
    return (0,0)

def conversations_to_prospects() -> Tuple[int,int]:
    conv_tbl = get_table_env("Conversations")
    pros_tbl = get_table_env("Prospects")
    if not conv_tbl or not pros_tbl:
        print("⚠️  Skip Prospects linking: table unavailable")
        return (0,0)
    PROSPECT_PHONE_FIELDS = ["phone","Phone","Phone (Raw)","Phone E164","Primary Phone","Mobile"]
    pros_map: Dict[str,str] = {}
    try:
        for r in pros_tbl.all():
            f = r.get("fields",{})
            rid = f.get("Record ID")
            if not rid: continue
            for k in PROSPECT_PHONE_FIELDS:
                val = f.get(k)
                if val:
                    p10 = last10(val)
                    if p10:
                        pros_map[p10] = rid
                        break
        print(f"ℹ️  Prospects phone map size: {len(pros_map)}")
    except Exception as e:
        print(f"⚠️  Prospects read error: {e}")
        return (0,0)

    linked = scanned = 0
    try:
        convs = conv_tbl.all()
    except Exception as e:
        print(f"⚠️  Conversations read error: {e}")
        return (0,0)
    for c in convs:
        scanned += 1        # link if we can match phone
        f = c.get("fields",{})
        if f.get("Prospect"): 
            continue
        p10 = extract_conv_phone(f)
        rid = pros_map.get(p10)
        if rid:
            link_record("Conversations", c["id"], "Prospect", [rid])
            linked += 1
    print(f"Conversations→Prospects linked={linked}, scanned={scanned}")
    return linked, scanned

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("=== Backfill start (DRY_RUN=" + str(DRY_RUN).upper() + ") ===")
    l_linked, l_already, l_scanned = conversations_to_leads()
    c_linked, c_scanned            = conversations_to_campaigns()  # intentionally skipped until we confirm a join key
    t_linked, t_scanned            = conversations_to_templates()
    p_linked, p_scanned            = conversations_to_prospects()

    print("\n=== Summary ===")
    print(f"Leads:       linked={l_linked} already_linked={l_already} scanned={l_scanned}")
    print(f"Campaigns:   linked={c_linked} scanned={c_scanned}")
    print(f"Templates:   linked={t_linked} scanned={t_scanned}")
    print(f"Prospects:   linked={p_linked} scanned={p_scanned}")
    print("✅ Backfill complete.")