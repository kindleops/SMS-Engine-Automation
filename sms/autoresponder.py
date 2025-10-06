# sms/autoresponder.py
from __future__ import annotations

import os
import re
import random
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Tuple, Optional

from sms.message_processor import MessageProcessor
from sms import templates as local_templates   # fallback if Airtable missing
from sms.ai_closer import run_ai_closer        # 🚀 AI takeover after Stage 3

try:
    from pyairtable import Table
except ImportError:
    Table = None

# --- ENV CONFIG ---
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
LEADS_TABLE        = os.getenv("LEADS_TABLE", "Leads")
PROSPECTS_TABLE    = os.getenv("PROSPECTS_TABLE", "Prospects")
TEMPLATES_TABLE    = os.getenv("TEMPLATES_TABLE", "Templates")
DRIP_QUEUE_TABLE   = os.getenv("DRIP_QUEUE_TABLE", "Drip Queue")

# -----------------
# Airtable Clients
# -----------------
def _mk_table(table_name: str) -> Any:
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
    return Table(api_key, base_id, table_name) if api_key and base_id and Table else None

@lru_cache(maxsize=None)
def get_convos():    return _mk_table(CONVERSATIONS_TABLE)

@lru_cache(maxsize=None)
def get_leads():     return _mk_table(LEADS_TABLE)

@lru_cache(maxsize=None)
def get_prospects(): return _mk_table(PROSPECTS_TABLE)

@lru_cache(maxsize=None)
def get_templates(): return _mk_table(TEMPLATES_TABLE)

@lru_cache(maxsize=None)
def get_drip():      return _mk_table(DRIP_QUEUE_TABLE)

# -----------------
# Helpers
# -----------------
def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()

def _digits(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    d = "".join(re.findall(r"\d+", s))
    return d if len(d) >= 10 else None

def last10(s: Any) -> Optional[str]:
    d = _digits(s)
    return d[-10:] if d else None

def _norm(s: Any) -> Any:
    return re.sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s

def _existing_fields(table: Any) -> set[str]:
    try:
        probe = table.all(max_records=1)
        return set((probe[0]["fields"].keys()) if probe else [])
    except Exception:
        return set()

def _safe_create(table: Any, payload: Dict) -> Optional[Dict]:
    if not table or not payload:
        return None
    try:
        fields = _existing_fields(table)
        to_send = {k: v for k, v in payload.items() if (not fields) or (k in fields)}
        if to_send:
            return table.create(to_send)
    except Exception:
        traceback.print_exc()
    return None

def _safe_update(table: Any, rec_id: str, payload: Dict) -> Optional[Dict]:
    if not table or not rec_id or not payload:
        return None
    try:
        fields = _existing_fields(table)
        to_send = {k: v for k, v in payload.items() if (not fields) or (k in fields)}
        if to_send:
            return table.update(rec_id, to_send)
    except Exception:
        traceback.print_exc()
    return None

def _compose_personalization(pf: Dict[str, Any]) -> Dict[str, Any]:
    full = pf.get("Owner Name") or f"{pf.get('Owner First Name','') or ''} {pf.get('Owner Last Name','') or ''}".strip()
    first = (full or "").split(" ")[0] if full else "there"
    address = pf.get("Property Address") or pf.get("Address") or "your property"
    return {"First": first, "Address": address}

def _record_matches_phone(f: Dict[str, Any], l10: str) -> bool:
    for k, v in f.items():
        if not isinstance(v, str):
            continue
        dv = last10(v)
        if dv and dv == l10:
            return True
    return False

def _find_prospect_by_phone(phone: str) -> Optional[Dict]:
    """Python-side match on last10; safe for ~<10k rows."""
    prospects = get_prospects()
    if not prospects or not phone:
        return None
    try:
        l10 = last10(phone)
        if not l10:
            return None
        rows = prospects.all()
        for r in rows:
            if _record_matches_phone(r.get("fields", {}), l10):
                return r
        return None
    except Exception:
        traceback.print_exc()
        return None

def _escape(s: str) -> str:
    return (s or "").replace("'", "\\'")

def set_phone_verified(phone_number: str, verified: bool = True):
    """Mark Phone 1/2 Verified on the matching Prospect row that owns this number."""
    prospects = get_prospects()
    if not (prospects and phone_number):
        return
    try:
        pn10 = last10(phone_number)
        if not pn10:
            return
        rows = prospects.all()
        target = None
        for r in rows:
            f = r.get("fields", {})
            for key in ("Phone 1","Phone 1 (from Linked Owner)","Phone 2","Phone 2 (from Linked Owner)","Phone","phone","Mobile","Cell","Phone Number"):
                dv = last10(f.get(key))
                if dv and dv == pn10:
                    target = (r, key)
                    break
            if target:
                break
        if not target:
            return
        rec, key = target
        pf = rec.get("fields", {})
        patch = {}
        if key in ("Phone 1","Phone 1 (from Linked Owner)"):
            patch["Phone 1 Verified"] = verified
            if "Phone 1 Ownership Verified" in pf:
                patch["Phone 1 Ownership Verified"] = verified
        else:
            patch["Phone 2 Verified"] = verified
            if "Phone 2 Ownership Verified" in pf:
                patch["Phone 2 Ownership Verified"] = verified
        _safe_update(prospects, rec["id"], patch)
    except Exception as e:
        print(f"⚠️ set_phone_verified failed: {e}")

# For copying data from Prospects → Leads, if we promote
FIELD_MAP = {
    "phone": "phone",
    "Property ID": "Property ID",
    "Owner Name": "Owner Name",
    "Owner First Name": "Owner First Name",
    "Owner Last Name": "Owner Last Name",
    "Property Address": "Address",      # Leads may call this "Address"
    "Market": "Market",
    "Synced From": "Sync Source",
    "Source List": "List",
    "Property Type": "Property Type",
}

STAGE_MAP = {
    "intro": "Stage 1 - Owner Check",
    "followup_yes": "Stage 2 - Offer Interest",
    "followup_no": "Stage 2 - Offer Declined",
    "followup_wrong": "Stage 2 - Wrong Number",
    "not_owner": "Stage 2 - Not Owner",
    "who_is_this": "Stage 1 - Identity",
    "how_get_number": "Stage 1 - Compliance",
    "price_response": "Stage 3 - Price Discussion",
    "condition_response": "Stage 3 - Condition Discussion",
    "optout": "Opt-Out",
    "neutral": "Stage 1 - Owner Check",
    "interest": "Stage 2 - Offer Interest",
}

STOP_WORDS = {"stop","unsubscribe","remove","quit","cancel","end"}

def classify_intent(body: str) -> str:
    text = (body or "").lower().strip()
    # Highest priority: DNC
    if any(w in text for w in STOP_WORDS):
        return "optout"
    # Identity / compliance
    if any(p in text for p in ["who is this","who's this","whos this","who are you","who dis","identify yourself"]):
        return "who_is_this"
    if any(p in text for p in [
        "how did you get my number","how did you get my #","how you get my number",
        "why do you have my number","where did you get my number","how got my number"
    ]):
        return "how_get_number"
    # Core intents
    if re.search(r"\b(yes|yeah|yep|sure|i do|that's me|of course)\b", text):
        return "followup_yes"
    if re.search(r"\b(no|nope|nah|not interested)\b", text):
        return "followup_no"
    if re.search(r"\b(wrong|don't own|not mine|wrong number)\b", text):
        return "followup_wrong"
    if any(w in text for w in ["price","asking","$"," k","k ", " number you have in mind"]):
        return "price_response"
    if any(w in text for w in ["condition","repairs","needs work","renovated","tenant"]):
        return "condition_response"
    if any(w in text for w in ["maybe","not sure","thinking","depends"]):
        return "neutral"
    return "intro"

def _choose_template(intent: str, fields: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Looks up a row in Templates where {Internal ID} == intent.
    Falls back to local templates file you provided.
    """
    templates = get_templates()
    msg, tid = None, None
    if templates:
        try:
            rows = templates.all()
            cands = []
            for r in rows:
                tf = r.get("fields", {})
                internal = (tf.get("Internal ID") or tf.get("intent") or "").strip().lower()
                if internal == intent:
                    cands.append(r)
            if cands:
                chosen = random.choice(cands)
                tid = chosen["id"]
                raw = chosen["fields"].get("Message") or ""
                msg = raw.format(First=fields.get("First","there"), Address=fields.get("Address","your property"))
        except Exception as e:
            print(f"⚠️ Template lookup failed: {e}")

    if not msg:
        # fallback – local list
        msg = local_templates.get_template(intent, fields)
    return msg or "Thanks for the reply.", tid

def promote_to_lead(phone_number: str, source: str = "Autoresponder") -> Tuple[Optional[str], Optional[str]]:
    leads = get_leads()
    if not phone_number or not leads:
        return None, None
    try:
        # If exists (match on last10 anywhere), return it
        existing = leads.all()
        l10 = last10(phone_number)
        for r in existing:
            lf = r.get("fields", {})
            if _record_matches_phone(lf, l10):
                return r["id"], lf.get("Property ID")

        # Copy from Prospect
        prospect_row = _find_prospect_by_phone(phone_number)
        fields, property_id = {}, None
        if prospect_row:
            pf = prospect_row["fields"]
            for src, dst in FIELD_MAP.items():
                if src in pf and pf.get(src) is not None:
                    fields[dst] = pf.get(src)
            property_id = pf.get("Property ID")

        new_lead = _safe_create(leads, {
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source,
        })
        if new_lead:
            print(f"✨ Promoted {phone_number} → Lead")
            return new_lead["id"], property_id
        return None, property_id
    except Exception as e:
        print(f"⚠️ Lead promotion failed: {e}")
        return None, None

def update_lead_activity(lead_id: str, body: str, direction: str, intent: str = None):
    leads = get_leads()
    if not lead_id or not leads:
        return
    try:
        updates = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        if intent == "followup_yes":
            updates["Lead Status"] = "Interested"
        if intent == "optout":
            updates["Lead Status"] = "DNC"
        _safe_update(leads, lead_id, updates)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")

# -----------------
# Core Autoresponder
# -----------------
def run_autoresponder(limit: int = 50, view: str = "Unprocessed Inbounds"):
    convos = get_convos()
    drip   = get_drip()
    if not convos:
        return {"ok": False, "processed": 0, "breakdown": {}, "errors": ["Missing Conversations table"]}

    processed, breakdown, errors = 0, {}, []
    processed_by = os.getenv("PROCESSED_BY_LABEL", "Autoresponder")

    try:
        rows = convos.all(view=view, max_records=limit)
        for r in rows:
            f = r.get("fields", {})
            msg_id  = r.get("id")
            from_num= f.get("phone") or f.get("From")
            to_did  = f.get("To")     # DID that received inbound (for thread continuity)
            body    = f.get("message") or f.get("Body")
            if not from_num or not body:
                continue

            # Context & personalization
            prospect_row = _find_prospect_by_phone(from_num)
            pf = prospect_row.get("fields", {}) if prospect_row else {}
            prospect_id = prospect_row["id"] if prospect_row else None
            pers = _compose_personalization(pf)
            market = f.get("Market") or pf.get("Market")
            campaign_id = None
            camp_link = f.get("Campaign") or []
            if isinstance(camp_link, list) and camp_link:
                campaign_id = camp_link[0]

            print(f"🤖 {processed_by} inbound {from_num}: {body}")
            intent = classify_intent(body)

            # Verify responder (unless clearly wrong/optout)
            if intent not in ("followup_wrong", "optout"):
                set_phone_verified(from_num, True)

            # STOP → DNC; no reply
            if intent == "optout":
                _safe_update(convos, msg_id, {
                    "status": "DNC",
                    "processed_by": processed_by,
                    "processed_at": iso_timestamp(),
                    "intent_detected": intent,
                    "stage": STAGE_MAP.get(intent, "Opt-Out"),
                })
                lead_id, _ = promote_to_lead(from_num, source=processed_by)
                update_lead_activity(lead_id, body, "IN", intent="optout")
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1
                continue

            # 🚀 Stage 3 → AI takeover
            if intent in ("price_response", "condition_response"):
                try:
                    ai_result = run_ai_closer(from_num, body, f)
                    _safe_update(convos, msg_id, {
                        "status": "AI_HANDOFF",
                        "processed_by": "AI Closer",
                        "processed_at": iso_timestamp(),
                        "intent_detected": intent,
                        "stage": STAGE_MAP.get(intent, "Stage 3 - AI Closing"),
                        "ai_result": str(ai_result),
                    })
                    processed += 1
                    breakdown[intent] = breakdown.get(intent, 0) + 1
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"AI closer failed: {e}"})
                continue

            # Stage 1–2 normal flow → choose template + queue reply
            reply_text, template_id = _choose_template(intent, pers)
            lead_id, property_id = promote_to_lead(from_num, source=processed_by)

            queued_ok = False
            if drip:
                try:
                    payload = {
                        "Prospect": [prospect_id] if prospect_id else None,
                        "Campaign": [campaign_id] if campaign_id else None,
                        "Template": [template_id] if template_id else None,
                        "Market": market,
                        "phone": from_num,
                        "message_preview": reply_text,
                        "from_number": to_did,  # reply from same DID if present
                        "status": "QUEUED",
                        "next_send_date": iso_timestamp(),
                        "Property ID": property_id,
                    }
                    payload = {k: v for k, v in payload.items() if v is not None}
                    _safe_create(drip, payload)
                    queued_ok = True
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"Queue failed: {e}"})

            if not queued_ok:
                # fallback immediate send
                send_result = MessageProcessor.send(
                    phone=from_num,
                    body=reply_text,
                    lead_id=lead_id,
                    property_id=property_id,
                    direction="OUT",
                )
                if send_result.get("status") != "sent":
                    errors.append({"phone": from_num, "error": send_result.get("error", "Send failed")})

            # Mark conversation processed + update lead trail
            _safe_update(convos, msg_id, {
                "status": "RESPONDED",
                "processed_by": processed_by,
                "processed_at": iso_timestamp(),
                "intent_detected": intent,
                "stage": STAGE_MAP.get(intent, "Stage 1 - Owner Check"),
                "template_id": template_id,
            })
            if lead_id:
                update_lead_activity(lead_id, body, "IN", intent=intent)

            processed += 1
            breakdown[intent] = breakdown.get(intent, 0) + 1

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        errors.append(str(e))

    return {"ok": processed > 0, "processed": processed, "breakdown": breakdown, "errors": errors}
