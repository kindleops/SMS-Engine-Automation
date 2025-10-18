# sms/autoresponder.py
from __future__ import annotations

import os
import re
import random
import traceback
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone

# Only executed if we actually call it (disabled in tests below)
from sms.followup_flow import schedule_from_response

# --- Core project config (one source of truth) ---
from sms.config import (
    settings,
    conversations,
    leads as leads_tbl,
    prospects as prospects_tbl,
    templates as templates_tbl,
    drip_queue as drip_tbl,
    remap_existing_only,
    CONV_FIELDS,
    PHONE_FIELDS,
)

# Local fallbacks / modules
try:
    from sms.message_processor import MessageProcessor
except Exception:
    MessageProcessor = None  # type: ignore

try:
    from sms import templates as local_templates
except Exception:
    local_templates = None  # type: ignore

try:
    from sms.ai_closer import run_ai_closer
except Exception:

    def run_ai_closer(*_args, **_kwargs):  # type: ignore
        return {"ok": False, "note": "ai_closer unavailable"}


# -------------------------------------------------
# Test-mode detection (runtime so pytest env is visible)
# -------------------------------------------------
def _is_test_mode() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("UNIT_TEST") or os.getenv("TEST_MODE"))


# -------------------------------------------------
# Back-compat shims so tests can monkeypatch these
# -------------------------------------------------
def get_convos():
    return conversations()


def get_leads():
    return leads_tbl()


def get_prospects():
    return prospects_tbl()


def get_templates():
    return templates_tbl()


def get_drip_queue():
    return drip_tbl()


# -----------------
# Constants / Maps
# -----------------
FIELD_MAP = {
    "phone": "phone",
    "Property ID": "Property ID",
    "Owner Name": "Owner Name",
    "Owner First Name": "Owner First Name",
    "Owner Last Name": "Owner Last Name",
    "Property Address": "Address",
    "Market": "Market",
    "Synced From": "Sync Source",
    "Source List": "List",
    "Property Type": "Property Type",
}

STAGE_MAP = {
    "intro": "Stage 1 - Owner Check",
    "who_is_this": "Stage 1 - Identity",
    "how_get_number": "Stage 1 - Compliance",
    "neutral": "Stage 1 - Owner Check",
    "followup_yes": "Stage 2 - Offer Interest",
    "followup_no": "Stage 2 - Offer Declined",
    "followup_wrong": "Stage 2 - Wrong Number",
    "not_owner": "Stage 2 - Not Owner",
    "interest": "Stage 2 - Offer Interest",
    "price_response": "Stage 3 - Price Discussion",
    "condition_response": "Stage 3 - Condition Discussion",
    "optout": "Opt-Out",
}

STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}

STATUS_ICON = {
    "QUEUED": "⏳",
    "READY": "⏳",
    "SENDING": "🔄",
    "SENT": "✅",
    "DELIVERED": "✅",
    "FAILED": "❌",
    "CANCELLED": "❌",
}


# -----------------
# Small helpers
# -----------------
def iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _digits(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    d = "".join(__import__("re").findall(r"\d+", s))
    return d if len(d) >= 10 else None


def last10(s: Any) -> Optional[str]:
    d = _digits(s)
    return d[-10:] if d else None


def _norm(s: Any) -> Any:
    return __import__("re").sub(r"[^a-z0-9]+", "", s.strip().lower()) if isinstance(s, str) else s


def _record_matches_phone(f: Dict[str, Any], l10: str) -> bool:
    for _, v in f.items():
        if not isinstance(v, str):
            continue
        if last10(v) == l10:
            return True
    return False


def _safe_create(table, payload: Dict) -> Optional[Dict]:
    if not table or not payload:
        return None
    try:
        to_send = remap_existing_only(table, payload)
        return table.create(to_send) if to_send else None
    except Exception:
        traceback.print_exc()
        return None


def _safe_update(table, rec_id: str, payload: Dict) -> Optional[Dict]:
    if not (table and rec_id and payload):
        return None
    try:
        to_send = remap_existing_only(table, payload)
        return table.update(rec_id, to_send) if to_send else None
    except Exception:
        traceback.print_exc()
        return None


def _compose_personalization(pf: Dict[str, Any]) -> Dict[str, Any]:
    full = pf.get("Owner Name") or f"{pf.get('Owner First Name', '') or ''} {pf.get('Owner Last Name', '') or ''}".strip()
    first = (full or "").split(" ")[0] if full else "there"
    address = pf.get("Property Address") or pf.get("Address") or "your property"
    return {"First": first, "Address": address}


def _find_prospect_by_phone(phone: str) -> Optional[Dict]:
    p = get_prospects()
    if not (p and phone):
        return None
    try:
        l10 = last10(phone)
        if not l10:
            return None
        for r in p.all():
            if _record_matches_phone(r.get("fields", {}), l10):
                return r
        return None
    except Exception:
        traceback.print_exc()
        return None


def _first_from_fields(f: Dict[str, Any]) -> Optional[str]:
    for k in ["Owner First Name", "First Name", "First", "Name", "Owner Name"]:
        v = f.get(k)
        if isinstance(v, str) and v.strip():
            return v.split()[0]
    return None


def _pref_phone_from_fields(f: Dict[str, Any]) -> Optional[str]:
    p1 = f.get("Phone 1") or f.get("Phone 1 (from Linked Owner)")
    p2 = f.get("Phone 2") or f.get("Phone 2 (from Linked Owner)")

    if f.get("Phone 1 Verified") or f.get("Phone 1 Ownership Verified"):
        d = _digits(p1)
        if d:
            return d
    if f.get("Phone 2 Verified") or f.get("Phone 2 Ownership Verified"):
        d = _digits(p2)
        if d:
            return d

    for k in PHONE_FIELDS:
        d = _digits(f.get(k))
        if d:
            return d
    return None


# -----------------
# Intent detection
# -----------------
def classify_intent(body: str) -> str:
    text = (body or "").lower().strip()

    if any(w in text for w in STOP_WORDS):
        return "optout"

    if any(p in text for p in ["who is this", "who's this", "whos this", "who are you", "who dis", "identify yourself"]):
        return "who_is_this"
    if any(
        p in text
        for p in [
            "how did you get my number",
            "how did you get my #",
            "how you get my number",
            "why do you have my number",
            "where did you get my number",
            "how got my number",
        ]
    ):
        return "how_get_number"

    if re.search(r"\b(yes|yeah|yep|sure|i do|that's me|of course)\b", text):
        return "followup_yes"
    if re.search(r"\b(no|nope|nah|not interested)\b", text):
        return "followup_no"
    if re.search(r"\b(wrong|don't own|not mine|wrong number)\b", text):
        return "followup_wrong"
    if any(w in text for w in ["price", "asking", "$", " k", "k ", " number you have in mind"]):
        return "price_response"
    if any(w in text for w in ["condition", "repairs", "needs work", "renovated", "tenant"]):
        return "condition_response"
    if any(w in text for w in ["maybe", "not sure", "thinking", "depends"]):
        return "neutral"

    return "intro"


def _choose_template(intent: str, fields: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    t = get_templates()
    msg, tid = None, None

    if t:
        try:
            rows = t.all()
            pool = []
            for r in rows:
                tf = r.get("fields", {})
                internal = (tf.get("Internal ID") or tf.get("intent") or "").strip().lower()
                if internal == intent:
                    pool.append(r)
            if pool:
                chosen = random.choice(pool)
                tid = chosen["id"]
                raw = chosen["fields"].get("Message") or ""
                first = fields.get("First") or _first_from_fields(fields) or "there"
                addr = fields.get("Address") or fields.get("Property Address") or fields.get("Address") or "your property"
                try:
                    msg = raw.format(First=first, Address=addr)
                except Exception:
                    msg = raw
        except Exception as e:
            print(f"⚠️ Template lookup failed: {e}")

    if not msg and local_templates:
        try:
            msg = local_templates.get_template(intent, fields)
        except Exception:
            msg = None

    return (msg or "Thanks for the reply.", tid)


# -----------------
# Lead promotion & updates
# -----------------
def promote_to_lead(phone_number: str, source: str = "Autoresponder") -> Tuple[Optional[str], Optional[str]]:
    ltbl = get_leads()
    if not (phone_number and ltbl):
        return (None, None)
    try:
        l10 = last10(phone_number)
        for r in ltbl.all():
            lf = r.get("fields", {})
            if _record_matches_phone(lf, l10):
                return r["id"], lf.get("Property ID")

        fields: Dict[str, Any] = {}
        property_id = None
        pr = _find_prospect_by_phone(phone_number)
        if pr:
            pf = pr.get("fields", {})
            for src, dst in FIELD_MAP.items():
                if pf.get(src) is not None:
                    fields[dst] = pf.get(src)
            property_id = pf.get("Property ID")

        new_row = {
            **fields,
            "phone": phone_number,
            "Lead Status": "New",
            "Source": source,
        }
        created = _safe_create(ltbl, new_row)
        if created:
            print(f"✨ Promoted {phone_number} → Lead")
            return created["id"], property_id
        return None, property_id
    except Exception as e:
        print(f"⚠️ Lead promotion failed: {e}")
        return None, None


def update_lead_activity(lead_id: str, body: str, direction: str, intent: Optional[str] = None):
    ltbl = get_leads()
    if not (ltbl and lead_id):
        return
    try:
        patch: Dict[str, Any] = {
            "Last Activity": iso_timestamp(),
            "Last Direction": direction,
            "Last Message": (body or "")[:500],
        }
        if intent == "followup_yes":
            patch["Lead Status"] = "Interested"
        if intent == "optout":
            patch["Lead Status"] = "DNC"
        _safe_update(ltbl, lead_id, patch)
    except Exception as e:
        print(f"⚠️ Failed to update lead activity: {e}")


def set_phone_verified(phone_number: str, verified: bool = True):
    ptbl = get_prospects()
    if not (ptbl and phone_number):
        return
    try:
        pn10 = last10(phone_number)
        if not pn10:
            return
        for r in ptbl.all():
            f = r.get("fields", {})
            for key in (
                "Phone 1",
                "Phone 1 (from Linked Owner)",
                "Phone 2",
                "Phone 2 (from Linked Owner)",
                "Phone",
                "phone",
                "Mobile",
                "Cell",
                "Phone Number",
            ):
                dv = last10(f.get(key))
                if dv and dv == pn10:
                    patch: Dict[str, Any] = {}
                    if key in ("Phone 1", "Phone 1 (from Linked Owner)"):
                        patch["Phone 1 Verified"] = verified
                        if "Phone 1 Ownership Verified" in f:
                            patch["Phone 1 Ownership Verified"] = verified
                    else:
                        patch["Phone 2 Verified"] = verified
                        if "Phone 2 Ownership Verified" in f:
                            patch["Phone 2 Ownership Verified"] = verified
                    _safe_update(ptbl, r["id"], patch)
                    return
    except Exception as e:
        print(f"⚠️ set_phone_verified failed: {e}")


# -----------------
# Core Autoresponder
# -----------------
def run_autoresponder(limit: int = 50, view: str = "Unprocessed Inbounds"):
    convos = get_convos()
    # In tests, force direct-send path (don’t touch Airtable Drip Queue)
    drip = None if _is_test_mode() else get_drip_queue()

    if not convos:
        return {
            "ok": False,
            "processed": 0,
            "breakdown": {},
            "errors": ["Missing Conversations table"],
        }

    processed = 0
    breakdown: Dict[str, int] = {}
    errors: list[Any] = []
    processed_by = settings().__dict__.get("PROCESSED_BY_LABEL") or "Autoresponder"

    try:
        rows = convos.all(view=view, max_records=limit) if view else convos.all(max_records=limit)
        for r in rows:
            f = r.get("fields", {}) or {}

            from_num = f.get(CONV_FIELDS["FROM"]) or f.get("From")
            to_did = f.get(CONV_FIELDS["TO"]) or f.get("To")
            body = f.get(CONV_FIELDS["BODY"]) or f.get("Body")
            msg_id = r.get("id")

            if not from_num or not body:
                continue

            prospect_row = _find_prospect_by_phone(from_num)
            pf = prospect_row.get("fields", {}) if prospect_row else {}
            prospect_id = prospect_row["id"] if prospect_row else None
            pers = _compose_personalization(pf)
            market = f.get("Market") or pf.get("Market")

            campaign_id = None
            camp_link = f.get("Campaign") or []
            if isinstance(camp_link, list) and camp_link:
                campaign_id = camp_link[0]
            elif isinstance(camp_link, str):
                campaign_id = camp_link

            intent = classify_intent(body)
            print(f"🤖 Autoresponder inbound {from_num}: intent={intent} | body={body[:160]}")

            if intent not in ("followup_wrong", "optout"):
                set_phone_verified(from_num, True)

            # STOP/DNC — no reply
            if intent == "optout":
                _safe_update(
                    convos,
                    msg_id,
                    remap_existing_only(
                        convos,
                        {
                            CONV_FIELDS["STATUS"]: "DNC",
                            CONV_FIELDS["PROCESSED_BY"]: processed_by,
                            "processed_at": iso_timestamp(),
                            CONV_FIELDS["INTENT"]: intent,
                            "stage": STAGE_MAP.get(intent, "Opt-Out"),
                        },
                    ),
                )
                lead_id, _ = promote_to_lead(from_num, source=processed_by)
                update_lead_activity(lead_id, body, "IN", intent="optout")
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1
                continue

            # Stage 3 → AI closer (and optional follow-up scheduling)
            if intent in ("price_response", "condition_response"):
                try:
                    ai_result = run_ai_closer(from_num, body, f)  # type: ignore[arg-type]
                    if not _is_test_mode():
                        try:
                            schedule_from_response(
                                phone=from_num,
                                intent=intent,
                                lead_id=None,
                                market=market,
                                property_id=None,
                                current_stage=None,
                            )
                        except Exception as e:
                            errors.append({"phone": from_num, "error": f"followup schedule failed: {e}"})

                    _safe_update(
                        convos,
                        msg_id,
                        {
                            "status": "AI_HANDOFF",
                            "processed_by": "AI Closer",
                            "processed_at": iso_timestamp(),
                            "intent_detected": intent,
                            "stage": STAGE_MAP.get(intent, "Stage 3 - AI Closing"),
                            "ai_result": str(ai_result),
                        },
                    )
                    processed += 1
                    breakdown[intent] = breakdown.get(intent, 0) + 1
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"AI closer failed: {e}"})
                continue

            # Stage 1–2 → choose template + queue reply or send directly
            reply_text, template_id = _choose_template(intent, pers)
            lead_id, property_id = promote_to_lead(from_num, source=processed_by)

            queued_ok = False
            if drip and not _is_test_mode():
                try:
                    payload = {
                        "Prospect": [prospect_id] if prospect_id else None,
                        "Campaign": [campaign_id] if campaign_id else None,
                        "Template": [template_id] if template_id else None,
                        "Market": market,
                        "phone": from_num,
                        "message_preview": reply_text,
                        "from_number": to_did,
                        "From Number": to_did,
                        "status": "QUEUED",
                        "next_send_date": iso_timestamp(),
                        "Property ID": property_id,
                        "UI": STATUS_ICON.get("QUEUED"),
                    }
                    payload = {k: v for k, v in payload.items() if v is not None}
                    created = _safe_create(drip, payload)
                    queued_ok = bool(created and created.get("id"))
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"Queue failed: {e}"})

            # Fallback: direct send (test path always lands here)
            if not queued_ok and MessageProcessor:
                try:
                    # Keep args minimal so tests' fake_send signature matches
                    send_result = MessageProcessor.send(  # type: ignore[attr-defined]
                        phone=from_num,
                        body=reply_text,
                        lead_id=lead_id,
                        property_id=property_id,
                        direction="OUT",
                    )
                    if (send_result or {}).get("status") != "sent":
                        errors.append(
                            {
                                "phone": from_num,
                                "error": (send_result or {}).get("error", "Send failed"),
                            }
                        )
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"Immediate send failed: {e}"})

            _safe_update(
                convos,
                msg_id,
                {
                    "status": "RESPONDED",
                    "processed_by": processed_by,
                    "processed_at": iso_timestamp(),
                    "intent_detected": intent,
                    "stage": STAGE_MAP.get(intent, "Stage 1 - Owner Check"),
                    "template_id": template_id,
                },
            )
            if lead_id:
                update_lead_activity(lead_id, body, "IN", intent=intent)

            if not _is_test_mode():
                try:
                    schedule_from_response(
                        phone=from_num,
                        intent=intent,
                        lead_id=lead_id,
                        market=market,
                        property_id=property_id,
                        current_stage=None,
                    )
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"followup schedule failed: {e}"})

            processed += 1
            breakdown[intent] = breakdown.get(intent, 0) + 1

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        errors.append(str(e))

    return {
        "ok": processed > 0,
        "processed": processed,
        "breakdown": breakdown,
        "errors": errors,
    }
