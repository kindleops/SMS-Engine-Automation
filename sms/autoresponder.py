# sms/autoresponder.py  — Final hardened version

from __future__ import annotations

import os
import re
import time
import random
import traceback
from typing import Any, Dict, Optional, Tuple, Iterable, Callable
from datetime import datetime, timezone, timedelta

# --- Project config / tables ---
from sms.config import (
    settings,
    conversations,
    leads as leads_tbl,
    prospects as prospects_tbl,
    templates as templates_tbl,
    drip_queue as drip_tbl,
    CONV_FIELDS,         # mapping hints for Conversations columns
    PHONE_FIELDS,        # possible prospect phone columns
)

# Optional modules
try:
    from sms.message_processor import MessageProcessor  # expects .send(phone, body, ...)
except Exception:
    MessageProcessor = None  # type: ignore

try:
    from sms.followup_flow import schedule_from_response
except Exception:
    def schedule_from_response(**_):  # no-op if not wired
        pass

try:
    from sms import templates as local_templates
except Exception:
    local_templates = None  # type: ignore


# =========================
# Config / constants
# =========================

# Known-safe Conversation status values (don’t create new Single-Select options)
SAFE_CONV_STATUS = ("RESPONDED", "AI_HANDOFF", "DNC")

STATUS_ICON = {
    "QUEUED": "⏳", "READY": "⏳", "SENDING": "🔄",
    "SENT": "✅", "DELIVERED": "✅",
    "FAILED": "❌", "CANCELLED": "❌",
}

# Prospect → Lead field map (copy only keys that exist in the destination)
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
    "negative": "Stage 2 - Negative",
    "delay": "Stage 2 - Follow Up Later",
}

# Words/phrases we’ll look for (all lowercased)
STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
YES_WORDS = {"yes", "yeah", "yep", "sure", "affirmative", "correct", "that is me", "that's me", "of course", "i am"}
NO_WORDS = {"no", "nope", "nah", "not interested", "dont bother", "stop texting"}
WRONG_WORDS = {"wrong number", "not mine", "dont own", "do not own", "no owner", "new number"}
INTEREST_WORDS = {
    "offer", "what can you offer", "how much", "cash", "interested", "curious", "talk", "price", "numbers",
    "what’s your number", "whats your number", "what is your number"
}
PRICE_WORDS = {"price", "asking", "$", " k", "k ", "number you have in mind", "how much", "range", "ballpark"}
COND_WORDS = {"condition", "repairs", "needs work", "renovated", "tenant", "tenants", "vacant", "occupied", "as-is"}
DELAY_WORDS = {"later", "next week", "tomorrow", "busy", "call me later", "text later", "reach out later", "follow up"}
NEG_WORDS = {"scam", "spam", "go away", "lose my number", "stop harassing", "reported", "lawsuit"}
WHO_PHRASES = {"who is this", "who's this", "whos this", "who are you", "who dis", "identify yourself"}
HOW_NUM_PHRASES = {
    "how did you get my number", "how did you get my #", "how you get my number",
    "why do you have my number", "where did you get my number", "how got my number"
}


# =========================
# Utilities
# =========================

def iso_timestamp(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(timezone.utc)).isoformat()

def _digits(s: Any) -> Optional[str]:
    if not isinstance(s, str):
        return None
    d = "".join(re.findall(r"\d+", s))
    return d if len(d) >= 10 else None

def last10(s: Any) -> Optional[str]:
    d = _digits(s)
    return d[-10:] if d else None

def _record_matches_phone(f: Dict[str, Any], l10: str) -> bool:
    for _, v in f.items():
        if isinstance(v, str) and last10(v) == l10:
            return True
    return False

def _first_from_fields(f: Dict[str, Any]) -> Optional[str]:
    for k in ["Owner First Name", "First Name", "First", "Name", "Owner Name"]:
        v = f.get(k)
        if isinstance(v, str) and v.strip():
            return v.split()[0]
    return None

def _compose_personalization(pf: Dict[str, Any]) -> Dict[str, Any]:
    full = pf.get("Owner Name") or f"{pf.get('Owner First Name', '') or ''} {pf.get('Owner Last Name', '') or ''}".strip()
    first = (full or "").split(" ")[0] if full else "there"
    address = pf.get("Property Address") or pf.get("Address") or "your property"
    return {"First": first, "Address": address}

def _get_setting(name: str, default=None):
    try:
        return getattr(settings(), name, default)
    except Exception:
        return default


def _followup_flow_enabled() -> bool:
    """Return True if we should attempt to call the follow-up scheduler."""

    if not callable(schedule_from_response):
        return False

    # Allow explicit disablement from env. Accept common truthy/falsey variants.
    flag = os.getenv("AR_DISABLE_FOLLOWUP_FLOW", "").strip().lower()
    if flag in {"1", "true", "yes", "on"}:
        return False

    # Avoid slow HTTP attempts while running unit tests or in TEST_MODE.
    if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("TEST_MODE"):
        return False

    return True

def _retry(fn: Callable[[], Any], retries: int = 3, delay: float = 0.6) -> Any:
    """
    Tiny retry wrapper for flaky network calls to Airtable.
    Only catches generic Exceptions; if the function raises, we retry a few times.
    """
    last_exc = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if i < retries - 1:
                time.sleep(delay * (2 ** i))
    if last_exc:
        raise last_exc

def _safe_create(table, payload: Dict) -> Optional[Dict]:
    if not table or not payload:
        return None
    try:
        return _retry(lambda: table.create(payload))
    except Exception:
        traceback.print_exc()
        return None

def _safe_update(table, rec_id: str, payload: Dict) -> Optional[Dict]:
    if not (table and rec_id and payload):
        return None
    try:
        return _retry(lambda: table.update(rec_id, payload))
    except Exception:
        traceback.print_exc()
        return None


# =========================
# Quiet Hours (optional)
# =========================

def _is_quiet_hours(now_utc: datetime) -> tuple[bool, Optional[datetime]]:
    """
    Reads any of these from settings():
      QUIET_ENABLED: bool
      QUIET_START_HOUR: int (0..23)  local hour
      QUIET_END_HOUR:   int (0..23)  local hour
      QUIET_TZ_OFFSET:  int (minutes offset from UTC), e.g. -300 for US Central
    If disabled or incomplete, returns (False, None).
    """
    enabled = bool(_get_setting("QUIET_ENABLED", False))
    if not enabled:
        return False, None

    start_h = _get_setting("QUIET_START_HOUR", None)
    end_h   = _get_setting("QUIET_END_HOUR", None)
    tz_off  = int(_get_setting("QUIET_TZ_OFFSET", 0))

    if start_h is None or end_h is None:
        return False, None

    # Convert UTC now → local
    local_now = now_utc + timedelta(minutes=tz_off)
    start = local_now.replace(hour=int(start_h), minute=0, second=0, microsecond=0)
    end   = local_now.replace(hour=int(end_h), minute=0, second=0, microsecond=0)

    def _to_utc(dt_local: datetime) -> datetime:
        return dt_local - timedelta(minutes=tz_off)

    # Quiet window can cross midnight
    if start <= end:
        in_quiet = start <= local_now < end
        next_allowed = end if in_quiet else local_now
    else:
        # e.g. 21:00 → 08:00
        in_quiet = not (end <= local_now < start)
        next_allowed = (end if local_now < end else end + timedelta(days=1)) if in_quiet else local_now

    next_allowed_utc = _to_utc(next_allowed)
    return in_quiet, next_allowed_utc


# =========================
# Data lookups
# =========================

def _find_prospect_by_phone(phone: str) -> Optional[Dict]:
    p = prospects_tbl()
    if not (p and phone):
        return None
    try:
        l10 = last10(phone)
        if not l10:
            return None
        for r in p.all():
            if _record_matches_phone(r.get("fields", {}) or {}, l10):
                return r
    except Exception:
        traceback.print_exc()
    return None

def set_phone_verified(phone_number: str, verified: bool = True):
    ptbl = prospects_tbl()
    if not (ptbl and phone_number):
        return
    try:
        pn10 = last10(phone_number)
        if not pn10:
            return
        for r in ptbl.all():
            f = r.get("fields", {}) or {}
            for key in (
                "Phone 1","Phone 1 (from Linked Owner)","Phone 2","Phone 2 (from Linked Owner)",
                "Phone","phone","Mobile","Cell","Phone Number"
            ):
                dv = last10(f.get(key))
                if dv and dv == pn10:
                    patch: Dict[str, Any] = {}
                    if key in ("Phone 1", "Phone 1 (from Linked Owner)"):
                        if "Phone 1 Verified" in f: patch["Phone 1 Verified"] = verified
                        if "Phone 1 Ownership Verified" in f: patch["Phone 1 Ownership Verified"] = verified
                    else:
                        if "Phone 2 Verified" in f: patch["Phone 2 Verified"] = verified
                        if "Phone 2 Ownership Verified" in f: patch["Phone 2 Ownership Verified"] = verified
                    if patch:
                        _safe_update(ptbl, r["id"], patch)
                    return
    except Exception:
        traceback.print_exc()


# =========================
# Intent detection
# =========================

def _has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(p in text for p in phrases)

def classify_intent(body: str) -> str:
    text = (body or "").lower().strip()

    if _has_any(text, STOP_WORDS):                 return "optout"
    if _has_any(text, WHO_PHRASES):                return "who_is_this"
    if _has_any(text, HOW_NUM_PHRASES):            return "how_get_number"
    if _has_any(text, WRONG_WORDS):                return "followup_wrong"
    if _has_any(text, NEG_WORDS):                  return "negative"
    if _has_any(text, DELAY_WORDS):                return "delay"

    # Word-boundary tolerant yes/no
    if re.search(r"\b(" + "|".join(map(re.escape, YES_WORDS)) + r")\b", text):
        return "followup_yes"
    if re.search(r"\b(" + "|".join(map(re.escape, NO_WORDS)) + r")\b", text):
        return "followup_no"

    if _has_any(text, PRICE_WORDS):                return "price_response"
    if _has_any(text, COND_WORDS):                 return "condition_response"
    if _has_any(text, INTEREST_WORDS):             return "interest"

    if any(w in text for w in ["maybe", "not sure", "thinking", "depends", "idk", "i don’t know", "i don't know"]):
        return "neutral"

    return "intro"


# =========================
# Templates
# =========================

def _choose_template(intent: str, fields: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    t = templates_tbl()
    msg, tid = None, None

    if t:
        try:
            rows = t.all()
            pool = []
            for r in rows:
                tf = r.get("fields", {}) or {}
                internal = (tf.get("Internal ID") or tf.get("intent") or "").strip().lower()
                if internal == intent:
                    pool.append(r)
            if pool:
                chosen = random.choice(pool)
                tid = chosen["id"]
                raw = (chosen["fields"] or {}).get("Message") or ""
                first = fields.get("First") or _first_from_fields(fields) or "there"
                addr = fields.get("Address") or fields.get("Property Address") or "your property"
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


# =========================
# Lead helpers
# =========================

def promote_to_lead(phone_number: str, source: str = "Autoresponder") -> Tuple[Optional[str], Optional[str]]:
    ltbl = leads_tbl()
    if not (phone_number and ltbl):
        return (None, None)
    try:
        l10 = last10(phone_number)
        for r in ltbl.all():
            lf = r.get("fields", {}) or {}
            if _record_matches_phone(lf, l10):
                return r["id"], lf.get("Property ID")

        fields: Dict[str, Any] = {}
        property_id = None
        pr = _find_prospect_by_phone(phone_number)
        if pr:
            pf = pr.get("fields", {}) or {}
            for src, dst in FIELD_MAP.items():
                if pf.get(src) is not None:
                    fields[dst] = pf.get(src)
            property_id = pf.get("Property ID")

        new_row = {**fields, "phone": phone_number, "Lead Status": "New", "Source": source}
        created = _safe_create(ltbl, new_row)
        if created:
            print(f"✨ Promoted {phone_number} → Lead")
            return created["id"], property_id
        return None, property_id
    except Exception as e:
        print(f"⚠️ Lead promotion failed: {e}")
        return None, None

def update_lead_activity(lead_id: Optional[str], body: str, direction: str, intent: Optional[str] = None):
    if not lead_id:
        return
    ltbl = leads_tbl()
    if not ltbl:
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


# =========================
# Fetch + Core
# =========================

def _fetch_unprocessed(convos, limit: int):
    """
    Prefer a curated view; fallback to scanning recent rows.
    A row is considered unprocessed if processed_by/PROCESSED_BY field is empty.
    """
    preferred_view = _get_setting("CONV_VIEW_INBOUND", "Unprocessed Inbounds")
    try:
        rows = convos.all(view=preferred_view, max_records=limit)
        if rows:
            return rows
    except Exception:
        pass

    rows = convos.all(max_records=limit * 2)
    out = []
    for r in rows:
        f = r.get("fields", {}) or {}
        direction = f.get(CONV_FIELDS.get("DIRECTION", "Direction")) or f.get("Direction")
        processed_by = f.get(CONV_FIELDS.get("PROCESSED_BY", "processed_by")) or f.get("processed_by")
        if (direction in ("IN", "Inbound")) and not processed_by:
            out.append(r)
        if len(out) >= limit:
            break
    return out


def _pick_safe_status(preferred: str) -> str:
    if preferred in SAFE_CONV_STATUS:
        return preferred
    # fallbacks
    return SAFE_CONV_STATUS[0]


def run_autoresponder(limit: int = 50):
    convos = conversations()
    drip = drip_tbl() if not os.getenv("TEST_MODE") else None

    if not convos:
        return {"ok": False, "processed": 0, "breakdown": {}, "errors": ["Missing Conversations table"]}

    processed = 0
    breakdown: Dict[str, int] = {}
    errors: list[Any] = []
    processed_by = (_get_setting("PROCESSED_BY_LABEL") or "Autoresponder").strip() or "Autoresponder"
    processor_tag = processed_by  # for queue attribution

    try:
        rows = _fetch_unprocessed(convos, limit)
        now = datetime.now(timezone.utc)
        next_allowed_utc = None
        try:
            is_quiet, next_allowed_utc = _is_quiet_hours(now)
        except Exception:
            is_quiet, next_allowed_utc = False, None

        for r in rows:
            f = r.get("fields", {}) or {}

            from_num = f.get(CONV_FIELDS.get("FROM", "From")) or f.get("From")
            to_did   = f.get(CONV_FIELDS.get("TO", "To")) or f.get("To")
            body     = f.get(CONV_FIELDS.get("BODY", "Body")) or f.get("Body")
            msg_id   = r.get("id")

            if not from_num or not body or not msg_id:
                continue

            # Idempotency: skip if already processed
            if f.get(CONV_FIELDS.get("PROCESSED_BY", "processed_by")) or f.get("processed_by"):
                continue

            prospect_row = _find_prospect_by_phone(from_num)
            pf = prospect_row.get("fields", {}) if prospect_row else {}
            pers = _compose_personalization(pf)
            market = f.get("Market") or pf.get("Market")

            campaign_id = None
            camp_link = f.get("Campaign") or []
            if isinstance(camp_link, list) and camp_link:
                campaign_id = camp_link[0]
            elif isinstance(camp_link, str):
                campaign_id = camp_link

            intent = classify_intent(body)
            stage = STAGE_MAP.get(intent, "Stage 1 - Owner Check")

            print(f"[{iso_timestamp()}] IN {from_num} | intent={intent} | body='{(body or '')[:160]}'")

            # Verify phones except explicit wrong number or optout
            if intent not in ("followup_wrong", "optout"):
                set_phone_verified(from_num, True)

            # STOP/DNC — no reply
            if intent == "optout":
                patch = {
                    (CONV_FIELDS.get("STATUS", "status")): _pick_safe_status("DNC"),
                    (CONV_FIELDS.get("PROCESSED_BY", "processed_by")): processed_by,
                    "processed_at": iso_timestamp(),
                    (CONV_FIELDS.get("INTENT", "intent_detected")): intent,
                    "stage": stage,
                }
                _safe_update(convos, msg_id, patch)
                lead_id, _ = promote_to_lead(from_num, source=processed_by)
                update_lead_activity(lead_id, body, "IN", intent="optout")
                processed += 1
                breakdown[intent] = breakdown.get(intent, 0) + 1
                continue

            followup_allowed = _followup_flow_enabled()

            # Stage 3 → optional AI handoff
            if intent in ("price_response", "condition_response"):
                try:
                    # Optional: AI closer if available
                    try:
                        from sms.ai_closer import run_ai_closer as _ai
                        ai_result = _ai(from_num, body, f)
                    except Exception:
                        ai_result = {"ok": False, "note": "ai_closer unavailable"}

                    # Optional schedule
                    if followup_allowed:
                        try:
                            schedule_from_response(
                                phone=from_num, intent=intent, lead_id=None,
                                market=market, property_id=None, current_stage=None
                            )
                        except Exception as e:
                            errors.append({"phone": from_num, "error": f"followup schedule failed: {e}"})

                    _safe_update(convos, msg_id, {
                        (CONV_FIELDS.get("STATUS", "status")): _pick_safe_status("AI_HANDOFF"),
                        (CONV_FIELDS.get("PROCESSED_BY", "processed_by")): "AI Closer",
                        "processed_at": iso_timestamp(),
                        (CONV_FIELDS.get("INTENT", "intent_detected")): intent,
                        "stage": stage,
                        "ai_result": str(ai_result),
                    })
                    processed += 1
                    breakdown[intent] = breakdown.get(intent, 0) + 1
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"AI closer failed: {e}"})
                continue

            # Stage 1–2 → pick template, queue or send
            reply_text, template_id = _choose_template(intent, pers)
            lead_id, property_id = promote_to_lead(from_num, source=processed_by)

            # Quiet-hours aware queue date
            queue_send_time = next_allowed_utc or datetime.now(timezone.utc)

            queued_ok = False
            if drip:
                try:
                    payload = {
                        # Relations (only include if we have them)
                        "Prospect": [prospect_row["id"]] if prospect_row else None,
                        "Campaign": [campaign_id] if campaign_id else None,
                        "Template": [template_id] if template_id else None,

                        # Routing + preview
                        "Market": market,
                        "phone": from_num,
                        "message_preview": reply_text,
                        "from_number": to_did,
                        "From Number": to_did,

                        # Status + meta
                        "status": "QUEUED",
                        "UI": STATUS_ICON.get("QUEUED"),
                        "next_send_date": iso_timestamp(queue_send_time),

                        # Additional helpful context
                        "Property ID": property_id,
                        "processor": processor_tag,   # <— who queued this
                        "source": "Autoresponder",    # <— how it got here
                    }
                    payload = {k: v for k, v in payload.items() if v is not None}
                    created = _safe_create(drip, payload)
                    queued_ok = bool(created and created.get("id"))
                    if queued_ok:
                        print("  ⏳ queued reply → Drip Queue")
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"Queue failed: {e}"})

            if not queued_ok and MessageProcessor:
                try:
                    send_result = MessageProcessor.send(
                        phone=from_num, body=reply_text, lead_id=lead_id,
                        property_id=property_id, direction="OUT",
                    )
                    if (send_result or {}).get("status") != "sent":
                        errors.append({"phone": from_num, "error": (send_result or {}).get("error", "Send failed")})
                    else:
                        print("  ✅ sent reply immediately")
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"Immediate send failed: {e}"})

            # Conversation bookkeeping
            conv_patch = {
                (CONV_FIELDS.get("STATUS", "status")): _pick_safe_status("RESPONDED"),
                (CONV_FIELDS.get("PROCESSED_BY", "processed_by")): processed_by,
                "processed_at": iso_timestamp(),
                (CONV_FIELDS.get("INTENT", "intent_detected")): intent,
                "stage": stage,
                "template_id": template_id,
            }
            _safe_update(convos, msg_id, conv_patch)

            if lead_id:
                update_lead_activity(lead_id, body, "IN", intent=intent)

            if followup_allowed:
                try:
                    schedule_from_response(
                        phone=from_num, intent=intent, lead_id=lead_id,
                        market=market, property_id=property_id, current_stage=None,
                    )
                except Exception as e:
                    errors.append({"phone": from_num, "error": f"followup schedule failed: {e}"})

            processed += 1
            breakdown[intent] = breakdown.get(intent, 0) + 1

    except Exception as e:
        print("❌ Autoresponder error:")
        traceback.print_exc()
        errors.append(str(e))

    return {"ok": processed > 0, "processed": processed, "breakdown": breakdown, "errors": errors}


# =========================
# CLI entry
# =========================
if __name__ == "__main__":
    from pprint import pprint
    limit = int(os.getenv("AR_LIMIT", "100"))
    result = run_autoresponder(limit=limit)
    print("\n=== Autoresponder Summary ===")
    pprint(result)
