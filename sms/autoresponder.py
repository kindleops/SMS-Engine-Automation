# sms/autoresponder.py
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Tuple

from pyairtable import Table
from sms.textgrid_sender import send_message

# ── Airtable keys & base IDs ────────────────────────────────────────────────
ACQ_KEY   = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")   # Leads & Conversations
DISPO_KEY = os.getenv("AIRTABLE_DISPO_KEY")        or os.getenv("AIRTABLE_API_KEY")   # Campaign Control

LEADS_CONVOS_BASE     = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID") or os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID") or os.getenv("CAMPAIGN_CONTROL_BASE")

# Table/view names (override via env if needed)
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
UNPROCESSED_VIEW    = os.getenv("UNPROCESSED_VIEW", "Unprocessed Inbounds")
OPTOUTS_TABLE       = os.getenv("OPTOUTS_TABLE", "Opt-Outs")

# Field names (override via env to match your schema)
FROM_FIELD   = os.getenv("CONV_FROM_FIELD",    "From Number")
TO_FIELD     = os.getenv("CONV_TO_FIELD",      "To Number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "Message")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD",  "Intent")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD",  "Status")

# ── Tables ──────────────────────────────────────────────────────────────────
convos  = Table(ACQ_KEY,   LEADS_CONVOS_BASE,     CONVERSATIONS_TABLE)
optouts = Table(DISPO_KEY, CAMPAIGN_CONTROL_BASE, OPTOUTS_TABLE) if CAMPAIGN_CONTROL_BASE else None

# ── Helpers ─────────────────────────────────────────────────────────────────
def _normalize(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.casefold()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _regex_set(patterns) -> re.Pattern:
    return re.compile("|".join(patterns), re.IGNORECASE)

# ── Advanced intent classifier ─────────────────────────────────────────────
OPTOUT = _regex_set([
    r"\bstop\b", r"\bunsubscribe\b", r"\bquit\b", r"\bend\b", r"\bcancel\b",
    r"\bdo not (text|message|contact)\b", r"\bremove me\b", r"\bdnc\b",
    r"\bdon'?t text\b", r"\bdont text\b",
])
WRONG = _regex_set([
    r"\bwrong (number|person)\b", r"\bnot (me|mine)\b",
    r"\bi (rent|am a tenant)\b", r"\btenant\b", r"\blandlord\b",
    r"\bwho (is|are) (this|you)\b", r"\b(this )?is(n'?t| not) (mine|my number)\b",
    r"\bno (soy|es) el du[eé]no\b", r"\bn[úu]mero equivocado\b",
])
YES = _regex_set([
    r"\byes\b", r"\byep\b", r"\byea?h\b", r"\bok\b", r"\bsure\b", r"\binterested\b",
    r"\bmaybe\b", r"\bdepends\b", r"\boffer\b", r"\bprice\b", r"\bhow much\b",
    r"\bmake me an offer\b", r"\bwhat (would|will) you pay\b",
    r"\bsend me an offer\b", r"\bnumbers?\b", r"\bgive me a price\b",
    r"\bconsidering\b", r"\bpossibly\b", r"\bpotentially\b",
])
NO = _regex_set([
    r"\bno\b", r"\bnot interested\b", r"\bno thanks?\b", r"\bgo away\b",
    r"\bleave me alone\b", r"\bdo not (text|message|contact)\b",
    r"\bnot selling\b", r"\bnot for sale\b",
])
LATER = _regex_set([
    r"\bnot now\b", r"\blater\b", r"\bcall me later\b", r"\bmaybe in (the )?future\b",
    r"\bcheck back\b", r"\bnot right now\b", r"\bthinking\b", r"\bnot selling yet\b",
    r"\bnext (week|month|year)\b", r"\breach out (later|another time)\b",
])
FALSE_OPTOUT = _regex_set([r"\bstop by\b", r"\bstop in\b", r"\bmake it stop raining\b"])
FALSE_NO     = _regex_set([r"\bno problem\b", r"\bno worries\b"])

def classify_reply(body: str) -> Tuple[str, Dict[str, int]]:
    b = _normalize(body)
    scores = {"OPTOUT": 0, "WRONG": 0, "YES": 0, "NO": 0, "LATER": 0, "OTHER": 0}
    if not b:
        scores["OTHER"] = 1
        return "OTHER", scores

    if not FALSE_OPTOUT.search(b) and OPTOUT.search(b):
        scores["OPTOUT"] += 5
        return "OPTOUT", scores  # compliance first

    if WRONG.search(b):
        scores["WRONG"] += 4
        return "WRONG", scores

    if YES.search(b): scores["YES"] += 2
    if not FALSE_NO.search(b) and NO.search(b): scores["NO"] += 2
    if LATER.search(b): scores["LATER"] += 1

    intent = max(scores, key=lambda k: scores[k])
    if scores[intent] == 0:
        intent = "OTHER"
        scores["OTHER"] = 1
    return intent, scores

REPLIES = {
    "WRONG": "Thanks for letting me know — I’ll remove this number.",
    "OPTOUT": "Got it — you’re opted out and won’t hear from us again.",
    "NO": "All good — thanks for confirming. If anything changes, text me anytime.",
    "YES": "Great — are you open to a cash offer if the numbers make sense?",
    "LATER": "No worries — I’ll check back down the road. If timing changes sooner, just text me.",
    "OTHER": "Thanks for the response. Are you the owner and open to an offer if the numbers work?",
}

# ── Autoresponder ───────────────────────────────────────────────────────────
def run_autoresponder(limit: int = 50, view: str = UNPROCESSED_VIEW):
    records = convos.all(view=view)[:limit]
    processed = 0
    breakdown = {"OPTOUT": 0, "WRONG": 0, "YES": 0, "NO": 0, "LATER": 0, "OTHER": 0}

    for r in records:
        try:
            f = r.get("fields", {})
            print("DEBUG fields received:", f)
            msg   = f.get(MSG_FIELD, "") or f.get("message", "")
            phone = f.get(FROM_FIELD)    or f.get("phone")

            if not phone or not isinstance(phone, str):
                convos.update(r["id"], {
                    STATUS_FIELD: "PROCESSED",
                    INTENT_FIELD: "OTHER",
                    "processed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
                breakdown["OTHER"] += 1
                continue

            intent, _ = classify_reply(msg)
            reply = REPLIES[intent]
            send_message(phone, reply)

            convos.update(r["id"], {
                STATUS_FIELD: "PROCESSED",
                INTENT_FIELD: intent,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            })

            if intent == "OPTOUT" and optouts is not None:
                try:
                    optouts.create({
                        "Phone": phone,
                        "Source": "Inbound SMS",
                        "Opt-Out Date": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    pass

            processed += 1
            breakdown[intent] += 1
            print(f"🤖 Reply → {phone}: {intent} | {reply}")
        except Exception as e:
            print(f"❌ Error processing {r.get('id')}: {e}")
            continue

    print(f"📊 Autoresponder finished — processed {processed} | {breakdown}")
    return {"processed": processed, "breakdown": breakdown}