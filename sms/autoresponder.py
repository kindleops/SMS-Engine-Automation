import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Tuple

from pyairtable import Table
from sms.textgrid_sender import send_message

# --- Airtable Keys & Base IDs ---
ACQ_KEY   = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")
DISPO_KEY = os.getenv("AIRTABLE_DISPO_KEY")        or os.getenv("AIRTABLE_API_KEY")

LEADS_CONVOS_BASE     = os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE")

# Tables
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
UNPROCESSED_VIEW    = os.getenv("UNPROCESSED_VIEW", "Unprocessed Inbounds")
OPTOUTS_TABLE       = os.getenv("OPTOUTS_TABLE", "Opt-Outs")

# Fields
FROM_FIELD   = os.getenv("CONV_FROM_FIELD",    "phone")
TO_FIELD     = os.getenv("CONV_TO_FIELD",      "to_number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "message")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD",  "intent")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD",  "status")

PROCESSED_BY_FIELD = os.getenv("CONV_PROCESSED_BY_FIELD", "Processed By")
PROCESSED_BY_LABEL = os.getenv("PROCESSED_BY_LABEL", "Autoresponder")

# Airtable Tables
convos  = Table(ACQ_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE) if ACQ_KEY and LEADS_CONVOS_BASE else None
optouts = Table(DISPO_KEY, CAMPAIGN_CONTROL_BASE, OPTOUTS_TABLE) if DISPO_KEY and CAMPAIGN_CONTROL_BASE else None

# --- Helpers ---
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

# --- Intent Classifiers ---
OPTOUT = _regex_set([r"\bstop\b", r"\bunsubscribe\b", r"\bquit\b", r"\bend\b", r"\bcancel\b",
                     r"\bdo not (text|message|contact)\b", r"\bremove me\b", r"\bdnc\b"])
WRONG  = _regex_set([r"\bwrong (number|person)\b", r"\bnot (me|mine)\b",
                     r"\bn[úu]mero equivocado\b", r"\bno (soy|es) el du[eé]no\b"])
YES    = _regex_set([r"\byes\b", r"\byep\b", r"\bok\b", r"\bsure\b", r"\binterested\b",
                     r"\bprice\b", r"\boffer\b", r"\bhow much\b", r"\bmake me an offer\b"])
NO     = _regex_set([r"\bno\b", r"\bnot interested\b", r"\bno thanks?\b", r"\bnot selling\b"])
LATER  = _regex_set([r"\blater\b", r"\bnot now\b", r"\bcall me later\b", r"\bcheck back\b"])

FALSE_OPTOUT = _regex_set([r"\bstop by\b", r"\bstop in\b"])
FALSE_NO     = _regex_set([r"\bno problem\b", r"\bno worries\b"])

def classify_reply(body: str) -> Tuple[str, Dict[str, int]]:
    b = _normalize(body)
    scores = {"OPTOUT": 0, "WRONG": 0, "YES": 0, "NO": 0, "LATER": 0, "OTHER": 0}
    if not b:
        scores["OTHER"] = 1
        return "OTHER", scores
    if not FALSE_OPTOUT.search(b) and OPTOUT.search(b):
        return "OPTOUT", {"OPTOUT": 5}
    if WRONG.search(b):
        return "WRONG", {"WRONG": 4}
    if YES.search(b): scores["YES"] += 2
    if not FALSE_NO.search(b) and NO.search(b): scores["NO"] += 2
    if LATER.search(b): scores["LATER"] += 1
    intent = max(scores, key=scores.get)
    if scores[intent] == 0:
        intent = "OTHER"; scores["OTHER"] = 1
    return intent, scores

REPLIES = {
    "WRONG":  "Thanks for letting me know — I’ll remove this number.",
    "OPTOUT": "Got it — you’re opted out and won’t hear from us again.",
    "NO":     "All good — thanks for confirming. If anything changes, text me anytime.",
    "YES":    "Great — are you open to a cash offer if the numbers make sense?",
    "LATER":  "No worries — I’ll check back down the road. If timing changes sooner, just text me.",
    "OTHER":  "Thanks for the response. Are you the owner and open to an offer if the numbers work?",
}

# --- Main Autoresponder ---
def run_autoresponder(limit: int = 50, view: str = UNPROCESSED_VIEW):
    if not convos:
        return {"ok": False, "error": "Airtable Conversations table not configured"}
    records = convos.all(view=view)[:limit]
    processed, breakdown = 0, {k: 0 for k in REPLIES.keys()}

    for r in records:
        try:
            f = r.get("fields", {})
            if f.get(STATUS_FIELD) != "UNPROCESSED":
                continue
            msg   = f.get(MSG_FIELD, "")
            phone = f.get(FROM_FIELD)
            if not phone:
                continue

            intent, _ = classify_reply(msg)
            send_message(phone, REPLIES[intent])

            convos.update(r["id"], {
                STATUS_FIELD: "PROCESSED",
                INTENT_FIELD: intent,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                PROCESSED_BY_FIELD: PROCESSED_BY_LABEL,
            })

            if intent == "OPTOUT" and optouts:
                optouts.create({
                    "Phone": phone,
                    "Source": "Inbound SMS",
                    "Opt-Out Date": datetime.now(timezone.utc).isoformat(),
                })

            processed += 1
            breakdown[intent] += 1
            print(f"🤖 Replied → {phone}: {intent}")

        except Exception as e:
            print(f"❌ Error processing {r.get('id')}: {e}")
            continue

    print(f"📊 Autoresponder finished — processed {processed} | {breakdown}")
    return {"processed": processed, "breakdown": breakdown}