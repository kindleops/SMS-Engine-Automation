# sms/autoresponder.py
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Tuple
from pyairtable import Table
from sms.textgrid_sender import send_message
import traceback

# ── Env Vars ───────────────────────────────────────
ACQ_KEY   = os.getenv("AIRTABLE_ACQUISITIONS_KEY") or os.getenv("AIRTABLE_API_KEY")
DISPO_KEY = os.getenv("AIRTABLE_DISPO_KEY")        or os.getenv("AIRTABLE_API_KEY")

LEADS_CONVOS_BASE     = os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID") or os.getenv("LEADS_CONVOS_BASE")
CAMPAIGN_CONTROL_BASE = os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID") or os.getenv("CAMPAIGN_CONTROL_BASE")

CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
UNPROCESSED_VIEW    = os.getenv("UNPROCESSED_VIEW", "Unprocessed Inbounds")
OPTOUTS_TABLE       = os.getenv("OPTOUTS_TABLE", "Opt-Outs")

FROM_FIELD   = os.getenv("CONV_FROM_FIELD",    "From Number")
TO_FIELD     = os.getenv("CONV_TO_FIELD",      "To Number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD", "Message")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD",  "Intent")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD",  "Status")

PROCESSED_BY_FIELD = os.getenv("CONV_PROCESSED_BY_FIELD", "Processed By")
PROCESSED_BY_LABEL = os.getenv("PROCESSED_BY_LABEL", "Autoresponder")

# ── Helpers ───────────────────────────────────────
def _normalize(text: str) -> str:
    if not text: return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.casefold()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _regex_set(patterns): return re.compile("|".join(patterns), re.IGNORECASE)

# Intents
OPTOUT = _regex_set([r"\bstop\b", r"\bunsubscribe\b", r"\bquit\b", r"\bcancel\b", r"\bremove me\b", r"\bdnc\b"])
WRONG  = _regex_set([r"\bwrong (number|person)\b", r"\bnot (me|mine)\b", r"\bn[úu]mero equivocado\b"])
YES    = _regex_set([r"\byes\b", r"\byep\b", r"\binterested\b", r"\boffer\b", r"\bprice\b"])
NO     = _regex_set([r"\bno\b", r"\bnot interested\b", r"\bno thanks?\b", r"\bnot selling\b"])
LATER  = _regex_set([r"\bnot now\b", r"\blater\b", r"\bmaybe in (the )?future\b", r"\bcheck back\b"])

FALSE_OPTOUT = _regex_set([r"\bstop by\b", r"\bstop in\b"])
FALSE_NO     = _regex_set([r"\bno problem\b", r"\bno worries\b"])

REPLIES = {
    "WRONG":  "Thanks for letting me know — I’ll remove this number.",
    "OPTOUT": "Got it — you’re opted out and won’t hear from us again.",
    "NO":     "All good — thanks for confirming. If anything changes, text me anytime.",
    "YES":    "Great — are you open to a cash offer if the numbers make sense?",
    "LATER":  "No worries — I’ll check back down the road.",
    "OTHER":  "Thanks for the response. Are you the owner and open to an offer?",
}

def classify_reply(body: str) -> Tuple[str, Dict[str, int]]:
    b = _normalize(body)
    scores = {"OPTOUT":0, "WRONG":0, "YES":0, "NO":0, "LATER":0, "OTHER":0}
    if not b: return "OTHER", {"OTHER":1}

    if not FALSE_OPTOUT.search(b) and OPTOUT.search(b): return "OPTOUT", {"OPTOUT":5}
    if WRONG.search(b): return "WRONG", {"WRONG":4}
    if YES.search(b): scores["YES"]+=2
    if not FALSE_NO.search(b) and NO.search(b): scores["NO"]+=2
    if LATER.search(b): scores["LATER"]+=1

    intent = max(scores, key=scores.get)
    if scores[intent] == 0: intent="OTHER"; scores["OTHER"]=1
    return intent, scores

# ── Lazy init for Airtable tables ──────────────────
def _get_tables():
    if not (ACQ_KEY and LEADS_CONVOS_BASE):
        print("❌ Missing Airtable env vars for Conversations")
        return None, None
    try:
        convos = Table(ACQ_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
        optouts = Table(DISPO_KEY, CAMPAIGN_CONTROL_BASE, OPTOUTS_TABLE) if CAMPAIGN_CONTROL_BASE else None
        return convos, optouts
    except Exception:
        print("❌ Failed to init Airtable tables in autoresponder")
        traceback.print_exc()
        return None, None

# ── Autoresponder ──────────────────────────────────
def run_autoresponder(limit: int = 50, view: str = UNPROCESSED_VIEW):
    convos, optouts = _get_tables()
    if not convos: return {"ok": False, "error": "Missing Airtable config"}

    records = convos.all(view=view)[:limit]
    processed, breakdown = 0, {k:0 for k in ["OPTOUT","WRONG","YES","NO","LATER","OTHER"]}

    for r in records:
        try:
            f = r.get("fields", {})
            if f.get(STATUS_FIELD) != "UNPROCESSED": continue

            msg, phone = f.get(MSG_FIELD,""), f.get(FROM_FIELD)
            intent,_ = classify_reply(msg)
            reply = REPLIES[intent]
            send_message(phone, reply)

            convos.update(r["id"], {
                STATUS_FIELD: "PROCESSED",
                INTENT_FIELD: intent,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                PROCESSED_BY_FIELD: PROCESSED_BY_LABEL,
            })

            if intent=="OPTOUT" and optouts:
                optouts.create({
                    "Phone": phone,
                    "Source": "Inbound SMS",
                    "Opt-Out Date": datetime.now(timezone.utc).isoformat(),
                })

            processed+=1; breakdown[intent]+=1
            print(f"🤖 Replied → {phone}: {intent} | {reply}")

        except Exception as e:
            print(f"❌ Error processing record {r.get('id')}: {e}")
            traceback.print_exc()

    print(f"📊 Autoresponder finished — processed {processed} | {breakdown}")
    return {"processed": processed, "breakdown": breakdown}