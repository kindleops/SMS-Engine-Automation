# sms/autoresponder.py
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Tuple

from pyairtable import Table
from sms.textgrid_sender import send_message

# ── Airtable env/config ─────────────────────────────────────────────────────────
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADS_CONVOS_BASE = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
UNPROCESSED_VIEW = os.getenv("UNPROCESSED_VIEW", "Unprocessed Inbounds")

# Field names (match Airtable exactly; override via env if needed)
FROM_FIELD   = os.getenv("CONV_FROM_FIELD",   "From Number")
TO_FIELD     = os.getenv("CONV_TO_FIELD",     "To Number")
MSG_FIELD    = os.getenv("CONV_MESSAGE_FIELD","Message")
INTENT_FIELD = os.getenv("CONV_INTENT_FIELD", "Intent")
STATUS_FIELD = os.getenv("CONV_STATUS_FIELD", "Status")

# Optional Opt-Out sink (Campaign Control base)
CAMPAIGN_CONTROL_BASE = os.getenv("CAMPAIGN_CONTROL_BASE") or os.getenv("AIRTABLE_CAMPAIGN_CONTROL_BASE_ID")
OPTOUTS_TABLE = os.getenv("OPTOUTS_TABLE", "Opt-Outs")

# Tables
convos = Table(AIRTABLE_API_KEY, LEADS_CONVOS_BASE, CONVERSATIONS_TABLE)
optouts = Table(AIRTABLE_API_KEY, CAMPAIGN_CONTROL_BASE, OPTOUTS_TABLE) if (AIRTABLE_API_KEY and CAMPAIGN_CONTROL_BASE) else None

# ── Helpers ─────────────────────────────────────────────────────────────────────
def _normalize(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.casefold()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _word_re(words) -> re.Pattern:
    # \b doesn't work well with phone numbers/emojis; use custom boundaries
    terms = [re.escape(w) for w in words]
    return re.compile(rf"(?<![A-Za-z0-9])(?:{'|'.join(terms)})(?![A-Za-z0-9])")

def _regex_set(patterns) -> re.Pattern:
    return re.compile("|".join(patterns), re.IGNORECASE)

# ── Advanced intent classifier ──────────────────────────────────────────────────
# Order matters for “exclusive” matches. We’ll score + short-circuit on OptOut/Wrong #.
OPTOUT = _regex_set([
    r"\bstop\b", r"\bunsubscribe\b", r"\bquit\b", r"\bend\b",
    r"\bcancel\b", r"\bdo not (text|message|contact)\b", r"\bremove me\b",
    r"\bdnc\b", r"\bdon'?t text\b", r"\bdont text\b",
])
WRONG = _regex_set([
    r"\bwrong (number|person)\b", r"\bnot (me|mine)\b",
    r"\bi (rent|am a tenant)\b", r"\btenant\b", r"\blandlord\b",
    r"\bwho (is|are) this\b", r"\bwho (is|are) (this|you)\b",
    r"\b(this )?is(n'?t| not) (mine|my number)\b",
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

# Disambiguators to avoid false positives (“stop by”, “no problem”)
FALSE_OPTOUT = _regex_set([
    r"\bstop by\b", r"\bstop in\b", r"\bmake it stop raining\b"
])
FALSE_NO = _regex_set([
    r"\bno problem\b", r"\bno worries\b"
])

def classify_reply(body: str) -> Tuple[str, Dict[str, int]]:
    """
    Return (intent, scores) with simple scoring.
    Intents: OPTOUT, WRONG, YES, NO, LATER, OTHER
    """
    b = _normalize(body)

    scores = {"OPTOUT": 0, "WRONG": 0, "YES": 0, "NO": 0, "LATER": 0, "OTHER": 0}

    if not b:
        scores["OTHER"] = 1
        return "OTHER", scores

    # Hard-block false positives
    if FALSE_OPTOUT.search(b):
        pass
    elif OPTOUT.search(b):
        scores["OPTOUT"] += 5
        return "OPTOUT", scores  # short-circuit: compliance first

    if WRONG.search(b):
        scores["WRONG"] += 4
        return "WRONG", scores

    # Soft scoring for the rest
    if YES.search(b):
        scores["YES"] += 2

    if not FALSE_NO.search(b) and NO.search(b):
        scores["NO"] += 2

    if LATER.search(b):
        scores["LATER"] += 1

    # Choose highest
    intent = max(scores, key=lambda k: scores[k])
    if scores[intent] == 0:
        intent = "OTHER"
        scores["OTHER"] = 1

    return intent, scores

# ── Reply copy by intent ────────────────────────────────────────────────────────
REPLIES = {
    "WRONG": "Thanks for letting me know — I’ll remove this number.",
    "OPTOUT": "Got it — you’re opted out and won’t hear from us again.",
    "NO": "All good — thanks for confirming. If anything changes, text me anytime.",
    "YES": "Great — are you open to a cash offer if the numbers make sense?",
    "LATER": "No worries — I’ll check back down the road. If timing changes sooner, just text me.",
    "OTHER": "Thanks for the response. Are you the owner and open to an offer if the numbers work?",
}

# ── Runner ─────────────────────────────────────────────────────────────────────
def run_autoresponder(limit: int = 50, view: str = UNPROCESSED_VIEW):
    """
    Pull records from Conversations view, classify, reply, and mark processed.
    Returns: {"processed": n, "breakdown": {...}}
    """
    # Fetch
    records = convos.all(view=view)[:limit]
    processed = 0
    breakdown = {"OPTOUT": 0, "WRONG": 0, "YES": 0, "NO": 0, "LATER": 0, "OTHER": 0}

    for r in records:
        try:
            fields = r.get("fields", {})
            msg   = fields.get(MSG_FIELD, "")
            phone = fields.get(FROM_FIELD)

            if not phone or not isinstance(phone, str):
                # Nothing to reply to — mark as processed OTHER so it leaves the view
                convos.update(r["id"], {
                    STATUS_FIELD: "Processed",
                    INTENT_FIELD: "OTHER",
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                })
                breakdown["OTHER"] += 1
                continue

            intent, _scores = classify_reply(msg)
            reply = REPLIES[intent]

            # Send SMS
            send_message(phone, reply)

            # Mark processed in Conversations
            convos.update(r["id"], {
                STATUS_FIELD: "Processed",
                INTENT_FIELD: intent,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            })

            # If OPTOUT, record in Campaign Control > Opt-Outs (if available)
            if intent == "OPTOUT" and optouts is not None:
                try:
                    optouts.create({
                        "Phone": phone,
                        "Source": "Inbound SMS",
                        "Opt-Out Date": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as _:
                    # don't fail the whole run on opt-out write issues
                    pass

            processed += 1
            breakdown[intent] += 1
            print(f"🤖 Reply → {phone}: {intent} | {reply}")

        except Exception as e:
            print(f"❌ Error processing {r.get('id')}: {e}")
            # Do not mark processed; it will retry next run.
            continue

    print(f"📊 Autoresponder finished — processed {processed} | {breakdown}")
    return {"processed": processed, "breakdown": breakdown}