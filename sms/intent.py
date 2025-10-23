# sms/intent.py
from __future__ import annotations
import re
from typing import Iterable

# --- Lexicon -----------------------------------------------------
STOP_WORDS = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
YES_WORDS = {"yes", "yeah", "yep", "sure", "affirmative", "correct", "that is me", "that's me", "of course", "i am"}
NO_WORDS = {"no", "nope", "nah", "not interested", "dont bother", "stop texting"}
WRONG_WORDS = {"wrong number", "not mine", "dont own", "do not own", "no owner", "new number"}
INTEREST_WORDS = {"offer", "what can you offer", "how much", "cash", "interested", "curious", "talk", "price",
                  "numbers", "what’s your number", "whats your number", "what is your number"}
PRICE_WORDS = {"price", "asking", "$", " k", "k ", "number you have in mind", "how much", "range", "ballpark"}
COND_WORDS = {"condition", "repairs", "needs work", "renovated", "tenant", "tenants", "vacant", "occupied", "as-is"}
DELAY_WORDS = {"later", "next week", "tomorrow", "busy", "call me later", "text later", "reach out later", "follow up"}
NEG_WORDS = {"scam", "spam", "go away", "lose my number", "stop harassing", "reported", "lawsuit"}
WHO_PHRASES = {"who is this", "who's this", "whos this", "who are you", "who dis", "identify yourself"}
HOW_NUM_PHRASES = {"how did you get my number", "how did you get my #", "how you get my number",
                   "why do you have my number", "where did you get my number", "how got my number"}
NOT_OWNER_PHRASES = {"not the owner", "i sold", "no longer own", "dont own", "do not own", "sold this",
                     "belong to", "wrong person", "new owner"}
APPOINTMENT_WORDS = {"appointment", "schedule", "scheduled", "set up", "meet", "meeting", "tomorrow at",
                     "let's meet", "see you"}
CONTRACT_WORDS = {"under contract", "we signed", "signed contract", "in escrow", "closing",
                  "close next", "contract sent", "executed"}

def _has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(p in text for p in phrases)

# --- Public API ---------------------------------------------------
def classify_intent(body: str) -> str:
    """Return our coarse intent label for a raw inbound message body."""
    text = (body or "").lower().strip()

    if _has_any(text, STOP_WORDS):
        return "optout"
    if _has_any(text, CONTRACT_WORDS):
        return "under_contract"
    if _has_any(text, APPOINTMENT_WORDS):
        return "appointment_set"
    if _has_any(text, NOT_OWNER_PHRASES):
        return "not_owner"
    if _has_any(text, WRONG_WORDS):
        return "wrong_number"
    if _has_any(text, NEG_WORDS) or re.search(r"\b(" + "|".join(map(re.escape, NO_WORDS)) + r")\b", text):
        return "negative"
    if _has_any(text, DELAY_WORDS):
        return "delay"
    if _has_any(text, WHO_PHRASES) or _has_any(text, HOW_NUM_PHRASES):
        return "info_request"
    if _has_any(text, PRICE_WORDS) or _has_any(text, COND_WORDS):
        return "offer_discussion"
    if re.search(r"\b(" + "|".join(map(re.escape, YES_WORDS)) + r")\b", text) or _has_any(text, INTEREST_WORDS):
        return "positive"
    if any(term in text for term in ["maybe", "not sure", "thinking", "depends", "idk", "i don’t know", "i don't know"]):
        return "neutral"
    return "initial_contact"

__all__ = ["classify_intent"]