# sms/intent.py
"""
Intent Classifier
-----------------
Rule-based text intent detection for inbound SMS replies.
Optimized for autoresponder + followup scheduling pipeline.
"""

from __future__ import annotations
import re
import string
from typing import Iterable

# -----------------------------
# Lexicons
# -----------------------------
STOP = {"stop", "unsubscribe", "remove", "quit", "cancel", "end"}
YES = {"yes", "yeah", "yep", "sure", "correct", "that's me", "that is me", "affirmative", "of course", "i am"}
NO = {"no", "nope", "nah", "not interested", "dont bother", "stop texting"}
WRONG = {"wrong number", "not mine", "dont own", "do not own", "new number"}
INTEREST = {"offer", "how much", "price", "cash", "interested", "numbers", "what can you offer", "curious", "quote"}
PRICE = {"asking", "$", " k", "k ", "number you have in mind", "range", "ballpark"}
COND = {"condition", "repairs", "needs work", "renovated", "tenant", "vacant", "occupied", "as-is"}
DELAY = {"later", "next week", "tomorrow", "busy", "follow up", "reach out", "call me later"}
NEG = {"scam", "spam", "go away", "lose my number", "lawsuit"}
WHO = {"who is this", "who's this", "who dis", "who are you"}
HOW_NUM = {"how did you get my number", "why do you have my number", "where did you get my number"}
NOT_OWNER = {"not the owner", "i sold", "no longer own", "sold this", "wrong person", "new owner"}
APPT = {"appointment", "schedule", "set up", "meet", "meeting", "tomorrow at", "see you"}
CONTRACT = {"under contract", "signed", "in escrow", "closing", "executed"}


# -----------------------------
# Utils
# -----------------------------
def _norm(text: str) -> str:
    return text.lower().translate(str.maketrans("", "", string.punctuation)).strip()


def _has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(p in text for p in phrases)


def _match_words(text: str, words: Iterable[str]) -> bool:
    pattern = r"\b(" + "|".join(map(re.escape, words)) + r")\b"
    return bool(re.search(pattern, text))


# -----------------------------
# Main classifier
# -----------------------------
def classify_intent(body: str) -> str:
    """Return standardized intent label for inbound SMS body."""
    text = _norm(body or "")
    if not text:
        return "blank"

    if _has_any(text, STOP):
        return "optout"
    if _has_any(text, CONTRACT):
        return "under_contract"
    if _has_any(text, APPT):
        return "appointment"
    if _has_any(text, NOT_OWNER) or _has_any(text, WRONG):
        return "wrong_number"
    if _match_words(text, NO) or _has_any(text, NEG):
        return "followup_no"
    if _has_any(text, DELAY):
        return "delay"
    if _has_any(text, WHO) or _has_any(text, HOW_NUM):
        return "inquiry"
    if _has_any(text, PRICE) or _has_any(text, COND):
        return "price_response"
    if _match_words(text, YES) or _has_any(text, INTEREST):
        return "followup_yes"
    if any(t in text for t in ["maybe", "not sure", "thinking", "depends", "idk", "i don't know", "i dont know"]):
        return "neutral"
    return "intro"


__all__ = ["classify_intent"]
