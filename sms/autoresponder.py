"""AI autoresponder policy and intent classification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from . import spec
from .dispatcher import DISPATCHER, OutboundMessage


POSITIVE_KEYWORDS = {
    "yes",
    "yeah",
    "interested",
    "offer",
    "price",
    "cash",
    "deal",
}

NEGATIVE_KEYWORDS = {
    "stop",
    "no",
    "not interested",
    "wrong number",
    "dnc",
}

DELAY_KEYWORDS = {
    "later",
    "busy",
    "tomorrow",
    "next week",
    "follow up",
}

QUESTION_KEYWORDS = {
    "who",
    "how did you get",
    "what is this",
    "how got",
}


@dataclass
class IntentClassification:
    body: str
    ai_intent: str
    intent_detected: str
    stage: Optional[str]
    summary: str
    should_promote: bool


@dataclass
class AutoresponderDecision:
    should_send: bool
    body: Optional[str]
    template_id: Optional[str]
    processed_by: str
    quiet_hours: bool


def classify_intent(body: str) -> IntentClassification:
    lower = body.lower()
    intent_detected = "Neutral"
    ai_intent = "neutral"

    if any(keyword in lower for keyword in NEGATIVE_KEYWORDS):
        intent_detected = "Reject"
        ai_intent = "not_interested"
    elif any(keyword in lower for keyword in POSITIVE_KEYWORDS):
        intent_detected = "Positive"
        ai_intent = "interest_detected"
    elif any(keyword in lower for keyword in DELAY_KEYWORDS):
        intent_detected = "Delay"
        ai_intent = "delay"
    elif any(keyword in lower for keyword in QUESTION_KEYWORDS):
        intent_detected = "Neutral"
        ai_intent = "who_is_this"

    stage = spec.stage_for_intent(ai_intent)
    should_promote = spec.should_promote(intent_detected, ai_intent, stage)
    summary = f"intent={intent_detected}, ai={ai_intent}"
    return IntentClassification(
        body=body,
        ai_intent=ai_intent,
        intent_detected=intent_detected,
        stage=stage,
        summary=summary,
        should_promote=should_promote,
    )


def autoresponder_policy(classification: IntentClassification) -> AutoresponderDecision:
    processed_by = spec.MODEL_PRIORITY[0]

    if classification.intent_detected in {"Reject", "DNC"}:
        return AutoresponderDecision(False, None, None, processed_by, False)

    quiet = spec.is_quiet_hours()

    if classification.intent_detected == "Delay":
        body = "Totally understand – I'll follow up later this week."
        return AutoresponderDecision(True, body, None, "Autoresponder", quiet)

    if classification.intent_detected == "Positive":
        body = "Great! We'd love to learn more and work out a fair offer. When is a good time to chat?"
        return AutoresponderDecision(True, body, None, processed_by, quiet)

    if classification.ai_intent == "who_is_this":
        body = "Hi! I'm with the local home buying team checking in about your property."
        return AutoresponderDecision(True, body, None, processed_by, quiet)

    body = "Thanks for the reply — we're here if you'd like to talk about the property."
    return AutoresponderDecision(True, body, None, "Autoresponder", quiet)


def maybe_send_reply(
    *,
    from_number: str,
    to_number: str,
    classification: IntentClassification,
    conversation_id: str,
    campaign_id: Optional[str] = None,
) -> Optional[OutboundMessage]:
    decision = autoresponder_policy(classification)
    if not decision.should_send or not decision.body:
        return None

    message = OutboundMessage(
        to_number=from_number,
        from_number=to_number,
        body=decision.body,
        campaign_id=campaign_id,
        metadata={
            "processed_by": decision.processed_by,
            "conversation_id": conversation_id,
            "ai_intent": classification.ai_intent,
            "intent_detected": classification.intent_detected,
        },
    )

    DISPATCHER.queue(message)
    return message

