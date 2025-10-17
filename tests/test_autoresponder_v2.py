from sms import autoresponder
from sms.dispatcher import DISPATCHER


def test_classify_positive_intent():
    classification = autoresponder.classify_intent("Yes I'm interested in your offer price")
    assert classification.intent_detected == "Positive"
    assert classification.ai_intent == "interest_detected"
    assert classification.stage == "STAGE 3 - PRICE QUALIFICATION"
    assert classification.should_promote is True


def test_autoresponder_queues_message():
    # Ensure queue empty
    DISPATCHER.pop_ready()

    classification = autoresponder.classify_intent("Who is this?")
    message = autoresponder.maybe_send_reply(
        from_number="+15550001111",
        to_number="+15550002222",
        classification=classification,
        conversation_id="rec_1",
    )
    assert message is not None
    assert DISPATCHER.pending_count() >= 1
    assert message.body.startswith("Hi!")
