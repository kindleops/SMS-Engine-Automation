import importlib

import pytest
from fastapi import HTTPException

import sms.inbound_webhook as inbound_webhook


@pytest.fixture(autouse=True)
def reset_inbound_state():
    inbound_webhook._SEEN_MESSAGE_IDS.clear()
    yield
    inbound_webhook._SEEN_MESSAGE_IDS.clear()


def test_inbound_promotes_on_positive_intent(monkeypatch):
    """Inbound webhook should promote when positive intent is detected."""

    captured = {}

    def fake_lookup(phone):
        return None, None

    def fake_promote(phone, source="Inbound"):
        captured["promoted"] = phone
        return "lead123", "prop123"

    def fake_log(payload):
        captured.setdefault("logged", []).append(payload)

    def fake_update(lead_id, body, direction, reply_increment=False):
        captured["updated"] = (lead_id, body, direction, reply_increment)

    monkeypatch.setattr(inbound_webhook, "_lookup_existing_lead", fake_lookup)
    monkeypatch.setattr(inbound_webhook, "promote_prospect_to_lead", fake_promote)
    monkeypatch.setattr(inbound_webhook, "log_conversation", fake_log)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", fake_update)

    payload = {
        "From": "+15555550123",
        "To": "+18885551234",
        "Body": "Yes I'm interested in your offer",
        "MessageSid": "SM-positive",
    }

    result = inbound_webhook.handle_inbound(payload)

    assert result["status"] == "ok"
    assert result["stage"] == inbound_webhook.STAGE_SEQUENCE[2]
    assert result["promoted"] is True
    assert "promoted" in captured
    assert captured["logged"][0][inbound_webhook.STAGE_FIELD] == inbound_webhook.STAGE_SEQUENCE[2]
    assert inbound_webhook.INTENT_FIELD in captured["logged"][0]
    assert "updated" in captured


def test_inbound_neutral_does_not_promote(monkeypatch):
    """Neutral inbound should not trigger promotion."""

    captured = {}

    def fake_lookup(phone):
        return None, None

    def fake_promote(phone, source="Inbound"):
        captured["promoted"] = True
        return "lead123", "prop123"

    def fake_log(payload):
        captured.setdefault("logged", []).append(payload)

    monkeypatch.setattr(inbound_webhook, "_lookup_existing_lead", fake_lookup)
    monkeypatch.setattr(inbound_webhook, "promote_prospect_to_lead", fake_promote)
    monkeypatch.setattr(inbound_webhook, "log_conversation", fake_log)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", lambda *a, **k: None)

    payload = {
        "From": "+15555550123",
        "To": "+18885551234",
        "Body": "Hello just checking in",
        "MessageSid": "SM-neutral",
    }

    result = inbound_webhook.handle_inbound(payload)

    assert result["status"] == "ok"
    assert result["stage"] == inbound_webhook.STAGE_SEQUENCE[0]
    assert result["promoted"] is False
    assert "promoted" not in captured
    assert captured["logged"][0][inbound_webhook.STAGE_FIELD] == inbound_webhook.STAGE_SEQUENCE[0]


def test_inbound_duplicate_is_idempotent(monkeypatch):
    """Posting the same MessageSid twice should not duplicate logs."""

    call_count = {"log": 0}

    monkeypatch.setattr(inbound_webhook, "_lookup_existing_lead", lambda *_: (None, None))
    monkeypatch.setattr(inbound_webhook, "promote_prospect_to_lead", lambda *_: (None, None))

    def fake_log(payload):
        call_count["log"] += 1

    monkeypatch.setattr(inbound_webhook, "log_conversation", fake_log)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", lambda *a, **k: None)

    payload = {
        "From": "+15555550123",
        "Body": "Yes I can chat",
        "MessageSid": "SM-dup",
    }

    first = inbound_webhook.handle_inbound(payload)
    second = inbound_webhook.handle_inbound(payload)

    assert first["status"] == "ok"
    assert second["status"] == "duplicate"
    assert call_count["log"] == 1


def test_inbound_optout(monkeypatch):
    """STOP-like messages should log opt-out without promoting new leads."""

    captured = {}

    monkeypatch.setattr(inbound_webhook, "increment_opt_out", lambda phone: captured.setdefault("optout", []).append(phone))
    monkeypatch.setattr(inbound_webhook, "_lookup_existing_lead", lambda *_: (None, None))
    monkeypatch.setattr(inbound_webhook, "promote_prospect_to_lead", lambda *_: (_ for _ in ()).throw(Exception("should not promote")))
    monkeypatch.setattr(inbound_webhook, "log_conversation", lambda payload: captured.setdefault("logged", []).append(payload))
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", lambda *a, **k: captured.setdefault("updated", True))
    monkeypatch.setattr(inbound_webhook, "_lookup_prospect_property", lambda *_: "prop123")

    payload = {"From": "+15555550123", "Body": "STOP", "MessageSid": "SM-stop"}

    result = inbound_webhook.process_optout(payload)

    assert result["status"] == "optout"
    assert "optout" in captured
    assert captured["logged"][0][inbound_webhook.STAGE_FIELD] == "OPT OUT"


def test_inbound_missing_fields_raise():
    """Missing body or from should raise HTTPException 422."""

    with pytest.raises(HTTPException):
        inbound_webhook.handle_inbound({"From": None, "Body": "Hello"})

    with pytest.raises(HTTPException):
        inbound_webhook.handle_inbound({"From": "+1555", "Body": None})
