import pytest
from fastapi import HTTPException
import sms.inbound_webhook as inbound_webhook


def test_inbound_valid(monkeypatch):
    """Inbound webhook should accept valid payload and call downstream logic."""

    called = {}

    def fake_promote(phone, source="Inbound"):
        called["promoted"] = phone
        return "lead123", "prop123"

    def fake_log(payload):
        called["logged"] = payload
        return {"id": "rec123"}

    def fake_update(lead_id, body, direction, reply_increment=False):
        called["updated"] = (lead_id, body, direction, reply_increment)

    monkeypatch.setattr(inbound_webhook, "promote_prospect_to_lead", fake_promote)
    monkeypatch.setattr(inbound_webhook, "log_conversation", fake_log)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", fake_update)

    payload = {
        "From": "+15555550123",
        "To": "+18885551234",
        "Body": "Hello there",
        "MessageSid": "SM123",
    }

    result = inbound_webhook.handle_inbound(payload)

    assert result["status"] == "ok"
    assert result["linked_to"] == "lead"
    assert result["conversation_id"] == "rec123"
    assert "promoted" in called
    assert "logged" in called
    assert "updated" in called


def test_inbound_optout(monkeypatch):
    """Opt-out webhook should handle STOP message and mark as optout."""

    called = {}

    def fake_increment(num):
        called["optout"] = num

    def fake_promote(phone, source="Opt-Out"):
        called["promoted"] = phone
        return "lead123", None

    def fake_log(payload):
        called["logged"] = payload
        return {"id": "rec123"}

    def fake_update(lead_id, body, direction, reply_increment=False):
        called["updated"] = (lead_id, body, direction, reply_increment)

    monkeypatch.setattr(inbound_webhook, "increment_opt_out", fake_increment)
    monkeypatch.setattr(inbound_webhook, "promote_prospect_to_lead", fake_promote)
    monkeypatch.setattr(inbound_webhook, "log_conversation", fake_log)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", fake_update)

    payload = {
        "From": "+15555550123",
        "Body": "STOP",
    }

    result = inbound_webhook.process_optout(payload)

    assert result["status"] == "optout"
    assert result["conversation_id"] == "rec123"
    assert "optout" in called
    assert "promoted" in called
    assert "logged" in called


def test_inbound_missing_fields():
    """Inbound webhook should raise if missing From or Body."""

    with pytest.raises(HTTPException):
        inbound_webhook.handle_inbound({"From": None, "Body": "Hello"})

    with pytest.raises(HTTPException):
        inbound_webhook.handle_inbound({"From": "+1555", "Body": None})
