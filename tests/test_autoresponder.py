import pytest
from sms import autoresponder


def test_autoresponder_basic(monkeypatch):
    """Simulate autoresponder flow with fake send + fake Airtable tables."""
    sent = {}

    # Fake send function that captures the outbound message
    def fake_send(phone, body, lead_id=None, property_id=None, direction=None):
        sent["to"] = phone
        sent["body"] = body
        return {"status": "sent"}  # Simulate success

    # Fake Airtable-like table
    class FakeTable:
        def __init__(self, *a, **k): pass
        def all(self, *a, **k):
            return [{"id": "msg1", "fields": {"phone": "+15555550123", "message": "yes"}}]
        def first(self, *a, **k):
            return {"id": "rec123", "fields": {"phone": "+15555550123"}}
        def create(self, data):
            return {"id": "new_lead", "fields": data}
        def update(self, *a, **k):
            return True

    # Patch dependencies inside autoresponder
    monkeypatch.setattr("sms.autoresponder.get_convos", lambda: FakeTable())
    monkeypatch.setattr("sms.autoresponder.get_leads", lambda: FakeTable())
    monkeypatch.setattr("sms.autoresponder.get_prospects", lambda: FakeTable())
    monkeypatch.setattr("sms.autoresponder.get_templates", lambda: FakeTable())
    monkeypatch.setattr("sms.message_processor.MessageProcessor.send", fake_send)

    result = autoresponder.run_autoresponder(limit=1)

    assert result["processed"] == 1
    assert sent["to"] == "+15555550123"
    assert "body" in sent


def test_autoresponder_invalid(monkeypatch):
    """Autoresponder should gracefully handle missing Conversations table."""
    monkeypatch.setattr("sms.autoresponder.get_convos", lambda: None)

    result = autoresponder.run_autoresponder(limit=1)

    assert result["ok"] is False
    assert "errors" in result