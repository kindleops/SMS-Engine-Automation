import pytest
from fastapi import HTTPException
import sms.inbound_webhook as inbound_webhook

def test_status_delivered(monkeypatch):
    called = {}

    def fake_inc_delivered(num):
        called["delivered"] = num

    monkeypatch.setattr(inbound_webhook, "increment_delivered", fake_inc_delivered)
    monkeypatch.setattr(inbound_webhook, "increment_failed", lambda _: None)
    monkeypatch.setattr(inbound_webhook, "convos", None)
    monkeypatch.setattr(inbound_webhook, "leads", None)

    payload = {"MessageSid": "SM123", "MessageStatus": "delivered", "To": "+15555550123", "From": "+18885551234"}
    result = inbound_webhook.process_status(payload)

    assert result["ok"] is True
    assert called["delivered"] == "+18885551234"

def test_status_failed(monkeypatch):
    called = {}

    def fake_inc_failed(num):
        called["failed"] = num

    monkeypatch.setattr(inbound_webhook, "increment_delivered", lambda _: None)
    monkeypatch.setattr(inbound_webhook, "increment_failed", fake_inc_failed)
    monkeypatch.setattr(inbound_webhook, "convos", None)
    monkeypatch.setattr(inbound_webhook, "leads", None)

    payload = {"MessageSid": "SM124", "MessageStatus": "failed", "To": "+15555550123", "From": "+18885551234"}
    result = inbound_webhook.process_status(payload)

    assert result["ok"] is True
    assert called["failed"] == "+18885551234"

def test_status_missing_fields():
    with pytest.raises(HTTPException):
        inbound_webhook.process_status({"MessageStatus": "delivered"})  # Missing To