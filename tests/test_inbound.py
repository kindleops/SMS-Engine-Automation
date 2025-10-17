import pytest
import pytest
from fastapi import HTTPException

import sms.inbound_webhook as inbound_webhook


def test_inbound_positive_promotes_and_logs(monkeypatch):
    """Positive inbound should log, promote to lead, and update lead activity."""

    calls = {}

    def fake_resolve(phone):
        calls["resolved"] = phone
        return None, {"id": "pros1"}

    def fake_upsert(payload, sid):
        calls["logged"] = payload
        calls["sid"] = sid
        return "convo1"

    def fake_promote(phone, source="Inbound", campaign_id=None):
        calls["promoted"] = (phone, source)
        return {"id": "lead1", "fields": {}}

    def fake_link(convo_id, **kwargs):
        calls.setdefault("linked", []).append((convo_id, kwargs))

    def fake_update_activity(lead, **kwargs):
        calls.setdefault("updated", []).append((lead, kwargs))

    monkeypatch.setattr(inbound_webhook, "resolve_contact_links", fake_resolve)
    monkeypatch.setattr(inbound_webhook, "upsert_conversation", fake_upsert)
    monkeypatch.setattr(inbound_webhook, "promote_to_lead", fake_promote)
    monkeypatch.setattr(inbound_webhook, "update_conversation_links", fake_link)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", fake_update_activity)
    monkeypatch.setattr(inbound_webhook, "increment_opt_out", lambda *_: None)

    payload = {
        "From": "+15555550123",
        "To": "+18885551234",
        "Body": "Yes I'm interested in the price",
        "MessageSid": "SM-positive",
    }

    result = inbound_webhook.handle_inbound(payload)

    assert result == {
        "status": "ok",
        "conversation_id": "convo1",
        "linked_to": "lead",
        "stage": "STAGE 3 - PRICE QUALIFICATION",
        "intent": "Positive",
    }
    assert calls["sid"] == "SM-positive"
    assert calls["logged"]["Stage"] == "STAGE 3 - PRICE QUALIFICATION"
    assert calls["logged"]["Delivery Status"] == "DELIVERED"
    assert calls["promoted"] == ("+15555550123", "Inbound")
    assert calls["updated"][0][0]["id"] == "lead1"


def test_inbound_optout_sets_status(monkeypatch):
    """STOP payloads should opt-out without promoting to a lead."""

    calls = {}

    def fake_resolve(phone):
        calls["resolved"] = phone
        return None, {"id": "pros2"}

    monkeypatch.setattr(inbound_webhook, "resolve_contact_links", fake_resolve)
    monkeypatch.setattr(inbound_webhook, "upsert_conversation", lambda payload, sid: "convo-stop")
    monkeypatch.setattr(inbound_webhook, "promote_to_lead", lambda *_: None)
    monkeypatch.setattr(inbound_webhook, "update_conversation_links", lambda *a, **k: None)
    monkeypatch.setattr(inbound_webhook, "update_lead_activity", lambda *a, **k: None)

    def fake_increment(num):
        calls["optout"] = num

    monkeypatch.setattr(inbound_webhook, "increment_opt_out", fake_increment)

    payload = {"From": "+15555550000", "Body": "STOP", "MessageSid": "SM-stop"}

    result = inbound_webhook.process_optout(payload)

    assert result == {
        "status": "optout",
        "conversation_id": "convo-stop",
        "linked_to": "prospect",
        "stage": "OPT OUT",
        "intent": "DNC",
    }
    assert calls["optout"] == "+15555550000"


def test_inbound_missing_fields():
    """Inbound webhook should raise if missing From or Body."""

    with pytest.raises(HTTPException):
        inbound_webhook.handle_inbound({"From": None, "Body": "Hello"})

    with pytest.raises(HTTPException):
        inbound_webhook.handle_inbound({"From": "+1555", "Body": None})
