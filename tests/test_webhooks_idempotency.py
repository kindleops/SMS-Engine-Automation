from fastapi import FastAPI
from fastapi.testclient import TestClient

import sms.outbound_webhook as outbound_webhook
import sms.delivery_webhook as delivery_webhook


def test_outbound_logs_with_required_fields(monkeypatch):
    captured = {}

    def fake_resolve(phone):
        captured["resolved"] = phone
        return {"id": "lead1", "fields": {}}, None

    def fake_upsert(payload, sid):
        captured["payload"] = payload
        captured["sid"] = sid
        return "rec123"

    def fake_update_activity(lead, **kwargs):
        captured["activity"] = (lead, kwargs)

    def fake_link(*args, **kwargs):
        captured["linked"] = (args, kwargs)

    monkeypatch.setattr(outbound_webhook, "resolve_contact_links", fake_resolve)
    monkeypatch.setattr(outbound_webhook, "upsert_conversation", fake_upsert)
    monkeypatch.setattr(outbound_webhook, "update_lead_activity", fake_update_activity)
    monkeypatch.setattr(outbound_webhook, "update_conversation_links", fake_link)

    payload = {
        "To": "+15555550123",
        "From": "+18885551234",
        "Body": "Campaign send",
        "MessageSid": "SM-OUT-1",
        "Processed By": "Campaign Runner",
        "Stage": "STAGE 6 - OFFER FOLLOW UP",
        "Campaign ID": "camp1",
    }

    result = outbound_webhook.handle_outbound(payload)

    assert result == {"status": "ok", "conversation_id": "rec123", "linked_to": "lead"}
    assert captured["sid"] == "SM-OUT-1"
    assert captured["payload"]["Stage"] == "STAGE 6 - OFFER FOLLOW UP"
    assert captured["payload"]["Direction"] == "OUTBOUND"
    assert captured["payload"]["Processed By"] == "Campaign Runner"
    assert captured["activity"][1]["send_increment"] is True


def test_delivery_updates_existing_conversation(monkeypatch):
    class FakeTable:
        def __init__(self):
            self.updated = []
            self.records = [
                {
                    "id": "rec_convo",
                    "fields": {
                        "TextGrid ID": "SM-DEL-1",
                        "Delivery Status": "SENT",
                        "Message Long text": "Hello",
                        "Lead": ["lead1"],
                    },
                }
            ]

        def all(self):
            return list(self.records)

        def update(self, rec_id, payload):
            self.updated.append((rec_id, payload))
            return {"id": rec_id, "fields": payload}

    class FakeLeads:
        def get(self, rec_id):
            return {"id": rec_id, "fields": {"Reply Count": 1, "Sent Count": 2}}

    captured = {}

    fake_convos = FakeTable()
    monkeypatch.setattr(delivery_webhook, "get_convos", lambda: fake_convos)
    monkeypatch.setattr(delivery_webhook, "get_leads", lambda: FakeLeads())
    monkeypatch.setattr(
        delivery_webhook,
        "update_lead_activity",
        lambda lead, **kwargs: captured.setdefault("lead_activity", []).append((lead, kwargs)),
    )
    monkeypatch.setattr(
        delivery_webhook,
        "update_conversation_links",
        lambda *args, **kwargs: captured.setdefault("links", []).append((args, kwargs)),
    )

    app = FastAPI()
    app.include_router(delivery_webhook.router)
    client = TestClient(app)

    resp = client.post("/delivery", json={"MessageSid": "SM-DEL-1", "MessageStatus": "delivered"})
    assert resp.status_code == 200
    data = resp.json()
    assert data == {"status": "ok", "sid": "SM-DEL-1", "normalized": "DELIVERED"}

    assert fake_convos.updated
    rec_id, update_payload = fake_convos.updated[0]
    assert rec_id == "rec_convo"
    assert update_payload["Delivery Status"] == "DELIVERED"
    assert captured["lead_activity"][0][1]["status_changed"] is True
