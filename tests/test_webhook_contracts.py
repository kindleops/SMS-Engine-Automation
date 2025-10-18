import importlib
import re
import sys
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _setup_outbound(monkeypatch):
    tables = {}

    class DummyTable:
        def __init__(self, api_key, base_id, table_name):
            self.name = table_name
            self.storage = tables.setdefault(table_name, [])

        def all(self, formula=None, max_records=None):
            records = list(self.storage)
            if formula:
                match = re.match(r"\{([^}]+)\}='([^']*)'", formula)
                if match:
                    field, value = match.groups()
                    records = [r for r in records if r.get("fields", {}).get(field) == value]
                else:
                    records = []
            if max_records:
                records = records[:max_records]
            return records

        def create(self, payload):
            record = {"id": f"{self.name}-{len(self.storage)+1}", "fields": dict(payload)}
            self.storage.append(record)
            return record

        def update(self, rec_id, payload):
            for record in self.storage:
                if record["id"] == rec_id:
                    record["fields"].update(payload)
                    return record
            raise KeyError(rec_id)

    monkeypatch.setenv("AIRTABLE_API_KEY", "test-key")
    monkeypatch.setenv("LEADS_CONVOS_BASE", "base")
    monkeypatch.setenv("CONVERSATIONS_TABLE", "Conversations")
    monkeypatch.setenv("LEADS_TABLE", "Leads")
    monkeypatch.setenv("PROSPECTS_TABLE", "Prospects")
    monkeypatch.setenv("CAMPAIGNS_TABLE", "Campaigns")
    monkeypatch.setenv("TEMPLATES_TABLE", "Templates")

    original = sys.modules.get("pyairtable")
    monkeypatch.setitem(sys.modules, "pyairtable", types.SimpleNamespace(Table=DummyTable))

    sys.modules.pop("sms.outbound_webhook", None)
    outbound_webhook = importlib.import_module("sms.outbound_webhook")

    if original is not None:
        monkeypatch.setitem(sys.modules, "pyairtable", original)

    return outbound_webhook, tables


def test_outbound_idempotent_logging(monkeypatch):
    outbound_webhook, tables = _setup_outbound(monkeypatch)

    tables.setdefault("Leads", []).append({"id": "lead-1", "fields": {"phone": "+18885551234"}})

    app = FastAPI()
    app.include_router(outbound_webhook.router)
    client = TestClient(app)

    payload = {
        "To": "+18885551234",
        "From": "+12223334444",
        "Body": "First touch",
        "MessageSid": "SM-out-1",
    }

    resp1 = client.post("/outbound", data=payload)
    assert resp1.status_code == 200
    first_id = resp1.json()["record_id"]

    assert len(tables["Conversations"]) == 1
    assert tables["Conversations"][0]["fields"][outbound_webhook.MSG_FIELD] == "First touch"

    payload["Body"] = "Updated touch"
    resp2 = client.post("/outbound", data=payload)
    assert resp2.status_code == 200
    assert resp2.json()["record_id"] == first_id

    assert len(tables["Conversations"]) == 1
    assert tables["Conversations"][0]["fields"][outbound_webhook.MSG_FIELD] == "Updated touch"


def test_delivery_receipts(monkeypatch):
    monkeypatch.delenv("UPSTASH_REDIS_REST_URL", raising=False)
    monkeypatch.delenv("UPSTASH_REDIS_REST_TOKEN", raising=False)
    monkeypatch.delenv("UPSTASH_REDIS_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)

    delivery_webhook = importlib.import_module("sms.delivery_webhook")
    importlib.reload(delivery_webhook)

    events = {"bump": [], "convo": [], "drip": []}

    def fake_bump(did, delivered):
        events["bump"].append((did, delivered))

    monkeypatch.setattr(delivery_webhook, "_bump_numbers_counters", fake_bump)
    monkeypatch.setattr(delivery_webhook, "_update_conversation_by_sid", lambda *a, **k: events["convo"].append(a))
    monkeypatch.setattr(delivery_webhook, "_update_drip_queue_by_sid", lambda *a, **k: events["drip"].append(a))

    delivery_webhook.IDEM = delivery_webhook.IdemStore()

    app = FastAPI()
    app.include_router(delivery_webhook.router)
    client = TestClient(app)

    payload = {
        "MessageSid": "SM-dlv-1",
        "MessageStatus": "delivered",
        "From": "+12223334444",
        "To": "+18885551234",
    }

    resp = client.post("/delivery", json=payload)
    assert resp.status_code == 200
    assert resp.json()["status"] == "delivered"
    assert events["bump"] == [("+12223334444", True)]

    # Duplicate should be ignored by idempotency store
    resp_dup = client.post("/delivery", json=payload)
    assert resp_dup.status_code == 200
    assert resp_dup.json().get("note") == "duplicate ignored"
    assert events["bump"] == [("+12223334444", True)]

    # Missing To/From should return 422 and not mutate counters
    bad_payload = {"MessageSid": "SM-dlv-2", "MessageStatus": "delivered", "From": ""}
    resp_bad = client.post("/delivery", json=bad_payload)
    assert resp_bad.status_code == 422
    assert events["bump"] == [("+12223334444", True)]
