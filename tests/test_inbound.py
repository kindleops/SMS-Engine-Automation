from fastapi.testclient import TestClient

from sms.main import app
from sms.datastore import conversation_by_sid
from sms import spec


client = TestClient(app)


def test_inbound_missing_fields():
    response = client.post("/inbound", data={"From": "", "Body": ""})
    assert response.status_code == 422


def test_inbound_optout():
    payload = {"From": "+15550001111", "Body": "STOP"}
    response = client.post("/inbound", data=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "optout"


def test_inbound_normal_flow():
    payload = {
        "From": "+15550003333",
        "To": "+15550004444",
        "Body": "Yes I'm interested",
        "MessageSid": "SM-inbound-1",
    }
    response = client.post("/inbound", data=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    record = conversation_by_sid("SM-inbound-1")
    assert record is not None
    fields = record.get("fields", {})
    assert fields.get(spec.CONVERSATION_FIELDS.intent_detected) == "Positive"
