from fastapi.testclient import TestClient

from sms.main import app
from sms.datastore import conversation_by_sid
from sms import spec


client = TestClient(app)


def test_delivery_updates_status_and_counters():
    # Seed outbound conversation
    client.post(
        "/outbound",
        data={
            "From": "+15550007777",
            "To": "+15550008888",
            "Body": "Outbound test",
            "MessageSid": "SM-out-1",
        },
    )

    response = client.post(
        "/delivery",
        data={
            "MessageSid": "SM-out-1",
            "MessageStatus": "delivered",
            "To": "+15550008888",
            "From": "+15550007777",
        },
    )
    assert response.status_code == 200
    assert response.json()["normalized"] == "DELIVERED"
    record = conversation_by_sid("SM-out-1")
    assert record.get("fields", {}).get(spec.CONVERSATION_FIELDS.delivery_status) == "DELIVERED"
