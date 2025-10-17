import sms.inbound_webhook as inbound_webhook


def test_status_normalizes_delivered():
    payload = {"MessageSid": "SM123", "MessageStatus": "delivered"}
    result = inbound_webhook.process_status(payload)
    assert result == {"ok": True, "status": "delivered"}


def test_status_normalizes_failed():
    payload = {"MessageSid": "SM124", "MessageStatus": "undelivered"}
    result = inbound_webhook.process_status(payload)
    assert result == {"ok": True, "status": "failed"}
