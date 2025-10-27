import pytest

from sms import textgrid_sender as tg


def test_validate_textgrid_payload_requires_from_number():
    payload = {"To": "+15551234567", "Body": "hello"}

    with pytest.raises(tg.TextGridError) as exc:
        tg._validate_textgrid_payload(payload)

    assert "From is required" in str(exc.value)


def test_send_message_exposes_error_body(monkeypatch):
    captured = {}

    def fake_log_conversation(**kwargs):
        captured.update(kwargs)

    def fake_http_post(*_a, **_k):
        raise tg.TextGridError(
            "TextGrid HTTP 400: Invalid",
            status_code=400,
            body={"message": "Invalid number"},
        )

    monkeypatch.setattr(tg, "ACCOUNT_SID", "AC123")
    monkeypatch.setattr(tg, "AUTH_TOKEN", "token")
    monkeypatch.setattr(tg, "API_URL", "https://example.com")
    monkeypatch.setattr(tg, "_log_conversation", fake_log_conversation)
    monkeypatch.setattr(tg, "_http_post", fake_http_post)

    with pytest.raises(tg.TextGridError) as exc:
        tg.send_message(from_number="+15550000000", to="+15551234567", message="hello")

    assert exc.value.body == {"message": "Invalid number"}
    assert captured["meta"]["error_body"] == {"message": "Invalid number"}

