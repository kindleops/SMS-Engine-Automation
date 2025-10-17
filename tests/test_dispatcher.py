import pytest

from sms import dispatcher


def _fake_result(processed=0):
    return {"processed": processed, "results": [], "errors": []}


def test_run_engine_handles_non_string_mode():
    result = dispatcher.run_engine(123)

    assert result["ok"] is False
    assert result["type"] == "Unknown"
    assert "123" in result["error"]


def test_run_engine_respects_quiet_hours(monkeypatch):
    captured = {}

    def fake_campaigns(*, limit, send_after_queue):
        captured["limit"] = limit
        captured["send_after_queue"] = send_after_queue
        return _fake_result(processed=1)

    monkeypatch.setattr(dispatcher, "run_campaigns", fake_campaigns)
    monkeypatch.setattr(dispatcher, "_is_quiet_hours_outbound", lambda: True)

    result = dispatcher.run_engine("prospects", send_after_queue=True, limit=5)

    assert result["ok"] is True
    assert result["quiet_hours"] is True
    assert captured["limit"] == 5
    assert captured["send_after_queue"] is False


def test_load_quiet_timezone_invalid_env(monkeypatch):
    if dispatcher.ZoneInfo is None:
        pytest.skip("ZoneInfo not available")

    monkeypatch.setenv("QUIET_TZ", "Not/A_Zone")

    tz = dispatcher._load_quiet_timezone()

    assert tz is None
