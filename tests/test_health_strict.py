# tests/test_health_strict.py
import pytest
from fastapi import HTTPException
from sms import health_strict


def test_invalid_mode_raises():
    """Invalid mode should raise 400, not 500."""
    with pytest.raises(HTTPException) as exc:
        health_strict.strict_health(mode="badmode")
    assert exc.value.status_code == 400


def test_missing_env(monkeypatch):
    """If env vars are missing, should raise 500."""
    monkeypatch.setenv("AIRTABLE_API_KEY", "")
    monkeypatch.setenv("LEADS_CONVOS_BASE", "")
    with pytest.raises(HTTPException) as exc:
        health_strict.strict_health(mode="prospects")
    assert exc.value.status_code == 500


def test_strict_health_success(monkeypatch):
    """Simulate healthy Airtable responses."""

    class FakeTable:
        def __init__(self, *args, **kwargs):
            pass

        def all(self, max_records=1):
            return [{"id": "rec123"}]

    monkeypatch.setattr("sms.health_strict.Table", FakeTable)

    result = health_strict.strict_health(mode="prospects", api_key="fake_key", base_id="fake_base")
    assert result["ok"] is True
    assert result["mode"] == "prospects"
    assert "timestamp" in result
