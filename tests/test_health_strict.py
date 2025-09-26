# tests/test_health_strict.py
import os
import pytest
from fastapi import HTTPException

from sms import health_strict


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    """Ensure env vars are set for tests."""
    monkeypatch.setenv("AIRTABLE_API_KEY", "fake_api_key")
    monkeypatch.setenv("LEADS_CONVOS_BASE", "fake_base_id")


def test_invalid_mode_raises():
    with pytest.raises(HTTPException) as exc:
        health_strict.strict_health(mode="badmode")
    assert exc.value.status_code == 400
    assert "Invalid mode" in str(exc.value.detail)


def test_missing_env(monkeypatch):
    monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
    with pytest.raises(HTTPException) as exc:
        health_strict.strict_health(mode="prospects")
    assert exc.value.status_code == 500
    assert "Missing Airtable API key" in str(exc.value.detail)


def test_strict_health_success(monkeypatch):
    """Patch Table to always succeed."""
    class FakeTable:
        def __init__(self, *args, **kwargs): pass
        def all(self, max_records=1): return [{"id": "rec123"}]

    monkeypatch.setattr("sms.health_strict.Table", FakeTable)

    result = health_strict.strict_health(mode="prospects")
    assert result["ok"] is True
    assert "Prospects" in result["checked"]

    result = health_strict.strict_health(mode="leads")
    assert result["ok"] is True
    assert "Leads" in result["checked"]

    result = health_strict.strict_health(mode="inbounds")
    assert result["ok"] is True
    assert "Conversations" in result["checked"]


def test_strict_health_failure(monkeypatch):
    """Patch Table to throw for one table."""
    class FailingTable:
        def __init__(self, *args, **kwargs): pass
        def all(self, max_records=1): raise Exception("Network error")

    monkeypatch.setattr("sms.health_strict.Table", FailingTable)

    with pytest.raises(HTTPException) as exc:
        health_strict.strict_health(mode="prospects")
    assert exc.value.status_code == 500
    assert "errors" in exc.value.detail