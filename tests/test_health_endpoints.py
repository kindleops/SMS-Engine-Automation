# tests/test_health_endpoints.py
from fastapi.testclient import TestClient
from sms.main import app

client = TestClient(app)


def test_healthz_endpoint():
    """Test that /healthz endpoint exists and returns expected response."""
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert "timestamp" in data
    assert "quiet_hours" in data
    assert "local_time_central" in data
    assert "version" in data


def test_health_endpoint():
    """Test that /health endpoint still works."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert "timestamp" in data
    assert "quiet_hours" in data
    assert "local_time_central" in data
    assert "version" in data


def test_ping_endpoint():
    """Test that /ping endpoint works."""
    response = client.get("/ping")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["pong"] is True
    assert "time" in data
