import os
from fastapi.testclient import TestClient

# Force mock mode (disable Airtable in tests)
os.environ.pop("AIRTABLE_API_KEY", None)
os.environ.pop("CAMPAIGN_CONTROL_BASE", None)
os.environ.pop("LEADS_CONVOS_BASE", None)

from sms import inbound_webhook

# Wrap FastAPI router in a test client
from fastapi import FastAPI
app = FastAPI()
app.include_router(inbound_webhook.router)
client = TestClient(app)

# --- Simulate inbound message ---
def test_inbound():
    payload = {
        "From": "+15555550123",
        "To": "+14444440123",
        "Body": "Yes I'm interested",
        "MessageSid": "SM123456789"
    }
    resp = client.post("/inbound", data=payload)
    print("Inbound Response:", resp.json())

# --- Simulate opt-out ---
def test_optout():
    payload = {
        "From": "+15555550123",
        "Body": "STOP"
    }
    resp = client.post("/optout", data=payload)
    print("Opt-Out Response:", resp.json())

# --- Simulate delivery receipt ---
def test_status():
    payload = {
        "MessageSid": "SM123456789",
        "MessageStatus": "delivered",
        "To": "+15555550123",
        "From": "+14444440123"
    }
    resp = client.post("/status", data=payload)
    print("Status Response:", resp.json())

if __name__ == "__main__":
    test_inbound()
    test_optout()
    test_status()