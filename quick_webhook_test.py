#!/usr/bin/env python3
"""
Quick test to verify webhook endpoint works without Airtable operations
"""

import json
import requests
import sys

def test_minimal_webhook():
    url = "https://rei-sms-engine.onrender.com/inbound"
    token = "bb2f2c97f36a4b9fa3ec2f8d6a5d7c52f83b4c0de719ebc44d40a90aa3d0c53a"
    
    payload = {
        "MessageSid": "SM1234567890", 
        "From": "+15551234567",
        "To": "+19725551234", 
        "Body": "TEST"
    }
    
    try:
        print("🔄 Testing webhook endpoint...")
        response = requests.post(
            f"{url}?token={token}",
            data=payload,
            timeout=10,
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        print(f"✅ Status: {response.status_code}")
        print(f"📋 Response: {response.text}")
        print(f"⏱️  Time: {response.elapsed.total_seconds():.2f}s")
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    test_minimal_webhook()