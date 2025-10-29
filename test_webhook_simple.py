#!/usr/bin/env python3

import requests
import time

def test_simple_health():
    """Test health endpoint"""
    url = "https://rei-sms-engine.onrender.com/health"
    try:
        start = time.time()
        response = requests.get(url, timeout=5)
        elapsed = time.time() - start
        print(f"✅ Health check: {response.status_code} in {elapsed:.2f}s")
        return True
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

def test_webhook_minimal():
    """Test webhook with minimal payload"""
    url = "https://rei-sms-engine.onrender.com/inbound"
    params = {"token": "bb2f2c97f36a4b9fa3ec2f8d6a5d7c52f83b4c0de719ebc44d40a90aa3d0c53a"}
    data = {
        "MessageSid": "TEST123",
        "From": "+15551234567",
        "To": "+19725551234", 
        "Body": "Quick test"
    }
    
    try:
        start = time.time()
        response = requests.post(url, params=params, data=data, timeout=15)
        elapsed = time.time() - start
        print(f"✅ Webhook test: {response.status_code} in {elapsed:.2f}s")
        print(f"Response: {response.text}")
        return True
    except requests.Timeout:
        print(f"❌ Webhook timed out after 15 seconds")
        return False
    except Exception as e:
        print(f"❌ Webhook failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing webhook endpoints...")
    
    if not test_simple_health():
        exit(1)
    
    print("\nTesting webhook...")
    test_webhook_minimal()