#!/usr/bin/env python3
"""
Fixed TextGrid webhook test with correct authentication
"""
import requests
import os
from dotenv import load_dotenv
load_dotenv()

def test_webhook_with_correct_auth():
    """Test webhook with the correct authentication format"""
    
    print("🧪 TESTING WEBHOOK WITH CORRECT AUTHENTICATION")
    print("=" * 60)
    
    webhook_url = "https://rei-sms-engine.onrender.com/inbound"
    cron_token = os.getenv("CRON_TOKEN")
    
    # Test 1: Using x-webhook-token header (preferred)
    print("Test 1: Using x-webhook-token header")
    print("-" * 40)
    
    headers = {
        "Content-Type": "application/json",
        "x-webhook-token": cron_token  # Correct header format
    }
    
    test_payload = {
        "From": "+15551234567",
        "To": "+13235589900",
        "Body": "Test with correct header auth",
        "MessageSid": f"TEST-HEADER-{int(__import__('time').time())}"
    }
    
    try:
        response = requests.post(webhook_url, json=test_payload, headers=headers, timeout=30)
        print(f"✅ Status: {response.status_code}")
        print(f"✅ Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            print("🎉 SUCCESS! Header authentication working!")
        else:
            print(f"❌ Failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "-" * 60)
    
    # Test 2: Using query parameter
    print("Test 2: Using query parameter")
    print("-" * 40)
    
    webhook_url_with_token = f"{webhook_url}?token={cron_token}"
    
    headers_minimal = {
        "Content-Type": "application/json"
    }
    
    test_payload_2 = {
        "From": "+15551234568", 
        "To": "+13235589900",
        "Body": "Test with query param auth",
        "MessageSid": f"TEST-QUERY-{int(__import__('time').time())}"
    }
    
    try:
        response = requests.post(webhook_url_with_token, json=test_payload_2, headers=headers_minimal, timeout=30)
        print(f"✅ Status: {response.status_code}")
        print(f"✅ Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            print("🎉 SUCCESS! Query parameter authentication working!")
        else:
            print(f"❌ Failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def print_textgrid_config_instructions():
    """Print corrected TextGrid configuration instructions"""
    
    print("\n🔧 CORRECTED TEXTGRID CONFIGURATION")
    print("=" * 60)
    
    cron_token = os.getenv("CRON_TOKEN")
    
    print("📱 Option 1: Configure TextGrid with Header Authentication")
    print("-" * 50)
    print("Webhook URL: https://rei-sms-engine.onrender.com/inbound")
    print("Authentication Method: Custom Header")
    print(f"Header Name: x-webhook-token")
    print(f"Header Value: {cron_token}")
    print()
    
    print("📱 Option 2: Configure TextGrid with Query Parameter")
    print("-" * 50)
    print(f"Webhook URL: https://rei-sms-engine.onrender.com/inbound?token={cron_token}")
    print("Authentication Method: None (token in URL)")
    print()
    
    print("🧪 Test Commands (both should work now):")
    print("-" * 50)
    print("# Test with header:")
    print(f"curl -X POST https://rei-sms-engine.onrender.com/inbound \\")
    print(f"  -H 'Content-Type: application/json' \\")
    print(f"  -H 'x-webhook-token: {cron_token}' \\")
    print(f"  -d '{{\"From\":\"+15551234567\",\"To\":\"+13235589900\",\"Body\":\"Test\",\"MessageSid\":\"TEST-123\"}}'")
    print()
    print("# Test with query param:")
    print(f"curl -X POST 'https://rei-sms-engine.onrender.com/inbound?token={cron_token}' \\")
    print(f"  -H 'Content-Type: application/json' \\")
    print(f"  -d '{{\"From\":\"+15551234567\",\"To\":\"+13235589900\",\"Body\":\"Test\",\"MessageSid\":\"TEST-123\"}}'")

if __name__ == "__main__":
    test_webhook_with_correct_auth()
    print_textgrid_config_instructions()