#!/usr/bin/env python3
"""
Simple webhook test to diagnose authentication issue
"""
import requests
import os
from dotenv import load_dotenv
load_dotenv()

def test_auth_bypass():
    """Test if authentication is actually disabled"""
    
    print("🔍 DIAGNOSING WEBHOOK AUTHENTICATION")
    print("=" * 60)
    
    webhook_url = "https://rei-sms-engine.onrender.com/inbound"
    
    # Test with no authentication (should work if WEBHOOK_TOKEN not set in Render)
    print("Test 1: No authentication at all")
    print("-" * 40)
    
    headers = {
        "Content-Type": "application/json"
    }
    
    test_payload = {
        "From": "+15551234567",
        "To": "+13235589900", 
        "Body": "Test with no auth",
        "MessageSid": f"TEST-NO-AUTH-{int(__import__('time').time())}"
    }
    
    try:
        response = requests.post(webhook_url, json=test_payload, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            print("🎉 SUCCESS! Authentication is disabled in production")
            print("💡 This means TextGrid doesn't need authentication")
        elif response.status_code == 401:
            print("🔒 Authentication is required in production")
        else:
            print(f"❓ Unexpected response: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        print("💡 Service might be sleeping (free tier) or down")

def test_webhook_formats():
    """Test different webhook payload formats TextGrid might use"""
    
    print("\n🧪 TESTING DIFFERENT PAYLOAD FORMATS")
    print("=" * 60)
    
    webhook_url = "https://rei-sms-engine.onrender.com/inbound"
    
    # Test form-encoded data (common with Twilio/TextGrid)
    print("Test 2: Form-encoded payload (like Twilio)")
    print("-" * 40)
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    form_data = {
        "From": "+15551234567",
        "To": "+13235589900",
        "Body": "Test form encoded",
        "MessageSid": f"TEST-FORM-{int(__import__('time').time())}"
    }
    
    try:
        response = requests.post(webhook_url, data=form_data, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            print("🎉 Form-encoded format works!")
        else:
            print(f"❌ Form-encoded failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def print_troubleshooting_steps():
    """Print troubleshooting steps"""
    
    print("\n🔧 TROUBLESHOOTING STEPS")
    print("=" * 60)
    
    print("1. 🔍 Check Render Environment Variables:")
    print("   - Go to https://dashboard.render.com")
    print("   - Open rei-sms-engine service")
    print("   - Check Environment tab")
    print("   - Verify WEBHOOK_TOKEN and CRON_TOKEN are set")
    print()
    
    print("2. 📋 Check Render Logs:")
    print("   - Go to Logs tab in Render dashboard")
    print("   - Look for authentication errors")
    print("   - Check if requests are reaching the service")
    print()
    
    print("3. 🚀 Wake Up Service (if sleeping):")
    print("   - Free tier services sleep after inactivity")
    print("   - Visit: https://rei-sms-engine.onrender.com/health")
    print("   - Wait 30-60 seconds for service to wake up")
    print()
    
    print("4. 📱 TextGrid Configuration:")
    print("   - Login to TextGrid dashboard")
    print("   - Find webhook/callback URL settings")
    print("   - Set URL to: https://rei-sms-engine.onrender.com/inbound")
    print("   - No authentication needed if our tests show auth is disabled")

if __name__ == "__main__":
    test_auth_bypass()
    test_webhook_formats()
    print_troubleshooting_steps()