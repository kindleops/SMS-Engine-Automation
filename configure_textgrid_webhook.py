#!/usr/bin/env python3
"""
TextGrid Webhook Configuration Guide and Test
"""
import requests
import os
import json
from dotenv import load_dotenv
load_dotenv()

def print_configuration_guide():
    """Print step-by-step TextGrid webhook configuration"""
    
    print("🔧 TEXTGRID WEBHOOK CONFIGURATION GUIDE")
    print("=" * 60)
    
    webhook_url = "https://rei-sms-engine.onrender.com/inbound"
    cron_token = os.getenv("CRON_TOKEN")
    
    print("📱 Step 1: TextGrid Dashboard Configuration")
    print("-" * 40)
    print("1. Log into your TextGrid dashboard")
    print("2. Go to Campaign Settings or Webhook Configuration")
    print("3. Set the Inbound Webhook URL to:")
    print(f"   {webhook_url}")
    print("4. Set authentication method (if required):")
    print(f"   - Query param: ?token={cron_token}")
    print(f"   - OR Header: Authorization: Bearer {cron_token}")
    print()
    
    print("🧪 Step 2: Test Your Deployed Webhook")
    print("-" * 40)
    print("Test the webhook is working by running this curl command:")
    print()
    print(f"curl -X POST {webhook_url} \\")
    print(f"  -H 'Content-Type: application/json' \\")
    print(f"  -H 'Authorization: Bearer {cron_token}' \\")
    print(f"  -d '{{")
    print(f"    \"From\": \"+15551234567\",")
    print(f"    \"To\": \"+13235589900\",")
    print(f"    \"Body\": \"Test webhook from curl\",")
    print(f"    \"MessageSid\": \"TEST-WEBHOOK-{int(__import__('time').time())}\"")
    print(f"  }}'")
    print()
    
    print("🔍 Step 3: Check Render Logs")
    print("-" * 40)
    print("1. Go to https://dashboard.render.com")
    print("2. Open your 'rei-sms-engine' service")
    print("3. Click on 'Logs' tab")
    print("4. Look for webhook requests when you:")
    print("   - Send the curl test above")
    print("   - Send real SMS to your TextGrid numbers")
    print()
    
    print("🚨 Step 4: Common Issues & Solutions")
    print("-" * 40)
    print("❌ Issue: No logs appear when sending real SMS")
    print("   💡 Solution: TextGrid webhook URL not configured")
    print()
    print("❌ Issue: 401/403 errors in logs")
    print("   💡 Solution: Fix authentication token")
    print()
    print("❌ Issue: 404 errors")
    print("   💡 Solution: Wrong webhook URL (check /inbound path)")
    print()
    print("❌ Issue: 500 errors")
    print("   💡 Solution: Check application logs for errors")

def test_deployed_webhook():
    """Test the deployed webhook endpoint"""
    
    print("\n🧪 TESTING DEPLOYED WEBHOOK")
    print("=" * 60)
    
    webhook_url = "https://rei-sms-engine.onrender.com/inbound"
    cron_token = os.getenv("CRON_TOKEN")
    
    print(f"Testing URL: {webhook_url}")
    
    # Test payload
    test_payload = {
        "From": "+15551234567",
        "To": "+13235589900", 
        "Body": "Test from webhook configuration script",
        "MessageSid": f"TEST-CONFIG-{int(__import__('time').time())}"
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cron_token}"
    }
    
    try:
        print("Sending test request...")
        response = requests.post(
            webhook_url,
            json=test_payload,
            headers=headers,
            timeout=30
        )
        
        print(f"✅ Status Code: {response.status_code}")
        print(f"✅ Response: {response.text}")
        
        if response.status_code == 200:
            print("🎉 Webhook is working! Check Conversations table for new record.")
        elif response.status_code == 401:
            print("❌ Authentication failed - check CRON_TOKEN")
        elif response.status_code == 404:
            print("❌ Endpoint not found - check URL")
        else:
            print(f"⚠️ Unexpected status code: {response.status_code}")
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out - webhook might be slow or down")
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed - check if service is deployed")
    except Exception as e:
        print(f"❌ Error: {e}")

def check_textgrid_api():
    """Check if we can access TextGrid API to see webhook config"""
    
    print("\n🔍 CHECKING TEXTGRID API ACCESS")
    print("=" * 60)
    
    api_key = os.getenv("TEXTGRID_API_KEY")
    account_sid = os.getenv("TEXTGRID_ACCOUNT_SID")
    
    if not api_key or not account_sid:
        print("❌ TextGrid credentials not found")
        return
    
    print("📱 TextGrid Configuration:")
    print(f"   Account SID: {account_sid}")
    print(f"   Campaign ID: {os.getenv('TEXTGRID_CAMPAIGN_ID')}")
    print()
    print("💡 To configure webhooks in TextGrid:")
    print("   1. Use TextGrid dashboard/portal")
    print("   2. Or contact TextGrid support")
    print("   3. Set webhook URL to: https://rei-sms-engine.onrender.com/inbound")

if __name__ == "__main__":
    print_configuration_guide()
    test_deployed_webhook()
    check_textgrid_api()