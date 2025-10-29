#!/usr/bin/env python3
"""
Debug TextGrid webhook integration
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_webhook_config():
    """Check current webhook and TextGrid configuration"""
    
    print("🔍 Checking TextGrid and Webhook Configuration...")
    print("=" * 60)
    
    # Check TextGrid environment variables
    textgrid_api_key = os.getenv("TEXTGRID_API_KEY")
    textgrid_campaign_id = os.getenv("TEXTGRID_CAMPAIGN_ID") 
    textgrid_account_sid = os.getenv("TEXTGRID_ACCOUNT_SID")
    textgrid_auth_token = os.getenv("TEXTGRID_AUTH_TOKEN")
    
    print("📱 TextGrid Configuration:")
    print(f"   API Key: {'✅ Set' if textgrid_api_key else '❌ Missing'}")
    print(f"   Campaign ID: {textgrid_campaign_id or '❌ Missing'}")
    print(f"   Account SID: {textgrid_account_sid or '❌ Missing'}")
    print(f"   Auth Token: {'✅ Set' if textgrid_auth_token else '❌ Missing'}")
    
    # Check webhook authentication
    cron_token = os.getenv("CRON_TOKEN")
    print(f"\n🔐 Webhook Authentication:")
    print(f"   CRON_TOKEN: {'✅ Set' if cron_token else '❌ Missing'}")
    
    # Check if we're in production (Render) or local
    render_service = os.getenv("RENDER_SERVICE_NAME")
    render_external_url = os.getenv("RENDER_EXTERNAL_URL")
    
    print(f"\n🌐 Deployment Environment:")
    if render_service:
        print(f"   Environment: 🚀 Production (Render)")
        print(f"   Service: {render_service}")
        print(f"   URL: {render_external_url}")
        
        # Expected webhook URL
        if render_external_url:
            webhook_url = f"{render_external_url}/inbound"
            print(f"   Expected Webhook URL: {webhook_url}")
        else:
            print(f"   ⚠️ RENDER_EXTERNAL_URL not set")
    else:
        print(f"   Environment: 💻 Local Development")
        print(f"   ⚠️ TextGrid can't reach local webhooks")
        print(f"   💡 Need ngrok or deployed webhook for real SMS testing")
    
    print(f"\n🔧 Next Steps to Debug:")
    print(f"1. ✅ Verify TextGrid webhook URL is set to your Render app")
    print(f"2. ✅ Check TextGrid dashboard for webhook configuration") 
    print(f"3. ✅ Test webhook endpoint directly")
    print(f"4. ✅ Check webhook logs in Render dashboard")
    print(f"5. ✅ Verify authentication token")

def print_webhook_test_info():
    """Print information about testing webhooks"""
    
    print(f"\n🧪 How to Test Real SMS Integration:")
    print(f"=" * 60)
    
    render_external_url = os.getenv("RENDER_EXTERNAL_URL")
    if render_external_url:
        webhook_url = f"{render_external_url}/inbound"
        print(f"1. 📱 TextGrid Webhook URL should be:")
        print(f"   {webhook_url}")
        print(f"")
        print(f"2. 🔐 Authentication methods TextGrid might use:")
        print(f"   - Query parameter: ?token={os.getenv('CRON_TOKEN', 'YOUR_TOKEN')}")
        print(f"   - Header: Authorization: Bearer {os.getenv('CRON_TOKEN', 'YOUR_TOKEN')}")
        print(f"")
        print(f"3. 🧪 Test webhook directly with curl:")
        print(f"   curl -X POST {webhook_url} \\")
        print(f"        -H 'Content-Type: application/json' \\")
        print(f"        -H 'Authorization: Bearer {os.getenv('CRON_TOKEN', 'YOUR_TOKEN')}' \\")
        print(f"        -d '{{")
        print(f"          \"From\": \"+15551234567\",")
        print(f"          \"To\": \"+13235589900\",")
        print(f"          \"Body\": \"Test from curl\",")
        print(f"          \"MessageSid\": \"TEST-CURL-123\"")
        print(f"        }}'")
    else:
        print(f"❌ RENDER_EXTERNAL_URL not configured")
        print(f"💡 Set this in your Render environment variables")

if __name__ == "__main__":
    check_webhook_config()
    print_webhook_test_info()