#!/usr/bin/env python3
"""
Test the enhanced TextGrid ID extraction after deployment
"""
import requests
import time

webhook_url = "https://rei-sms-engine.onrender.com/inbound?token=bb2f2c97f36a4b9fa3ec2f8d6a5d7c52f83b4c0de719ebc44d40a90aa3d0c53a"

# Test payload that should now populate the TextGrid ID field correctly
test_payload = {
    "From": "+15551234567",
    "To": "+17042405818",
    "Body": "Testing enhanced TextGrid ID extraction",
    "MessageSid": "TG-ENHANCED-TEST-12345"  # This should now be captured
}

print("🧪 TESTING ENHANCED TEXTGRID ID EXTRACTION")
print("=" * 60)
print(f"Webhook URL: {webhook_url}")
print(f"Test payload: {test_payload}")
print()

# Wait a moment for deployment to complete
print("⏳ Waiting for deployment to complete...")
time.sleep(10)

try:
    print("📤 Sending test webhook...")
    response = requests.post(
        webhook_url,
        json=test_payload,
        headers={'Content-Type': 'application/json'},
        timeout=60  # Longer timeout for potential cold start
    )
    
    print(f"✅ Status Code: {response.status_code}")
    print(f"✅ Response: {response.text}")
    
    if response.status_code == 200:
        print("\n🎉 SUCCESS! Webhook processed successfully")
        print("📋 Next steps:")
        print("1. Check your Airtable Conversations table")
        print("2. Look for a new record with:")
        print(f"   - From: +15551234567")
        print(f"   - To: +17042405818") 
        print(f"   - TextGrid ID: TG-ENHANCED-TEST-12345")
        print(f"   - Message: Testing enhanced TextGrid ID extraction")
        print("\n💡 If TextGrid ID field is now populated, the fix is working!")
    else:
        print(f"\n❌ Unexpected response: {response.status_code}")
        print("Check Render logs for details")
        
except requests.exceptions.Timeout:
    print("❌ Request timed out - webhook might be processing slowly")
    print("💡 Check Render logs and Airtable to see if record was created")
except Exception as e:
    print(f"❌ Request failed: {e}")

print("\n🔍 DEBUGGING TIPS:")
print("1. Check Render logs at: https://dashboard.render.com")
print("2. Look for the enhanced logging messages:")
print("   - '🔍 TextGrid ID found in field...'")
print("   - '⚠️ No TextGrid ID found. Available fields...'")
print("3. If TextGrid sends different field names, the logs will show them")
print("4. Configure TextGrid webhook and test with real SMS message")