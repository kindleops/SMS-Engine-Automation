#!/usr/bin/env python3
"""
Test TextGrid webhook payload and check what fields are being sent
"""
import requests
import json

# Test with different possible TextGrid field names
test_payloads = [
    {
        "name": "Standard Twilio-style",
        "payload": {
            "From": "+15551234567",
            "To": "+17042405818", 
            "Body": "Test message with MessageSid",
            "MessageSid": "TEST-MESSAGESID-12345"
        }
    },
    {
        "name": "TextGrid specific",
        "payload": {
            "From": "+15551234567",
            "To": "+17042405818",
            "Body": "Test message with TextGridId", 
            "TextGridId": "TEST-TEXTGRIDID-12345"
        }
    },
    {
        "name": "Alternative ID fields",
        "payload": {
            "From": "+15551234567",
            "To": "+17042405818",
            "Body": "Test with alternative ID fields",
            "Id": "TEST-ID-12345",
            "MessageId": "TEST-MESSAGEID-12345",
            "SmsId": "TEST-SMSID-12345"
        }
    },
    {
        "name": "Common webhook format",
        "payload": {
            "from": "+15551234567",
            "to": "+17042405818",
            "text": "Test with common webhook format",
            "id": "TEST-COMMON-ID-12345",
            "message_id": "TEST-MSG-ID-12345"
        }
    }
]

webhook_url = "https://rei-sms-engine.onrender.com/inbound?token=bb2f2c97f36a4b9fa3ec2f8d6a5d7c52f83b4c0de719ebc44d40a90aa3d0c53a"

print("🧪 TESTING TEXTGRID WEBHOOK FIELD MAPPING")
print("=" * 60)

for test in test_payloads:
    print(f"\n📨 Testing: {test['name']}")
    print(f"Payload: {json.dumps(test['payload'], indent=2)}")
    
    try:
        response = requests.post(
            webhook_url,
            json=test['payload'],
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        print(f"✅ Status: {response.status_code}")
        if response.status_code == 200:
            print(f"✅ Response: {response.text}")
        else:
            print(f"❌ Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
    
    print("-" * 40)

print("\n💡 FIELD MAPPING RECOMMENDATIONS:")
print("Based on test results, check what TextGrid actually sends")
print("Common TextGrid field patterns:")
print("- MessageSid (Twilio-style)")  
print("- TextGridId (TextGrid-specific)")
print("- Id / MessageId (generic)")
print("- SmsId (SMS-specific)")
print("\nUpdate the msg_id extraction in inbound_webhook.py if needed:")
print('msg_id = payload.get("MessageSid") or payload.get("TextGridId") or payload.get("Id") or payload.get("MessageId")')