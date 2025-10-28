#!/usr/bin/env python3
"""
Test script to simulate inbound message from specific number
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from sms.inbound_webhook import handle_inbound

def test_message_from_number():
    """Test inbound message from 6128072000"""
    
    print("🧪 Testing inbound message from 6128072000...")
    
    # Format the phone number properly
    test_phone = "+16128072000"
    
    # Sample inbound payload from the specific number
    test_payload = {
        "From": test_phone,
        "To": "+15551239999", 
        "Body": "Yes, I'm interested in selling my property. Can you send me more information?",
        # No MessageSid to avoid idempotency issues
    }
    
    print(f"📥 Test payload: {test_payload}")
    
    try:
        result = handle_inbound(test_payload)
        print(f"✅ Handler result: {result}")
        
        if result.get("status") == "ok":
            print(f"🎉 Message successfully processed!")
            print(f"📊 Stage: {result.get('stage')}")
            print(f"🎯 Intent: {result.get('intent')}")
            print(f"🚀 Promoted to Lead: {result.get('promoted')}")
        elif result.get("status") == "duplicate":
            print(f"⚠️ Message was flagged as duplicate")
        else:
            print(f"⚠️ Unexpected result: {result}")
        
    except Exception as e:
        print(f"❌ Handler failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_message_from_number()