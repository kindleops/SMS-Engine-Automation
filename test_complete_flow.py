#!/usr/bin/env python3
"""
Test the complete inbound webhook flow with real TextGrid ID format.
"""

import os
import sys
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_complete_inbound_flow():
    """Test complete inbound message processing with TextGrid ID."""
    print("🧪 Testing complete inbound webhook flow...")
    
    from sms.inbound_webhook import handle_inbound
    
    # Create realistic TextGrid payload
    test_payload = {
        "From": "+15551234567",
        "To": "+15559876543", 
        "Body": "Hi, I'm interested in your property on Main Street. Can you tell me more?",
        "TextGridId": "SMIOv7MB7dIQDBtIjPsAinpHA==",  # Real TextGrid ID format
        "MessageSid": None  # TextGrid doesn't use MessageSid
    }
    
    print(f"📤 Sending test payload:")
    print(json.dumps(test_payload, indent=2))
    
    try:
        result = handle_inbound(test_payload)
        print(f"\n✅ Webhook processing completed successfully!")
        print(f"📊 Result: {json.dumps(result, indent=2)}")
        
        # Verify expected fields in result
        if "status" in result and result["status"] == "ok":
            print("✅ Status: OK")
        if "stage" in result:
            print(f"✅ Stage detected: {result['stage']}")
        if "intent" in result:
            print(f"✅ Intent detected: {result['intent']}")
            
        return True
        
    except Exception as e:
        print(f"❌ Webhook processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_complete_inbound_flow()
    if success:
        print("\n🎉 All tests passed! Webhook is ready for production.")
    else:
        print("\n❌ Tests failed. Check the errors above.")