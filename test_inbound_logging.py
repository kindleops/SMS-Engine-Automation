#!/usr/bin/env python3
"""
Test script to debug inbound message logging issues
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from sms.inbound_webhook import handle_inbound

def test_inbound_logging():
    """Test the inbound logging with a sample message"""
    
    print("🧪 Testing inbound message logging...")
    
    # Sample inbound payload WITHOUT MessageSid to bypass idempotency
    test_payload = {
        "From": "+15551234567",
        "To": "+15551239999", 
        "Body": "Yes, I'm interested in selling my house"
        # No MessageSid - should bypass idempotency check
    }
    
    print(f"📥 Test payload: {test_payload}")
    
    try:
        result = handle_inbound(test_payload)
        print(f"✅ Handler result: {result}")
        
    except Exception as e:
        print(f"❌ Handler failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_inbound_logging()