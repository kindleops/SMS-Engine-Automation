#!/usr/bin/env python3
"""Test enhanced conversation logging functionality."""

import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def test_enhanced_logging():
    """Test that enhanced conversation logging functionality works."""
    try:
        from sms.message_processor import MessageProcessor, get_prospect_total_counts
        from sms.inbound_webhook import enhance_conversation_payload
        
        print("✅ Successfully imported enhanced logging functions")
        
        # Test prospect count function
        test_phone = "5551234567"
        sent_count, reply_count = get_prospect_total_counts(test_phone)
        print(f"✅ get_prospect_total_counts for {test_phone}: sent={sent_count}, replies={reply_count}")
        
        # Test enhanced counts method
        enhanced_counts = MessageProcessor._get_enhanced_counts(test_phone, "OUTBOUND")
        print(f"✅ _get_enhanced_counts: {enhanced_counts}")
        
        # Test stage determination
        stage = MessageProcessor._determine_stage(test_phone, "OUTBOUND", {"ai_intent": "interested"})
        print(f"✅ _determine_stage: {stage}")
        
        # Test enhance conversation payload
        test_payload = {
            "conversation_id": "test_123",
            "phone": test_phone,
            "body": "Test message",
            "direction": "INBOUND"
        }
        enhanced = enhance_conversation_payload(test_payload)
        print(f"✅ enhance_conversation_payload worked: {len(enhanced)} fields")
        
        print("\n🎉 All enhanced conversation logging tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing enhanced logging: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_enhanced_logging()
    exit(0 if success else 1)