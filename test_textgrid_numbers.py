#!/usr/bin/env python3
"""
Test inbound webhook with actual TextGrid numbers
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def test_textgrid_number(textgrid_number, unique_suffix):
    """Test inbound webhook with a specific TextGrid number"""
    
    print(f"\n📱 Testing with TextGrid number: {textgrid_number}")
    
    # Create unique message to avoid duplicate detection
    unique_message = f"Testing TextGrid integration {unique_suffix}. I'm interested in selling."
    
    # Create test payload from real sender to TextGrid number
    payload = {
        "From": "+16128072000",  # Test sender that we know works
        "To": textgrid_number,
        "Body": unique_message,
        "MessageSid": f"test-{textgrid_number.replace('+', '').replace(' ', '')}-{int(time.time())}-{unique_suffix}"
    }
    
    print(f"📥 Payload: From={payload['From']} To={payload['To']}")
    print(f"📝 Message: {payload['Body']}")
    
    try:
        result = process_inbound_sms(payload)
        print(f"✅ Result: {result}")
        
        if result.get('status') == 'duplicate':
            print("  ⚠️ Duplicate message detected")
        elif result.get('status') == 'success':
            print("  🎉 Successfully processed!")
            if 'conversation_id' in result:
                print(f"  📋 Conversation ID: {result['conversation_id']}")
            if 'lead_id' in result:
                print(f"  � Lead ID: {result['lead_id']}")
        else:
            print(f"  ❓ Unexpected status: {result}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_textgrid_numbers()