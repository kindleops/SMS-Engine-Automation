#!/usr/bin/env python3
"""
Test inbound webhook with actual TextGrid numbers
"""
import os
import sys
import time
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound

def test_textgrid_number(textgrid_number, unique_suffix):
    """Test inbound webhook with a specific TextGrid number"""
    
    print(f"\n📱 Testing with TextGrid number: {textgrid_number}")
    
    # Create unique message to avoid duplicate detection
    unique_message = f"Testing TextGrid integration {unique_suffix}. I'm interested in selling."
    
    # Create test payload from unique sender to TextGrid number
    test_sender = f"+1555000{unique_suffix.replace('test', '')}"  # Use unique sender numbers
    payload = {
        "From": test_sender,
        "To": textgrid_number,
        "Body": unique_message,
        "MessageSid": f"test-{textgrid_number.replace('+', '').replace(' ', '')}-{int(time.time())}-{unique_suffix}"
    }
    
    print(f"📥 Payload: From={payload['From']} To={payload['To']}")
    print(f"📝 Message: {payload['Body']}")
    
    try:
        result = handle_inbound(payload)
        print(f"✅ Result: {result}")
        
        if result.get('status') == 'duplicate':
            print("  ⚠️ Duplicate message detected")
        elif result.get('status') == 'success':
            print("  🎉 Successfully processed!")
            if 'conversation_id' in result:
                print(f"  📋 Conversation ID: {result['conversation_id']}")
            if 'lead_id' in result:
                print(f"  👤 Lead ID: {result['lead_id']}")
        else:
            print(f"  ❓ Unexpected status: {result}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def test_textgrid_numbers():
    """Test inbound webhook with actual TextGrid numbers"""
    
    print("🧪 Testing inbound webhook with actual TextGrid numbers...")
    
    # First 3 TextGrid numbers from your list
    textgrid_numbers = [
        "+13235589900",
        "+13235589881", 
        "+13235538059"
    ]
    
    for i, number in enumerate(textgrid_numbers, 1):
        test_textgrid_number(number, f"test{i}")
        
        # Small delay between tests
        time.sleep(1)


if __name__ == "__main__":
    test_textgrid_numbers()