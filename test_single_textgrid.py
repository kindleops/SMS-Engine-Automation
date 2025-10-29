#!/usr/bin/env python3
"""
Test one specific TextGrid number with extremely unique ID
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound

def test_single_textgrid():
    """Test one TextGrid number with maximum uniqueness"""
    
    print("🧪 Testing single TextGrid number with maximum uniqueness...")
    
    # Use first TextGrid number
    textgrid_number = "+13235589900"
    
    # Create ultra-unique identifiers
    random_suffix = random.randint(100000, 999999)
    precise_timestamp = int(time.time() * 1000000)  # Microsecond precision
    sender = f"+19998887777"  # Completely different sender
    
    # Ultra-unique message
    message = f"COMPLETELY UNIQUE MESSAGE {random_suffix} AT {precise_timestamp} - PLEASE RESPOND"
    
    payload = {
        "From": sender,
        "To": textgrid_number,
        "Body": message,
        "MessageSid": f"ULTRA-UNIQUE-{precise_timestamp}-{random_suffix}-TEXTGRID-TEST"
    }
    
    print(f"📱 Testing TextGrid: {textgrid_number}")
    print(f"📞 From: {sender}")
    print(f"💬 Message: {message}")
    print(f"🆔 MessageSid: {payload['MessageSid']}")
    
    try:
        result = handle_inbound(payload)
        print(f"\n✅ RESULT: {result}")
        
        if result.get('status') == 'duplicate':
            print("  ❌ STILL SHOWING AS DUPLICATE!")
            print("  🔍 This suggests an issue with the cache or duplicate detection logic")
        elif result.get('status') == 'success':
            print("  🎉 SUCCESS! TextGrid number is working!")
            if 'conversation_id' in result:
                print(f"  📋 Conversation created: {result['conversation_id']}")
            if 'lead_id' in result:
                print(f"  👤 Lead promoted: {result['lead_id']}")
        else:
            print(f"  📊 Status: {result.get('status')}")
            print(f"  📝 Full result: {result}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_textgrid()