#!/usr/bin/env python3
"""
Test TextGrid by clearing the memory cache first
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound, IDEM

def test_clear_cache():
    """Test TextGrid number by clearing the memory cache first"""
    
    print("🧪 Testing TextGrid after clearing memory cache...")
    
    # Clear the in-memory cache
    print("🗑️ Clearing IDEM memory cache...")
    IDEM._mem.clear()
    print(f"   Cache size after clear: {len(IDEM._mem)}")
    
    # Test actual TextGrid number
    textgrid_number = "+13235589900"
    
    # Create unique identifiers  
    random_suffix = random.randint(100000, 999999)
    precise_timestamp = int(time.time() * 1000)
    sender = f"+16661234567"  # Different test sender
    
    payload = {
        "From": sender,
        "To": textgrid_number,
        "Body": f"Fresh cache test {random_suffix} - want to sell property",
        "MessageSid": f"FRESH-CACHE-{precise_timestamp}-{random_suffix}"
    }
    
    print(f"📱 TextGrid: {textgrid_number}")
    print(f"📞 From: {sender}")
    print(f"💬 Message: {payload['Body']}")
    print(f"🆔 MessageSid: {payload['MessageSid']}")
    
    # Check if MessageSid is seen before processing
    is_seen_before = IDEM.seen(payload['MessageSid'])
    print(f"🔍 MessageSid seen before processing: {is_seen_before}")
    
    # If it was seen, clear and try again
    if is_seen_before:
        print("⚠️ MessageSid was already seen, clearing cache again...")
        IDEM._mem.clear()
        
        # Create completely new payload
        random_suffix = random.randint(100000, 999999)
        precise_timestamp = int(time.time() * 1000)
        payload = {
            "From": sender,
            "To": textgrid_number,  
            "Body": f"Second attempt {random_suffix} - selling property",
            "MessageSid": f"SECOND-ATTEMPT-{precise_timestamp}-{random_suffix}"
        }
        print(f"🔄 New MessageSid: {payload['MessageSid']}")
    
    try:
        result = handle_inbound(payload)
        print(f"\n✅ RESULT: {result}")
        
        if result.get('status') == 'duplicate':
            print("  ❌ STILL DUPLICATE - investigating further...")
            print(f"  📊 Cache size after processing: {len(IDEM._mem)}")
        elif result.get('status') == 'success':
            print("  🎉 SUCCESS! TextGrid working after cache clear!")
            if 'conversation_id' in result:
                print(f"  📋 Conversation: {result['conversation_id']}")
            if 'lead_id' in result:
                print(f"  👤 Lead: {result['lead_id']}")
        else:
            print(f"  📊 Status: {result.get('status')}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_clear_cache()