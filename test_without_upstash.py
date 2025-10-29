#!/usr/bin/env python3
"""
Test TextGrid by temporarily disabling Upstash to avoid duplicate detection
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

# Temporarily disable Upstash for this test
original_upstash_url = os.environ.get("UPSTASH_REDIS_REST_URL")
if original_upstash_url:
    del os.environ["UPSTASH_REDIS_REST_URL"]

from sms.inbound_webhook import handle_inbound

def test_without_upstash():
    """Test TextGrid number without Upstash duplicate detection"""
    
    print("🧪 Testing TextGrid without Upstash duplicate detection...")
    
    # Test actual TextGrid number
    textgrid_number = "+13235589900"
    
    # Create unique identifiers
    random_suffix = random.randint(100000, 999999)
    precise_timestamp = int(time.time() * 1000)
    sender = f"+15551234567"  # Test sender
    
    payload = {
        "From": sender,
        "To": textgrid_number,
        "Body": f"Testing without Upstash {random_suffix} - interested in selling",
        "MessageSid": f"NO-UPSTASH-{precise_timestamp}-{random_suffix}"
    }
    
    print(f"📱 TextGrid: {textgrid_number}")
    print(f"📞 From: {sender}")
    print(f"💬 Message: {payload['Body']}")
    print(f"🆔 MessageSid: {payload['MessageSid']}")
    
    try:
        result = handle_inbound(payload)
        print(f"\n✅ RESULT: {result}")
        
        if result.get('status') == 'duplicate':
            print("  ⚠️ Still duplicate (using local memory cache)")
        elif result.get('status') == 'success':
            print("  🎉 SUCCESS! TextGrid working without Upstash!")
            if 'conversation_id' in result:
                print(f"  📋 Conversation: {result['conversation_id']}")
            if 'lead_id' in result:
                print(f"  👤 Lead: {result['lead_id']}")
        elif result.get('status') == 'error':
            print(f"  ❌ Error: {result.get('error', 'Unknown error')}")
        else:
            print(f"  📊 Status: {result.get('status')}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Restore Upstash URL
        if original_upstash_url:
            os.environ["UPSTASH_REDIS_REST_URL"] = original_upstash_url

if __name__ == "__main__":
    test_without_upstash()