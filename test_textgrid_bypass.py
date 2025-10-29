#!/usr/bin/env python3
"""
Test TextGrid numbers without duplicate detection
"""
import os
import sys
import time
import uuid
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound

def test_textgrid_bypass_duplicates():
    """Test TextGrid numbers with completely unique messages to bypass duplicates"""
    
    print("🧪 Testing TextGrid numbers - bypassing duplicate detection...")
    
    textgrid_numbers = [
        "+13235589900",
        "+13235589881", 
        "+13235538059"
    ]
    
    for i, textgrid_number in enumerate(textgrid_numbers, 1):
        print(f"\n📱 Testing TextGrid number: {textgrid_number}")
        
        # Generate completely unique identifiers
        unique_id = str(uuid.uuid4())[:8]
        timestamp = int(time.time() * 1000)  # More precise timestamp
        sender = f"+1{555000000 + i}"  # Unique sender
        
        payload = {
            "From": sender,
            "To": textgrid_number,
            "Body": f"Unique test message {unique_id} - interested in selling {timestamp}",
            "MessageSid": f"UNIQUE-{unique_id}-{timestamp}-{i}"
        }
        
        print(f"📥 From: {payload['From']} To: {payload['To']}")
        print(f"📝 Message: {payload['Body']}")
        print(f"🆔 MessageSid: {payload['MessageSid']}")
        
        try:
            result = handle_inbound(payload)
            print(f"✅ Result: {result}")
            
            if result.get('status') == 'duplicate':
                print("  ⚠️ Still duplicate - investigating cache key")
            elif result.get('status') == 'success':
                print("  🎉 Successfully processed!")
                if 'conversation_id' in result:
                    print(f"  📋 Conversation ID: {result['conversation_id']}")
                if 'lead_id' in result:
                    print(f"  👤 Lead ID: {result['lead_id']}")
            else:
                print(f"  📊 Status: {result.get('status', 'unknown')}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        # Longer delay between tests
        time.sleep(2)

if __name__ == "__main__":
    test_textgrid_bypass_duplicates()