#!/usr/bin/env python3
"""
Test first 5 TextGrid numbers to verify functionality
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound

def test_first_5_numbers():
    """Test first 5 TextGrid numbers"""
    
    print("🧪 Testing first 5 TextGrid numbers...")
    
    # First 5 numbers from your list
    numbers = [
        "+13235589900", 
        "+13235589881", 
        "+13235538059", 
        "+13235405969",
        "+16562219706"
    ]
    
    results = {'success': 0, 'errors': 0}
    
    for i, number in enumerate(numbers, 1):
        print(f"\n📱 [{i}/5] Testing: {number}")
        
        # Generate unique test data
        random_id = random.randint(10000, 99999)
        timestamp = int(time.time() * 1000)
        sender = f"+1555{random.randint(1000000, 9999999)}"
        
        payload = {
            "From": sender,
            "To": number,
            "Body": f"Quick test {random_id} - interested in selling",
            "MessageSid": f"QUICK-TEST-{timestamp}-{random_id}"
        }
        
        try:
            result = handle_inbound(payload)
            
            if result.get('status') in ['ok', 'duplicate']:
                print(f"   ✅ Working! Status: {result.get('status')}")
                results['success'] += 1
            else:
                print(f"   ❌ Issue: {result}")
                results['errors'] += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results['errors'] += 1
        
        time.sleep(1)  # 1 second delay
    
    print(f"\n📊 Quick test results: {results['success']}/5 working, {results['errors']} errors")
    
    if results['success'] == 5:
        print("✅ All first 5 numbers working! Ready for full test.")
        return True
    else:
        print("⚠️ Some issues detected. Need to investigate before full test.")
        return False

if __name__ == "__main__":
    test_first_5_numbers()