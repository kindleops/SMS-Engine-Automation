#!/usr/bin/env python3
"""
Check all numbers in the Numbers table and test them systematically
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.airtable_client import get_numbers
from sms.inbound_webhook import handle_inbound

def get_all_numbers():
    """Get all numbers from the Numbers table"""
    print("📋 Fetching all numbers from Airtable Numbers table...")
    
    try:
        numbers_table = get_numbers()
        
        # Get all records
        records = numbers_table.all()
        
        numbers = []
        for record in records:
            fields = record.get('fields', {})
            phone_field = os.getenv("NUMBERS_PHONE_FIELD", "Number")
            status_field = os.getenv("NUMBERS_STATUS_FIELD", "Status")
            market_field = os.getenv("NUMBERS_MARKET_FIELD", "Market")
            active_field = os.getenv("NUMBERS_ACTIVE_FIELD", "Active")
            
            phone = fields.get(phone_field)
            status = fields.get(status_field)
            market = fields.get(market_field)
            active = fields.get(active_field)
            
            if phone:
                numbers.append({
                    'phone': phone,
                    'status': status,
                    'market': market,
                    'active': active,
                    'record_id': record['id']
                })
        
        print(f"📱 Found {len(numbers)} numbers in the table")
        return numbers
        
    except Exception as e:
        print(f"❌ Error fetching numbers: {e}")
        import traceback
        traceback.print_exc()
        return []

def test_number(phone_number, index):
    """Test a specific phone number"""
    print(f"\n📱 Testing number {index}: {phone_number}")
    
    # Create unique identifiers
    random_suffix = random.randint(100000, 999999)
    timestamp = int(time.time() * 1000)
    sender = f"+1555{random.randint(1000000, 9999999)}"
    
    payload = {
        "From": sender,
        "To": phone_number,
        "Body": f"Test message {random_suffix} - interested in selling property",
        "MessageSid": f"TEST-{phone_number.replace('+', '').replace(' ', '')}-{timestamp}-{random_suffix}"
    }
    
    print(f"   📞 From: {sender}")
    print(f"   💬 Message: {payload['Body']}")
    
    try:
        result = handle_inbound(payload)
        
        if result.get('status') == 'ok':
            print(f"   ✅ SUCCESS")
            if result.get('promoted'):
                print(f"      🚀 Lead promoted!")
            if 'conversation_id' in result:
                print(f"      📋 Conversation: {result.get('conversation_id')}")
        elif result.get('status') == 'duplicate':
            print(f"   ⚠️ Duplicate (expected for repeated tests)")
        elif result.get('status') == 'error':
            print(f"   ❌ ERROR: {result.get('error', 'Unknown error')}")
        else:
            print(f"   ❓ Status: {result.get('status')}")
            
        return result
        
    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'exception', 'error': str(e)}

def main():
    """Test all numbers from the Numbers table"""
    print("🧪 Testing all numbers from Numbers table...\n")
    
    numbers = get_all_numbers()
    
    if not numbers:
        print("❌ No numbers found to test")
        return
    
    print(f"\n📊 Numbers Summary:")
    for i, num_info in enumerate(numbers, 1):
        print(f"   {i}. {num_info['phone']} (Status: {num_info['status']}, Market: {num_info['market']}, Active: {num_info['active']})")
    
    print("\n" + "="*60)
    print("🧪 Starting systematic testing...")
    
    results = []
    for i, num_info in enumerate(numbers, 1):
        result = test_number(num_info['phone'], i)
        results.append({
            'phone': num_info['phone'],
            'result': result,
            'info': num_info
        })
        
        # Small delay between tests
        time.sleep(1)
    
    print("\n" + "="*60)
    print("📊 FINAL RESULTS SUMMARY:")
    
    success_count = 0
    error_count = 0
    
    for r in results:
        status = r['result'].get('status', 'unknown')
        phone = r['phone']
        
        if status == 'ok':
            print(f"   ✅ {phone} - Working")
            success_count += 1
        elif status == 'duplicate':
            print(f"   ⚠️ {phone} - Duplicate (likely working)")
            success_count += 1
        else:
            print(f"   ❌ {phone} - {status}")
            if 'error' in r['result']:
                print(f"      Error: {r['result']['error']}")
            error_count += 1
    
    print(f"\n📈 Summary: {success_count} working, {error_count} errors out of {len(results)} total")

if __name__ == "__main__":
    main()