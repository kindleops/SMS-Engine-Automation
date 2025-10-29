#!/usr/bin/env python3
"""
Test all known TextGrid numbers systematically
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound

def get_all_textgrid_numbers():
    """Return all exact TextGrid numbers from your production list"""
    return [
        "+13235589900", "+13235589881", "+13235538059", "+13235405969",
        "+16562219706", "+18139558539", "+18139558233", "+18139558399",
        "+18139558255", "+19804589954", "+19804589924", "+19804589911",
        "+19804589889", "+17867851070", "+17866059995", "+17866059994",
        "+17866059384", "+17866059149", "+16127469639", "+17633358822",
        "+16127469588", "+17633358770", "+16127469566", "+19045124141",
        "+19045124118", "+19045124117", "+19045124091", "+19048348997",
        "+18329063695", "+18329063669", "+18329063655", "+18329063577",
        "+18322633878", "+13234104544", "+17042405818"
    ]

def test_number(phone_number, index, total):
    """Test a specific phone number"""
    print(f"\n📱 Testing {index}/{total}: {phone_number}")
    
    # Create unique identifiers to avoid duplicates
    random_suffix = random.randint(100000, 999999)
    timestamp = int(time.time() * 1000000)  # Microsecond precision
    sender = f"+1{random.randint(5550000000, 5559999999)}"
    
    payload = {
        "From": sender,
        "To": phone_number,
        "Body": f"Test {random_suffix} - interested in selling my property at {timestamp}",
        "MessageSid": f"SYSTEMATIC-TEST-{phone_number.replace('+', '')}-{timestamp}-{random_suffix}"
    }
    
    print(f"   📞 From: {sender}")
    print(f"   💬 Message: Test {random_suffix} - interested in selling...")
    
    try:
        result = handle_inbound(payload)
        
        if result.get('status') == 'ok':
            print(f"   ✅ SUCCESS")
            if result.get('promoted'):
                print(f"      🚀 Lead promoted!")
            else:
                print(f"      📋 Conversation logged")
            return 'success'
            
        elif result.get('status') == 'duplicate':
            print(f"   ⚠️ Duplicate detected")
            return 'duplicate'
            
        elif result.get('status') == 'error':
            error_msg = result.get('error', 'Unknown error')
            print(f"   ❌ ERROR: {error_msg}")
            return 'error'
            
        else:
            print(f"   ❓ Unknown status: {result.get('status')}")
            return 'unknown'
            
    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        return 'exception'

def main():
    """Test all exact production TextGrid numbers systematically"""
    print("🧪 Testing all 35 production TextGrid numbers systematically...\n")
    
    numbers = get_all_textgrid_numbers()
    total = len(numbers)
    
    print(f"📊 Testing {total} exact production TextGrid numbers...")
    print("="*60)
    
    results = {
        'success': [],
        'duplicate': [],
        'error': [],
        'exception': [],
        'unknown': []
    }
    
    for i, phone_number in enumerate(numbers, 1):
        result = test_number(phone_number, i, total)
        results[result].append(phone_number)
        
        # Small delay between tests to avoid rate limiting
        time.sleep(0.5)
    
    print("\n" + "="*60)
    print("📊 FINAL RESULTS SUMMARY:")
    print("="*60)
    
    print(f"✅ WORKING ({len(results['success'])} numbers):")
    for phone in results['success']:
        print(f"   {phone}")
    
    if results['duplicate']:
        print(f"\n⚠️ DUPLICATES ({len(results['duplicate'])} numbers):")
        for phone in results['duplicate']:
            print(f"   {phone}")
    
    if results['error']:
        print(f"\n❌ ERRORS ({len(results['error'])} numbers):")
        for phone in results['error']:
            print(f"   {phone}")
    
    if results['exception']:
        print(f"\n💥 EXCEPTIONS ({len(results['exception'])} numbers):")
        for phone in results['exception']:
            print(f"   {phone}")
    
    if results['unknown']:
        print(f"\n❓ UNKNOWN STATUS ({len(results['unknown'])} numbers):")
        for phone in results['unknown']:
            print(f"   {phone}")
    
    # Calculate working percentage
    working_count = len(results['success']) + len(results['duplicate'])  # Duplicates likely work
    issue_count = len(results['error']) + len(results['exception']) + len(results['unknown'])
    
    percentage = (working_count / total) * 100 if total > 0 else 0
    
    print(f"\n📈 SUMMARY:")
    print(f"   • {working_count}/{total} numbers working ({percentage:.1f}%)")
    print(f"   • {issue_count} numbers with issues")
    
    if issue_count > 0:
        print(f"\n🔧 NEXT STEPS:")
        print(f"   • Investigate error/exception numbers")
        print(f"   • Check TextGrid configuration for failing numbers")
        print(f"   • Verify webhook routing for problematic numbers")

if __name__ == "__main__":
    main()