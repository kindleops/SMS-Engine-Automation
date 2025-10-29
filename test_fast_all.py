#!/usr/bin/env python3
"""
Fast batch test of all TextGrid numbers
"""
import os
import sys
import time
import random
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sms.inbound_webhook import handle_inbound

def fast_test_all_numbers():
    """Fast test of all 35 TextGrid numbers"""
    
    print("🚀 Fast testing all 35 TextGrid numbers...")
    
    numbers = [
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
    
    working = []
    failed = []
    
    print(f"Testing {len(numbers)} numbers...")
    
    for i, number in enumerate(numbers, 1):
        print(f"[{i:2d}/35] {number}", end=" -> ")
        
        try:
            # Quick test payload
            payload = {
                "From": f"+1555{random.randint(1000000, 9999999)}",
                "To": number,
                "Body": f"Fast test {i}",
                "MessageSid": f"FAST-{int(time.time())}-{i}"
            }
            
            result = handle_inbound(payload)
            
            if result.get('status') in ['ok', 'duplicate']:
                print("✅")
                working.append(number)
            else:
                print(f"❌ {result.get('status', 'unknown')}")
                failed.append(number)
                
        except Exception as e:
            print(f"❌ Error: {str(e)[:30]}...")
            failed.append(number)
        
        # Very short delay
        time.sleep(0.1)
    
    print(f"\n📊 RESULTS:")
    print(f"✅ Working: {len(working)}/35 ({len(working)/35*100:.1f}%)")
    print(f"❌ Failed: {len(failed)}/35")
    
    if failed:
        print(f"\n❌ Failed numbers:")
        for num in failed:
            print(f"   {num}")
    
    if len(working) >= 30:
        print("🎉 Most numbers working! TextGrid integration is solid.")
    
    return working, failed

if __name__ == "__main__":
    fast_test_all_numbers()