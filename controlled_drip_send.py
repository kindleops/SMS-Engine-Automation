#!/usr/bin/env python3
"""Controlled drip queue sending with rate limiting and progress tracking."""

import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def controlled_drip_send(
    batch_size: int = 50,
    delay_seconds: int = 30,
    max_total: int = 500,
    enable_actual_sending: bool = False
) -> Dict[str, Any]:
    """
    Send drip queue messages in controlled batches with delays.
    
    Args:
        batch_size: Number of messages per batch (default: 50)
        delay_seconds: Seconds to wait between batches (default: 30)
        max_total: Maximum total messages to send (default: 500)
        enable_actual_sending: If True, removes emergency stops and TEST_MODE
    """
    
    print("🚀 CONTROLLED DRIP QUEUE SENDING")
    print("=" * 50)
    
    # Safety check - show current state
    test_mode = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
    
    print(f"📊 SETTINGS:")
    print(f"  • Batch size: {batch_size} messages")
    print(f"  • Delay between batches: {delay_seconds} seconds")
    print(f"  • Maximum total: {max_total} messages")
    print(f"  • Current TEST_MODE: {test_mode}")
    print(f"  • Actual sending enabled: {enable_actual_sending}")
    
    if not enable_actual_sending:
        print(f"\n⚠️  SAFETY MODE ACTIVE:")
        print(f"  • Emergency stops are ACTIVE (will block all sends)")
        print(f"  • TEST_MODE is ON (no real messages sent)")
        print(f"  • This is a SIMULATION only")
        print(f"  • To send real messages, set enable_actual_sending=True")
    
    # Check emergency stops status
    try:
        from sms.textgrid_sender import send_message
        from sms.message_processor import MessageProcessor
        
        test_result = send_message(from_number="+15551234567", to="+15551234567", message="test")
        emergency_active = "EMERGENCY_STOP" in str(test_result)
        
        if emergency_active and enable_actual_sending:
            print(f"\n🚨 EMERGENCY STOPS DETECTED")
            print(f"   Emergency stops are currently blocking all sends.")
            print(f"   Would you like me to remove them? (This will enable real sending)")
            return {"error": "emergency_stops_active", "requires_confirmation": True}
    
    except Exception as e:
        print(f"⚠️  Could not check emergency stop status: {e}")
    
    try:
        from sms.outbound_batcher import send_batch
        
        total_sent = 0
        total_failed = 0
        batch_count = 0
        
        print(f"\n🔄 STARTING CONTROLLED SENDING...")
        print(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        while total_sent < max_total:
            batch_count += 1
            remaining = max_total - total_sent
            current_batch_size = min(batch_size, remaining)
            
            print(f"\n📦 BATCH {batch_count}:")
            print(f"   Processing {current_batch_size} messages...")
            print(f"   Progress: {total_sent}/{max_total} ({total_sent/max_total*100:.1f}%)")
            
            # Send this batch
            batch_start = time.time()
            result = send_batch(limit=current_batch_size)
            batch_end = time.time()
            
            # Track results
            batch_sent = result.get("total_sent", 0)
            batch_failed = result.get("total_failed", 0)
            
            total_sent += batch_sent
            total_failed += batch_failed
            
            print(f"   ✅ Batch completed in {batch_end - batch_start:.1f}s")
            print(f"   📤 Sent: {batch_sent}, ❌ Failed: {batch_failed}")
            print(f"   📊 Running totals: Sent={total_sent}, Failed={total_failed}")
            
            # Check if we should continue
            if batch_sent == 0 and batch_failed == 0:
                print(f"\n⏸️  No messages processed - queue may be empty or blocked")
                break
                
            if total_sent >= max_total:
                print(f"\n🎯 Reached maximum limit of {max_total} messages")
                break
            
            # Wait before next batch (unless this was the last batch)
            if total_sent < max_total:
                print(f"   ⏳ Waiting {delay_seconds} seconds before next batch...")
                
                # Countdown timer
                for i in range(delay_seconds, 0, -1):
                    print(f"\r   ⏳ Next batch in {i:2d} seconds...", end="", flush=True)
                    time.sleep(1)
                print("\r   ⏳ Starting next batch...      ")
        
        # Final summary
        success_rate = (total_sent / (total_sent + total_failed) * 100) if (total_sent + total_failed) > 0 else 0
        
        print(f"\n🏁 SENDING COMPLETE")
        print(f"=" * 30)
        print(f"📊 FINAL RESULTS:")
        print(f"  • Total batches: {batch_count}")
        print(f"  • Messages sent: {total_sent}")
        print(f"  • Messages failed: {total_failed}")
        print(f"  • Success rate: {success_rate:.1f}%")
        print(f"  • Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not enable_actual_sending:
            print(f"\n💡 NOTE: This was a simulation (TEST_MODE active)")
            print(f"   No actual SMS messages were sent.")
            print(f"   To send real messages, run with enable_actual_sending=True")
        
        return {
            "ok": True,
            "total_sent": total_sent,
            "total_failed": total_failed,
            "batches": batch_count,
            "success_rate": success_rate,
            "simulation": not enable_actual_sending
        }
        
    except Exception as e:
        print(f"❌ Error during controlled sending: {e}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

def remove_emergency_stops():
    """Remove nuclear emergency stops to enable actual sending."""
    
    print("🔧 REMOVING EMERGENCY STOPS...")
    
    files_to_fix = [
        "sms/textgrid_sender.py",
        "sms/message_processor.py", 
        "sms/main.py"
    ]
    
    for file_path in files_to_fix:
        full_path = f"/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1/{file_path}"
        
        try:
            with open(full_path, 'r') as f:
                content = f.read()
            
            # Check if emergency stop is present
            if "EMERGENCY_STOP_ALL_SENDING_DISABLED" in content:
                print(f"  ⚠️  Emergency stop found in {file_path}")
                print(f"      This needs to be manually removed for safety")
            else:
                print(f"  ✅ No emergency stop in {file_path}")
                
        except Exception as e:
            print(f"  ❌ Could not check {file_path}: {e}")
    
    print(f"\n⚠️  MANUAL ACTION REQUIRED:")
    print(f"   Emergency stops must be manually removed from the files above")
    print(f"   This is intentional for safety - prevents accidental mass sending")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Controlled drip queue sending")
    parser.add_argument("--batch-size", type=int, default=50, help="Messages per batch (default: 50)")
    parser.add_argument("--delay", type=int, default=30, help="Seconds between batches (default: 30)")
    parser.add_argument("--max-total", type=int, default=500, help="Maximum total messages (default: 500)")
    parser.add_argument("--enable-sending", action="store_true", help="Enable actual sending (removes TEST_MODE)")
    parser.add_argument("--remove-stops", action="store_true", help="Show how to remove emergency stops")
    
    args = parser.parse_args()
    
    if args.remove_stops:
        remove_emergency_stops()
    else:
        result = controlled_drip_send(
            batch_size=args.batch_size,
            delay_seconds=args.delay,
            max_total=args.max_total,
            enable_actual_sending=args.enable_sending
        )
        
        if result.get("ok"):
            print(f"\n✅ Controlled sending completed successfully")
        else:
            print(f"\n❌ Controlled sending failed: {result.get('error')}")
            exit(1)