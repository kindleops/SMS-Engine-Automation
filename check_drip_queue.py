#!/usr/bin/env python3
"""
Check the status of the drip queue to see pending messages
"""

import json
from datetime import datetime, timezone
from sms.datastore import CONNECTOR

print("🔍 Checking drip queue status...")

try:
    # Get the drip queue table
    drip_handle = CONNECTOR.drip_queue()
    
    if drip_handle.in_memory:
        print("⚠️ Drip queue is in memory mode - no real data available")
        exit(0)
    
    drip_table = drip_handle.table
    
    # Get all records to analyze
    print("📊 Fetching drip queue records...")
    all_records = drip_table.all()
    
    print(f"📈 Total records in drip queue: {len(all_records)}")
    
    # Analyze by status
    status_counts = {}
    due_count = 0
    now = datetime.now(timezone.utc)
    
    for record in all_records:
        fields = record.get("fields", {})
        status = fields.get("Status", "Unknown")
        next_send = fields.get("Next Send Date")
        
        # Count by status
        status_counts[status] = status_counts.get(status, 0) + 1
        
        # Check if due for sending
        if status in ["Queued", "Sending"]:
            if next_send:
                try:
                    from dateutil.parser import parse
                    send_time = parse(next_send)
                    if send_time <= now:
                        due_count += 1
                except:
                    due_count += 1  # If we can't parse, assume it's due
            else:
                due_count += 1  # No send date means send now
    
    print("\n📊 Status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"   {status}: {count}")
    
    print(f"\n⏰ Messages due for sending now: {due_count}")
    
    if due_count == 0:
        print("\n💭 No messages are currently due for sending. This could be because:")
        print("   • All messages have been sent")
        print("   • Messages are scheduled for future dates")
        print("   • System is in quiet hours")
        print("   • Rate limits are preventing sends")
    else:
        print(f"\n🚀 {due_count} messages are ready to be sent!")
        
    # Show a few sample records for debugging
    if all_records:
        print(f"\n🔍 Sample records (showing first 3):")
        for i, record in enumerate(all_records[:3]):
            fields = record.get("fields", {})
            status = fields.get("Status", "N/A")
            phone = fields.get("Seller Phone Number", "N/A")
            next_send = fields.get("Next Send Date", "N/A")
            print(f"   {i+1}. {phone} | Status: {status} | Next Send: {next_send}")
        
except Exception as e:
    print(f"❌ Error checking drip queue: {e}")
    import traceback
    traceback.print_exc()