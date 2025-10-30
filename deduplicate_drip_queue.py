#!/usr/bin/env python3
"""Deduplicate drip queue to prevent multiple messages to same phone numbers."""

import os
import sys
from collections import defaultdict
from typing import Dict, List, Any

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def deduplicate_drip_queue(dry_run=True):
    """
    Remove duplicate phone numbers from drip queue, keeping only the best record for each phone.
    
    Priority logic:
    1. Keep records with status "Sent" (already processed)
    2. For duplicates, prefer "Ready" over "Queued" 
    3. If same status, keep the one with earliest Next Send Time
    4. If same send time, keep the first record encountered
    """
    try:
        from sms.datastore import CONNECTOR, list_records, update_record
        
        print("🔄 Starting drip queue deduplication...")
        print(f"   Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will make changes)'}")
        
        # Get all drip queue records
        print("\n📊 Fetching all drip queue records...")
        drip_handle = CONNECTOR.drip_queue()
        records = list_records(drip_handle, max_records=10000)
        print(f"   Total records: {len(records)}")
        
        # Group by phone number
        phone_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        for record in records:
            fields = record.get("fields", {})
            # Try multiple potential phone field names
            phone = (fields.get("Phone", "") or 
                    fields.get("phone", "") or
                    fields.get("Phone Number", "") or
                    fields.get("Mobile", "") or
                    fields.get("Cell", "") or 
                    fields.get("Primary Phone", "") or
                    fields.get("TextGrid Phone Number", "") or
                    "")
            if phone:
                phone_groups[phone].append({
                    "record": record,
                    "record_id": record.get("id"),
                    "phone": phone,
                    "status": fields.get("Status", "Unknown"),
                    "next_send": fields.get("Next Send Time", ""),
                    "campaign": fields.get("Campaign", ["Unknown"])[0] if fields.get("Campaign") else "Unknown",
                    "template": fields.get("Template", ["Unknown"])[0] if fields.get("Template") else "Unknown",
                    "message": fields.get("Message", "")[:50] + "..." if len(fields.get("Message", "")) > 50 else fields.get("Message", "")
                })
        
        # Find duplicates
        duplicates = {phone: entries for phone, entries in phone_groups.items() if len(entries) > 1}
        
        print(f"\n📈 DEDUPLICATION ANALYSIS:")
        print(f"   Total unique phone numbers: {len(phone_groups)}")
        print(f"   Phone numbers with duplicates: {len(duplicates)}")
        print(f"   Total records to review: {sum(len(entries) for entries in duplicates.values())}")
        
        if not duplicates:
            print("   ✅ No duplicates found - drip queue is already clean!")
            return
        
        # Process duplicates
        records_to_keep = []
        records_to_remove = []
        
        for phone, entries in duplicates.items():
            # Sort by priority:
            # 1. "Sent" status first (already processed, don't touch)
            # 2. "Ready" over "Queued" 
            # 3. Earlier Next Send Time
            # 4. Record ID (for consistency)
            
            def priority_key(entry):
                status = entry["status"]
                next_send = entry["next_send"] or "9999-12-31"  # Default to far future if empty
                record_id = entry["record_id"]
                
                # Priority order: Sent=0, Ready=1, Queued=2, Other=3
                status_priority = {"Sent": 0, "Ready": 1, "Queued": 2}.get(status, 3)
                
                return (status_priority, next_send, record_id)
            
            sorted_entries = sorted(entries, key=priority_key)
            
            # Keep the first (highest priority) record
            keep_entry = sorted_entries[0]
            remove_entries = sorted_entries[1:]
            
            records_to_keep.append(keep_entry)
            records_to_remove.extend(remove_entries)
        
        print(f"\n🎯 DEDUPLICATION PLAN:")
        print(f"   Records to keep: {len(records_to_keep)}")
        print(f"   Records to remove: {len(records_to_remove)}")
        
        # Show some examples
        print(f"\n📝 EXAMPLE DUPLICATES (showing first 5):")
        example_phones = list(duplicates.keys())[:5]
        for phone in example_phones:
            entries = duplicates[phone]
            print(f"\n   {phone} ({len(entries)} records):")
            
            # Sort same way as above
            def priority_key(entry):
                status = entry["status"]
                next_send = entry["next_send"] or "9999-12-31"
                record_id = entry["record_id"]
                status_priority = {"Sent": 0, "Ready": 1, "Queued": 2}.get(status, 3)
                return (status_priority, next_send, record_id)
            
            sorted_entries = sorted(entries, key=priority_key)
            
            for i, entry in enumerate(sorted_entries, 1):
                action = "KEEP" if i == 1 else "REMOVE"
                status_emoji = "✅" if entry["status"] == "Sent" else "⏳" if entry["status"] == "Queued" else "🔄"
                print(f"     {i}. {action:6} | {status_emoji} {entry['status']:6} | {entry['next_send'][:10]} | {entry['message']}")
        
        # Analyze what we're removing
        remove_status_count = defaultdict(int)
        for entry in records_to_remove:
            remove_status_count[entry["status"]] += 1
        
        print(f"\n📊 RECORDS TO REMOVE BY STATUS:")
        for status, count in sorted(remove_status_count.items()):
            print(f"   {status}: {count} records")
        
        if dry_run:
            print(f"\n🔍 DRY RUN COMPLETE - No changes made")
            print(f"   To actually remove duplicates, run with dry_run=False")
            return
        
        # Actually remove duplicates
        print(f"\n🚀 REMOVING DUPLICATE RECORDS...")
        removed_count = 0
        failed_count = 0
        
        # First, let's try just a few records to test
        test_batch = records_to_remove[:5] if dry_run else records_to_remove
        print(f"   Processing {len(test_batch)} records...")
        
        for entry in test_batch:
            try:
                # Actually delete the record from Airtable
                record_id = entry["record_id"]
                table = drip_handle.table
                result = table.delete(record_id)
                if result:
                    removed_count += 1
                    print(f"   ✅ Deleted duplicate record {record_id} ({entry['phone']})")
                    if removed_count % 10 == 0:
                        print(f"   Progress: {removed_count}/{len(test_batch)} removed...")
                else:
                    failed_count += 1
                    print(f"   ❌ Failed to delete record {record_id}")
            except Exception as e:
                print(f"   ⚠️ Failed to delete {entry['record_id']}: {e}")
                failed_count += 1
        
        print(f"\n✅ DEDUPLICATION COMPLETE:")
        print(f"   Successfully removed: {removed_count} duplicate records")
        print(f"   Failed to remove: {failed_count} records")
        print(f"   Unique phone numbers: {len(phone_groups)}")
        print(f"   Active records remaining: {len(records_to_keep)}")
        
    except Exception as e:
        print(f"❌ Error during deduplication: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Run in dry-run mode by default
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1].lower() in ['--live', '--execute', '--real']:
        dry_run = False
        print("⚠️  LIVE MODE - Changes will be made to the drip queue!")
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Cancelled.")
            exit(0)
    
    deduplicate_drip_queue(dry_run=dry_run)