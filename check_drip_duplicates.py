#!/usr/bin/env python3
"""Check for duplicate phone numbers in drip queue."""

import os
import sys
from collections import defaultdict
from typing import Dict, List, Any

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def check_drip_queue_duplicates():
    """Check for duplicate phone numbers in the drip queue."""
    try:
        from sms.datastore import CONNECTOR, list_records
        
        print("🔍 Analyzing drip queue for duplicate phone numbers...")
        
        # Get all drip queue records
        print("📊 Fetching all drip queue records...")
        drip_handle = CONNECTOR.drip_queue()
        records = list_records(drip_handle, max_records=10000)  # Get more records
        print(f"   Total records: {len(records)}")
        
        # Debug: Check field names in first few records
        if records:
            print(f"\n🔍 SAMPLE RECORD FIELDS:")
            for i, record in enumerate(records[:3], 1):
                fields = record.get("fields", {})
                print(f"   Record {i} fields: {list(fields.keys())}")
                # Show phone-related fields
                phone_fields = [k for k in fields.keys() if 'phone' in k.lower() or 'number' in k.lower()]
                if phone_fields:
                    print(f"     Phone-related fields: {phone_fields}")
                    for pf in phone_fields:
                        print(f"       {pf}: {fields.get(pf)}")
        
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
                    "record_id": record.get("id"),
                    "phone": phone,
                    "status": fields.get("Status", "Unknown"),
                    "next_send": fields.get("Next Send Time", ""),
                    "campaign": fields.get("Campaign", ["Unknown"])[0] if fields.get("Campaign") else "Unknown",
                    "template": fields.get("Template", ["Unknown"])[0] if fields.get("Template") else "Unknown",
                    "message": fields.get("Message", "")[:100] + "..." if len(fields.get("Message", "")) > 100 else fields.get("Message", "")
                })
        
        # Find duplicates
        duplicates = {phone: entries for phone, entries in phone_groups.items() if len(entries) > 1}
        
        print(f"\n📈 DUPLICATE ANALYSIS:")
        print(f"   Total unique phone numbers: {len(phone_groups)}")
        print(f"   Phone numbers with duplicates: {len(duplicates)}")
        print(f"   Total duplicate records: {sum(len(entries) for entries in duplicates.values())}")
        
        if duplicates:
            print(f"\n🚨 FOUND {len(duplicates)} PHONE NUMBERS WITH DUPLICATES:")
            
            # Show top 10 duplicates
            duplicate_list = list(duplicates.items())
            duplicate_list.sort(key=lambda x: len(x[1]), reverse=True)
            
            for i, (phone, entries) in enumerate(duplicate_list[:10], 1):
                print(f"\n{i}. {phone} ({len(entries)} records):")
                for j, entry in enumerate(entries, 1):
                    status_emoji = "✅" if entry["status"] == "Sent" else "⏳" if entry["status"] == "Queued" else "🔄"
                    print(f"     {j}. {status_emoji} {entry['status']} | Campaign: {entry['campaign']} | Template: {entry['template']}")
                    print(f"        Message: {entry['message']}")
                    print(f"        Record ID: {entry['record_id']}")
            
            if len(duplicate_list) > 10:
                print(f"\n... and {len(duplicate_list) - 10} more phones with duplicates")
        
        # Check status distribution of duplicates
        duplicate_status_count = defaultdict(int)
        for entries in duplicates.values():
            for entry in entries:
                duplicate_status_count[entry["status"]] += 1
        
        if duplicate_status_count:
            print(f"\n📊 DUPLICATE RECORDS BY STATUS:")
            for status, count in sorted(duplicate_status_count.items()):
                print(f"   {status}: {count} records")
        
        # Check what causes duplicates (campaign/template combinations)
        print(f"\n🔍 ANALYZING DUPLICATE CAUSES:")
        duplicate_causes = defaultdict(int)
        for phone, entries in duplicates.items():
            campaigns = set(entry["campaign"] for entry in entries)
            templates = set(entry["template"] for entry in entries)
            if len(campaigns) > 1:
                duplicate_causes["Different Campaigns"] += 1
            if len(templates) > 1:
                duplicate_causes["Different Templates"] += 1
            if len(campaigns) == 1 and len(templates) == 1:
                duplicate_causes["Same Campaign & Template"] += 1
        
        for cause, count in sorted(duplicate_causes.items()):
            print(f"   {cause}: {count} phones")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        if duplicates:
            queued_duplicates = sum(1 for entries in duplicates.values() 
                                  for entry in entries if entry["status"] == "Queued")
            sent_duplicates = sum(1 for entries in duplicates.values() 
                                for entry in entries if entry["status"] == "Sent")
            
            print(f"   📤 {sent_duplicates} duplicate records already sent (may have caused actual duplicate messages)")
            print(f"   ⏳ {queued_duplicates} duplicate records still queued (will cause duplicates if sent)")
            print(f"   🔧 Consider removing queued duplicates to prevent further duplicate sends")
            print(f"   📝 Review campaign/template logic to prevent future duplicates")
        else:
            print(f"   ✅ No duplicates found - drip queue is clean!")
        
        return len(duplicates) > 0
        
    except Exception as e:
        print(f"❌ Error checking drip queue duplicates: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    has_duplicates = check_drip_queue_duplicates()
    exit(1 if has_duplicates else 0)