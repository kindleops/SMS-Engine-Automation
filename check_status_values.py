#!/usr/bin/env python3
"""Check what status values are available in the drip queue."""

import os
import sys
from collections import defaultdict

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def check_status_values():
    """Check what status values exist in the drip queue."""
    try:
        from sms.datastore import CONNECTOR, list_records
        
        print("🔍 Checking existing status values in drip queue...")
        
        # Get sample records
        drip_handle = CONNECTOR.drip_queue()
        records = list_records(drip_handle, max_records=100)
        
        status_counts = defaultdict(int)
        
        for record in records:
            fields = record.get("fields", {})
            status = fields.get("Status", "No Status")
            status_counts[status] += 1
        
        print(f"\n📊 Found {len(records)} sample records")
        print(f"📈 Status Values Found:")
        for status, count in sorted(status_counts.items()):
            print(f"   '{status}': {count} records")
        
        return list(status_counts.keys())
        
    except Exception as e:
        print(f"❌ Error checking status values: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    valid_statuses = check_status_values()
    print(f"\nValid status options: {valid_statuses}")