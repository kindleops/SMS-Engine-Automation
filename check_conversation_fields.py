#!/usr/bin/env python3
"""
Check recent conversations to verify all fields are being populated correctly
"""

from sms.datastore import CONNECTOR

print("🔍 Checking recent conversation records for field completeness...")

try:
    convos_handle = CONNECTOR.conversations()
    if convos_handle.in_memory:
        print("⚠️ Conversations table is in memory mode - no real data")
        exit(0)
    
    convos_table = convos_handle.table
    
    # Get the 5 most recent records
    recent_records = convos_table.all(sort=["-Received Time"], max_records=5)
    
    print(f"\n📊 Found {len(recent_records)} recent conversation records:")
    
    # Fields to check for completeness
    expected_fields = [
        "Direction", "Message", "Seller Phone Number", "TextGrid Phone Number",
        "TextGrid ID", "Received Time", "Processed Time", "Processed By",
        "Delivery Status", "Intent Detected", "AI Intent", "Stage",
        "Sent Count", "Reply Count", "Date Received", "Time"
    ]
    
    for i, record in enumerate(recent_records, 1):
        fields = record.get("fields", {})
        record_id = record.get("id", "unknown")
        
        print(f"\n📝 Record {i} ({record_id}):")
        print(f"   Direction: {fields.get('Direction', 'MISSING')}")
        print(f"   Phone: {fields.get('Seller Phone Number', 'MISSING')}")
        print(f"   TextGrid ID: {fields.get('TextGrid ID', 'MISSING')}")
        print(f"   Time: {fields.get('Received Time', 'MISSING')}")
        print(f"   Processed By: {fields.get('Processed By', 'MISSING')}")
        print(f"   Status: {fields.get('Delivery Status', 'MISSING')}")
        
        # Check linking fields
        linking_info = []
        if fields.get('Prospect'):
            linking_info.append(f"Prospect: {fields['Prospect']}")
        if fields.get('Bulk Campaign'):
            linking_info.append(f"Campaign: {fields['Bulk Campaign']}")
        if fields.get('Template'):
            linking_info.append(f"Template: {fields['Template']}")
        if fields.get('Drip Queue'):
            linking_info.append(f"Drip Queue: {fields['Drip Queue']}")
        if fields.get('County'):
            linking_info.append(f"County: {fields['County']}")
            
        if linking_info:
            print(f"   Linked: {', '.join(linking_info)}")
        else:
            print(f"   Linked: None")
        
        # Count missing vs populated fields
        populated = sum(1 for field in expected_fields if fields.get(field))
        missing = len(expected_fields) - populated
        print(f"   Fields: {populated}/{len(expected_fields)} populated ({missing} missing)")
        
        # Show any missing critical fields
        critical_missing = [field for field in expected_fields if not fields.get(field)]
        if critical_missing:
            print(f"   Missing: {', '.join(critical_missing)}")
        
except Exception as e:
    print(f"❌ Error checking conversations: {e}")
    import traceback
    traceback.print_exc()