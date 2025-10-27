#!/usr/bin/env python3
"""
Test script to check more Airtable field names
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def check_more_fields():
    """Check more records to find all possible field names"""
    
    print("🧪 Checking more Airtable Conversations table records...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get multiple records to see all possible field names
        records = convos.all(max_records=10)
        
        all_fields = set()
        for record in records:
            fields = record.get("fields", {})
            all_fields.update(fields.keys())
        
        print(f"✅ Found {len(all_fields)} unique fields across {len(records)} records:")
        for field_name in sorted(all_fields):
            print(f"  - {field_name}")
            
        # Check if our required fields exist
        required_fields = ["status", "Stage", "intent_detected", "AI Intent"]
        print(f"\n🔍 Checking for required fields:")
        for req_field in required_fields:
            if req_field in all_fields:
                print(f"  ✅ {req_field}")
            else:
                print(f"  ❌ {req_field} (MISSING)")
                
    except Exception as e:
        print(f"❌ Field check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_more_fields()