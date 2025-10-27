#!/usr/bin/env python3
"""
Test script to check actual Airtable field names
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def check_airtable_fields():
    """Check what fields actually exist in the Conversations table"""
    
    print("🧪 Checking actual Airtable Conversations table fields...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE")
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        print(f"📊 Connecting to:")
        print(f"  Base: {BASE_ID}")
        print(f"  Table: {CONVERSATIONS_TABLE}")
        print(f"  API Key: {'SET' if AIRTABLE_API_KEY else 'NOT SET'}")
        
        if not AIRTABLE_API_KEY or not BASE_ID:
            print("❌ Missing API key or base ID")
            return
            
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get a sample record to see field names
        records = convos.all(max_records=1)
        
        if records:
            sample_record = records[0]
            fields = sample_record.get("fields", {})
            print(f"✅ Found {len(fields)} fields in table:")
            for field_name in sorted(fields.keys()):
                print(f"  - {field_name}")
        else:
            print("⚠️ No records found in table")
            
        # Try to get table schema
        try:
            schema = convos.schema()
            field_names = [field['name'] for field in schema['fields']]
            print(f"\n📋 All field names from schema ({len(field_names)} total):")
            for field_name in sorted(field_names):
                print(f"  - {field_name}")
        except Exception as schema_e:
            print(f"⚠️ Could not get schema: {schema_e}")
        
    except Exception as e:
        print(f"❌ Table check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_airtable_fields()