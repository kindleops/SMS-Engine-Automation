#!/usr/bin/env python3
"""
Check Leads table schema
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_leads_schema():
    """Check the Leads table schema"""
    
    print("🧪 Checking Leads table schema...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
        
        print(f"📊 Connecting to:")
        print(f"  Base: {BASE_ID}")
        print(f"  Table: {LEADS_TABLE}")
        
        leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
        
        # Try to get table schema
        try:
            schema = leads.schema()
            field_names = [field['name'] for field in schema['fields']]
            print(f"\n✅ Found {len(field_names)} fields in Leads table schema:")
            for field_name in sorted(field_names):
                print(f"  - {field_name}")
                
            # Look for phone-related fields
            phone_fields = [f for f in field_names if 'phone' in f.lower()]
            print(f"\n📱 Phone-related fields:")
            for field in phone_fields:
                print(f"  - {field}")
        except Exception as schema_e:
            print(f"⚠️ Could not get schema: {schema_e}")
            
        # Try to create a test record to see what happens
        try:
            print(f"\n🧪 Testing record creation...")
            test_data = {"phone": "+15551234567"}  # This should fail
            result = leads.create(test_data)
            print(f"✅ Unexpected success: {result}")
        except Exception as create_e:
            print(f"❌ Expected failure: {create_e}")
                
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_leads_schema()