#!/usr/bin/env python3
"""
Check Leads table field names
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_leads_fields():
    """Check what fields exist in the Leads table"""
    
    print("🧪 Checking Leads table field names...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
        
        leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
        
        # Get records to see field names
        records = leads.all(max_records=5)
        
        all_fields = set()
        for record in records:
            fields = record.get("fields", {})
            all_fields.update(fields.keys())
        
        print(f"✅ Found {len(all_fields)} fields in Leads table:")
        for field_name in sorted(all_fields):
            print(f"  - {field_name}")
            
        # Look for phone-related fields
        phone_fields = [f for f in all_fields if 'phone' in f.lower()]
        print(f"\n📱 Phone-related fields:")
        for field in phone_fields:
            print(f"  - {field}")
                
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_leads_fields()