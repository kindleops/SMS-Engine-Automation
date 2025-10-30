#!/usr/bin/env python3
"""Check actual Airtable field names vs schema definitions."""

import os
import sys
from typing import Dict, List, Set

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def check_actual_airtable_fields():
    """Check actual Airtable field names vs schema definitions."""
    
    print("🔍 CHECKING ACTUAL AIRTABLE VS SCHEMA")
    print("=" * 50)
    
    try:
        from pyairtable import Table
        from sms.airtable_schema import CONVERSATIONS_TABLE
        
        # Load env
        from dotenv import load_dotenv
        load_dotenv()
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE_NAME = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        if not AIRTABLE_API_KEY or not BASE_ID:
            print("❌ Missing AIRTABLE_API_KEY or LEADS_CONVOS_BASE")
            return False
            
        print(f"📋 Connecting to: {BASE_ID}/{CONVERSATIONS_TABLE_NAME}")
        
        # Get actual table
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE_NAME)
        
        # Get one record to see actual field names
        records = convos.all(max_records=1)
        
        if not records:
            print("⚠️ No records found in table")
            return False
            
        actual_fields = set(records[0].get("fields", {}).keys())
        print(f"✅ Found {len(actual_fields)} actual fields in Airtable")
        
        # Get schema expected fields
        schema_fields = CONVERSATIONS_TABLE.field_names()
        expected_fields = set(schema_fields.values())
        
        print(f"\n🔍 FIELD COMPARISON:")
        print(f"  Schema expects: {len(expected_fields)} fields")
        print(f"  Airtable has: {len(actual_fields)} fields")
        
        # Find mismatches
        missing_in_airtable = expected_fields - actual_fields
        extra_in_airtable = actual_fields - expected_fields
        matching_fields = expected_fields & actual_fields
        
        print(f"\n✅ MATCHING FIELDS ({len(matching_fields)}):")
        for field in sorted(matching_fields):
            print(f"  • '{field}'")
        
        if missing_in_airtable:
            print(f"\n❌ MISSING IN AIRTABLE ({len(missing_in_airtable)}):")
            for field in sorted(missing_in_airtable):
                # Find which schema key this belongs to
                schema_key = None
                for key, value in schema_fields.items():
                    if value == field:
                        schema_key = key
                        break
                print(f"  • '{field}' (schema key: {schema_key})")
        
        if extra_in_airtable:
            print(f"\n➕ EXTRA IN AIRTABLE ({len(extra_in_airtable)}):")
            for field in sorted(extra_in_airtable):
                print(f"  • '{field}'")
        
        # Check critical single select and linked fields
        print(f"\n🔗 CRITICAL FIELD STATUS:")
        critical_mappings = {
            "STAGE": "Stage",
            "PROCESSED_BY": "processed_by", 
            "DIRECTION": "Direction",
            "STATUS": "status",
            "PROSPECT_LINK": "Prospect",
            "CAMPAIGN_LINK": "Campaign",
            "TEMPLATE_LINK": "Template",
            "DRIP_QUEUE_LINK": "Drip Queue",
            "LEAD_LINK": "Lead"
        }
        
        for schema_key, expected_name in critical_mappings.items():
            actual_name = schema_fields.get(schema_key, "NOT_FOUND")
            if actual_name in actual_fields:
                print(f"  ✅ {schema_key}: '{actual_name}' ← FOUND")
            else:
                print(f"  ❌ {schema_key}: '{actual_name}' ← MISSING")
                
                # Check if there's a close match
                close_matches = [f for f in actual_fields if expected_name.lower() in f.lower() or f.lower() in expected_name.lower()]
                if close_matches:
                    print(f"     Possible matches: {close_matches}")
        
        # Sample data validation
        print(f"\n📊 SAMPLE DATA VALIDATION:")
        sample_record = records[0]["fields"]
        
        # Check single select field values
        for schema_key in ["STAGE", "PROCESSED_BY", "DIRECTION", "STATUS"]:
            field_name = schema_fields.get(schema_key)
            if field_name and field_name in sample_record:
                value = sample_record[field_name]
                print(f"  {schema_key} ('{field_name}'): '{value}'")
            else:
                print(f"  {schema_key}: MISSING OR NO DATA")
        
        # Check linked fields  
        for schema_key in ["PROSPECT_LINK", "CAMPAIGN_LINK", "TEMPLATE_LINK"]:
            field_name = schema_fields.get(schema_key)
            if field_name and field_name in sample_record:
                value = sample_record[field_name]
                if isinstance(value, list) and value:
                    print(f"  {schema_key} ('{field_name}'): LINKED TO {len(value)} record(s)")
                else:
                    print(f"  {schema_key} ('{field_name}'): NO LINKS")
            else:
                print(f"  {schema_key}: MISSING OR NO DATA")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking Airtable fields: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = check_actual_airtable_fields()
    exit(0 if success else 1)