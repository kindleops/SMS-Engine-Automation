#!/usr/bin/env python3
"""
Check Leads table schema with different approach
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_leads_schema_alt():
    """Check the Leads table schema with different approach"""
    
    print("🧪 Checking Leads table schema (alternative method)...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        LEADS_TABLE = os.getenv("LEADS_TABLE", "Leads")
        
        leads = Table(AIRTABLE_API_KEY, BASE_ID, LEADS_TABLE)
        
        # Try to get table schema using different approach
        try:
            schema = leads.schema()
            print(f"✅ Schema object: {type(schema)}")
            
            # Try to access fields differently
            if hasattr(schema, 'fields'):
                fields = schema.fields
                print(f"✅ Found fields: {len(fields)}")
                for field in fields:
                    print(f"  - {field.name} ({field.type})")
                    
                # Look for phone-related fields
                phone_fields = [f for f in fields if 'phone' in f.name.lower()]
                print(f"\n📱 Phone-related fields:")
                for field in phone_fields:
                    print(f"  - {field.name}")
                    
            elif hasattr(schema, '__dict__'):
                print(f"Schema attributes: {list(schema.__dict__.keys())}")
        except Exception as schema_e:
            print(f"⚠️ Could not get schema: {schema_e}")
            
        # Try common phone field names
        common_phone_fields = [
            "Phone", "Phone Number", "Mobile", "Cell Phone", 
            "Primary Phone", "Owner Phone", "Contact Phone",
            "Seller Phone", "phone", "mobile", "cell"
        ]
        
        print(f"\n🧪 Testing common phone field names...")
        for field_name in common_phone_fields:
            try:
                test_data = {field_name: "+15551234567"}
                result = leads.create(test_data)
                print(f"✅ SUCCESS with field '{field_name}': {result['id']}")
                # Clean up the test record
                try:
                    leads.delete(result['id'])
                    print(f"🧹 Cleaned up test record {result['id']}")
                except:
                    pass
                break
            except Exception as e:
                if "UNKNOWN_FIELD_NAME" in str(e):
                    print(f"❌ Field '{field_name}' not found")
                else:
                    print(f"⚠️ Field '{field_name}' error: {e}")
                
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_leads_schema_alt()