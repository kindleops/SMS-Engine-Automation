#!/usr/bin/env python3
"""
Check Processed By field values
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_processed_by_values():
    """Check what values exist in the Processed By field"""
    
    print("🧪 Checking Processed By field values...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get records to see Processed By field values
        records = convos.all(max_records=50)
        
        processed_by_values = set()
        for record in records:
            fields = record.get("fields", {})
            processed_by = fields.get("Processed By")
            if processed_by:
                processed_by_values.add(processed_by)
        
        print(f"✅ Found Processed By values in existing records:")
        for value in sorted(processed_by_values):
            print(f"  - '{value}'")
            
        if not processed_by_values:
            print("⚠️ No Processed By values found in existing records")
                
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_processed_by_values()