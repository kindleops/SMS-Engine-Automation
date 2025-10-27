#!/usr/bin/env python3
"""
Test script to check Direction field values
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def check_direction_values():
    """Check what values exist in the Direction field"""
    
    print("🧪 Checking Direction field values...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get records to see Direction field values
        records = convos.all(max_records=20)
        
        direction_values = set()
        for record in records:
            fields = record.get("fields", {})
            direction = fields.get("Direction")
            if direction:
                direction_values.add(direction)
        
        print(f"✅ Found Direction values in existing records:")
        for value in sorted(direction_values):
            print(f"  - '{value}'")
            
        if not direction_values:
            print("⚠️ No Direction values found in existing records")
                
    except Exception as e:
        print(f"❌ Direction check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_direction_values()