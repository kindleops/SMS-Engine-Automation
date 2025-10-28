#!/usr/bin/env python3
"""
Test script to get the complete Conversations table schema
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def get_complete_schema():
    """Get complete schema of Conversations table"""
    
    print("🧪 Getting complete Conversations table schema...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get all records to see all possible field names
        records = convos.all()
        
        all_fields = set()
        field_examples = {}
        
        for record in records:
            fields = record.get("fields", {})
            all_fields.update(fields.keys())
            
            # Store examples of non-empty values
            for field_name, value in fields.items():
                if value is not None and value != "":
                    if field_name not in field_examples:
                        field_examples[field_name] = value
        
        print(f"✅ Complete field list ({len(all_fields)} fields):")
        for field_name in sorted(all_fields):
            example = field_examples.get(field_name, "")
            if isinstance(example, (list, dict)):
                example = str(type(example).__name__)
            elif isinstance(example, str) and len(example) > 50:
                example = example[:47] + "..."
            print(f"  - {field_name:<35} (ex: {example})")
                
    except Exception as e:
        print(f"❌ Schema check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    get_complete_schema()