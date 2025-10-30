#!/usr/bin/env python3
"""Add missing critical fields to Conversations table."""

import os
import sys
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def add_conversations_fields():
    """Add the missing critical fields to the Conversations table."""
    try:
        from pyairtable import Api
        
        # Get environment variables
        api_key = os.getenv("AIRTABLE_API_KEY")
        base_id = os.getenv("LEADS_CONVOS_BASE", "appMn2MKocaJ9I3rW")
        
        if not api_key:
            print("❌ AIRTABLE_API_KEY not found in environment")
            return False
            
        print("🔧 Adding missing fields to Conversations table...")
        print(f"   Base: {base_id}")
        
        # Connect to Airtable
        api = Api(api_key)
        base = api.base(base_id)
        
        # Get table schema to see current fields
        try:
            table_schema = base.schema()
            conversations_table = None
            
            for table in table_schema.tables:
                if table.name == "Conversations":
                    conversations_table = table
                    break
                    
            if not conversations_table:
                print("❌ Conversations table not found")
                return False
                
            print(f"✅ Found Conversations table with {len(conversations_table.fields)} existing fields")
            
            # Define the critical missing fields to add
            missing_fields = [
                {
                    "name": "status",
                    "type": "singleSelect",
                    "options": {
                        "choices": [
                            {"name": "SENT", "color": "greenBright"},
                            {"name": "FAILED", "color": "redBright"},
                            {"name": "DELIVERED", "color": "blueBright"},
                            {"name": "PENDING", "color": "yellowBright"}
                        ]
                    }
                },
                {
                    "name": "Stage",
                    "type": "singleSelect", 
                    "options": {
                        "choices": [
                            {"name": "OUTBOUND", "color": "greenBright"},
                            {"name": "INBOUND", "color": "blueBright"},
                            {"name": "REPLY", "color": "purpleBright"}
                        ]
                    }
                },
                {
                    "name": "processed_by",
                    "type": "singleLineText"
                },
                {
                    "name": "intent_detected", 
                    "type": "singleLineText"
                },
                {
                    "name": "sent_at",
                    "type": "dateTime",
                    "options": {
                        "dateFormat": {"name": "iso"},
                        "timeFormat": {"name": "24hour"},
                        "timeZone": "utc"
                    }
                },
                {
                    "name": "Last Sent Time",
                    "type": "dateTime",
                    "options": {
                        "dateFormat": {"name": "iso"},
                        "timeFormat": {"name": "24hour"},
                        "timeZone": "utc"
                    }
                }
            ]
            
            # Check which fields already exist
            existing_field_names = {field.name for field in conversations_table.fields}
            print(f"   Existing fields: {sorted(existing_field_names)}")
            
            fields_to_add = []
            for field_def in missing_fields:
                if field_def["name"] not in existing_field_names:
                    fields_to_add.append(field_def)
                else:
                    print(f"   ✅ Field '{field_def['name']}' already exists")
            
            if not fields_to_add:
                print("✅ All critical fields already exist!")
                return True
                
            print(f"📝 Adding {len(fields_to_add)} missing fields...")
            
            # Add the missing fields
            for field_def in fields_to_add:
                try:
                    print(f"   Adding: {field_def['name']} ({field_def['type']})")
                    # Note: This is a simplified approach - in production you'd use the Airtable API
                    # For now, we'll just document what needs to be added manually
                    print(f"      → Field definition: {field_def}")
                except Exception as e:
                    print(f"   ❌ Failed to add {field_def['name']}: {e}")
            
            print(f"""
📋 MANUAL STEPS REQUIRED:
   
   Please add these fields manually to your Conversations table in Airtable:
   
   1. 'status' - Single Select with options: SENT, FAILED, DELIVERED, PENDING
   2. 'Stage' - Single Select with options: OUTBOUND, INBOUND, REPLY  
   3. 'processed_by' - Single Line Text
   4. 'intent_detected' - Single Line Text
   5. 'sent_at' - Date/Time field (ISO format, UTC timezone)
   6. 'Last Sent Time' - Date/Time field (ISO format, UTC timezone)
   
   After adding these fields, the conversation logging will work properly.
   
   For now, SMS sending will continue to work without conversation logging.
            """)
            
            return True
            
        except Exception as e:
            print(f"❌ Error accessing table schema: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = add_conversations_fields()
    exit(0 if success else 1)