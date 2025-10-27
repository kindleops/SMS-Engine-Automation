#!/usr/bin/env python3
"""
Test script to debug field mapping resolution
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

def test_field_mapping():
    """Test the field mapping resolution"""
    
    print("🧪 Testing field mapping resolution...")
    
    try:
        from sms.airtable_schema import conversations_field_map
        
        CONV_FIELDS = conversations_field_map()
        print(f"✅ Field mappings loaded: {CONV_FIELDS}")
        
        FROM_FIELD = CONV_FIELDS["FROM"]
        TO_FIELD = CONV_FIELDS["TO"]
        MSG_FIELD = CONV_FIELDS["BODY"]
        STATUS_FIELD = CONV_FIELDS["STATUS"]
        
        print(f"📋 Field mapping results:")
        print(f"  FROM_FIELD: {FROM_FIELD}")
        print(f"  TO_FIELD: {TO_FIELD}")
        print(f"  MSG_FIELD: {MSG_FIELD}")
        print(f"  STATUS_FIELD: {STATUS_FIELD}")
        
    except Exception as e:
        print(f"❌ Field mapping failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_field_mapping()