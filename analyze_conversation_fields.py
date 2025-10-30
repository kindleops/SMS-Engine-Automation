#!/usr/bin/env python3
"""Analyze all linked fields and single select fields in Conversations table."""

import os
import sys
from typing import Dict, List, Tuple

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def analyze_conversation_fields():
    """Analyze linked fields and single select fields in Conversations table."""
    
    print("🔍 ANALYZING CONVERSATION FIELD MAPPINGS")
    print("=" * 60)
    
    try:
        from sms.airtable_schema import (
            CONVERSATIONS_TABLE, 
            ConversationStage,
            ConversationProcessor, 
            ConversationIntent,
            ConversationDirection,
            ConversationDeliveryStatus,
            ConversationAIIntent
        )
        
        # Get field mappings
        field_map = CONVERSATIONS_TABLE.field_names()
        print(f"\n📋 CONVERSATIONS TABLE FIELD MAPPINGS:")
        print(f"Table: {CONVERSATIONS_TABLE.default}")
        
        # Categorize fields
        linked_fields = {}
        single_select_fields = {}
        other_fields = {}
        
        for key, resolved_name in field_map.items():
            field_def = CONVERSATIONS_TABLE.fields[key]
            
            # Check if it's a linked field (contains LINK in name)
            if "LINK" in key:
                linked_fields[key] = {
                    "resolved_name": resolved_name,
                    "env_vars": field_def.env_vars,
                    "fallbacks": field_def.fallbacks
                }
            # Check if it has options (single select)
            elif field_def.options:
                single_select_fields[key] = {
                    "resolved_name": resolved_name,
                    "options": field_def.options,
                    "env_vars": field_def.env_vars,
                    "fallbacks": field_def.fallbacks
                }
            else:
                other_fields[key] = {
                    "resolved_name": resolved_name,
                    "env_vars": field_def.env_vars,
                    "fallbacks": field_def.fallbacks
                }
        
        # Print linked fields
        print(f"\n🔗 LINKED FIELDS ({len(linked_fields)}):")
        for key, info in linked_fields.items():
            print(f"  {key}:")
            print(f"    → Field Name: '{info['resolved_name']}'")
            print(f"    → Env Vars: {info['env_vars']}")
            if info['fallbacks']:
                print(f"    → Fallbacks: {info['fallbacks']}")
            print()
        
        # Print single select fields  
        print(f"\n📝 SINGLE SELECT FIELDS ({len(single_select_fields)}):")
        for key, info in single_select_fields.items():
            print(f"  {key}:")
            print(f"    → Field Name: '{info['resolved_name']}'")
            print(f"    → Options ({len(info['options'])}):")
            for option in info['options']:
                print(f"      • '{option}'")
            print(f"    → Env Vars: {info['env_vars']}")
            if info['fallbacks']:
                print(f"    → Fallbacks: {info['fallbacks']}")
            print()
        
        # Check current environment configuration
        print(f"\n🔧 CURRENT ENVIRONMENT CONFIGURATION:")
        for key, info in {**linked_fields, **single_select_fields}.items():
            for env_var in info['env_vars']:
                value = os.getenv(env_var)
                if value:
                    print(f"  {env_var} = '{value}'")
        
        # Show enum mappings for single selects
        print(f"\n📊 ENUM MAPPINGS:")
        enum_mappings = {
            "STAGE": ConversationStage,
            "PROCESSED_BY": ConversationProcessor,
            "INTENT": ConversationIntent, 
            "DIRECTION": ConversationDirection,
            "STATUS": ConversationDeliveryStatus,
            "AI_INTENT": ConversationAIIntent
        }
        
        for field_key, enum_class in enum_mappings.items():
            if field_key in single_select_fields:
                print(f"  {field_key} → {enum_class.__name__}:")
                for enum_val in enum_class:
                    print(f"    • {enum_val.name} = '{enum_val.value}'")
                print()
        
        # Summary
        print(f"\n📈 SUMMARY:")
        print(f"  • Linked Fields: {len(linked_fields)}")
        print(f"  • Single Select Fields: {len(single_select_fields)}")
        print(f"  • Other Fields: {len(other_fields)}")
        print(f"  • Total Fields: {len(field_map)}")
        
        # Critical field validation
        print(f"\n⚠️  CRITICAL FIELD VALIDATION:")
        critical_fields = ["STAGE", "PROCESSED_BY", "DIRECTION", "STATUS", "PROSPECT_LINK", "CAMPAIGN_LINK"]
        
        for field in critical_fields:
            if field in field_map:
                resolved = field_map[field]
                print(f"  ✅ {field} → '{resolved}'")
            else:
                print(f"  ❌ {field} → MISSING!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error analyzing conversation fields: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = analyze_conversation_fields()
    exit(0 if success else 1)