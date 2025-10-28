#!/usr/bin/env python3
"""
Check the Message Summary (AI) field format
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_ai_summary_field():
    """Check the format of Message Summary (AI) field"""
    
    print("🧪 Checking Message Summary (AI) field format...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get records that have Message Summary (AI) data
        records = convos.all()
        
        ai_summary_examples = []
        for record in records:
            fields = record.get("fields", {})
            ai_summary = fields.get("Message Summary (AI)")
            if ai_summary is not None:
                ai_summary_examples.append({
                    "record_id": record.get("id"),
                    "value": ai_summary,
                    "type": type(ai_summary).__name__,
                    "content": str(ai_summary)[:200] + "..." if len(str(ai_summary)) > 200 else str(ai_summary)
                })
                if len(ai_summary_examples) >= 5:  # Get first 5 examples
                    break
        
        print(f"✅ Found {len(ai_summary_examples)} examples of Message Summary (AI):")
        for i, example in enumerate(ai_summary_examples, 1):
            print(f"\n  Example {i}:")
            print(f"    Record ID: {example['record_id']}")
            print(f"    Type: {example['type']}")
            print(f"    Content: {example['content']}")
                
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_ai_summary_field()