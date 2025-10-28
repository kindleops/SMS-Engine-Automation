#!/usr/bin/env python3
"""
Check the Response Time field format
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def check_response_time_field():
    """Check the format of Response Time field"""
    
    print("🧪 Checking Response Time (Minutes) field format...")
    
    try:
        from pyairtable import Table
        
        AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
        BASE_ID = os.getenv("LEADS_CONVOS_BASE") 
        CONVERSATIONS_TABLE = os.getenv("CONVERSATIONS_TABLE", "Conversations")
        
        convos = Table(AIRTABLE_API_KEY, BASE_ID, CONVERSATIONS_TABLE)
        
        # Get records that have Response Time data
        records = convos.all()
        
        response_time_examples = []
        for record in records:
            fields = record.get("fields", {})
            response_time = fields.get("Response Time (Minutes)")
            if response_time is not None:
                response_time_examples.append({
                    "record_id": record.get("id"),
                    "value": response_time,
                    "type": type(response_time).__name__,
                    "content": str(response_time)[:200] + "..." if len(str(response_time)) > 200 else str(response_time)
                })
                if len(response_time_examples) >= 3:  # Get first 3 examples
                    break
        
        print(f"✅ Found {len(response_time_examples)} examples of Response Time (Minutes):")
        for i, example in enumerate(response_time_examples, 1):
            print(f"\n  Example {i}:")
            print(f"    Record ID: {example['record_id']}")
            print(f"    Type: {example['type']}")
            print(f"    Content: {example['content']}")
                
    except Exception as e:
        print(f"❌ Check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_response_time_field()