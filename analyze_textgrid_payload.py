#!/usr/bin/env python3
"""
Debug script to capture and analyze real TextGrid webhook payloads
"""

import json
import sys
from datetime import datetime

def analyze_textgrid_payload():
    """Analyze what TextGrid actually sends"""
    
    # Example of what we know TextGrid sends based on user feedback
    real_textgrid_example = {
        "TextGridId": "SMIOv7MB7dIQDBtIjPsAinpHA==",
        "From": "+15551234567", 
        "To": "+19725551234",
        "Body": "Hello, I want to sell my house"
    }
    
    print("🔍 TEXTGRID PAYLOAD ANALYSIS")
    print("=" * 50)
    print(f"📅 Analysis Time: {datetime.now()}")
    print()
    
    print("🎯 REAL TEXTGRID EXAMPLE:")
    for key, value in real_textgrid_example.items():
        print(f"  {key}: {value}")
    print()
    
    print("🔧 CURRENT EXTRACTION LOGIC:")
    print("  msg_id = payload.get('MessageSid') or payload.get('TextGridId')")
    print()
    
    print("✅ EXPECTED BEHAVIOR:")
    print("  - TextGrid sends 'TextGridId' field")
    print("  - Value should be base64-encoded string")
    print("  - This should be captured and logged to Airtable")
    print()
    
    print("🚨 ISSUE DIAGNOSIS:")
    print("  - We disabled Airtable logging due to performance issues")
    print("  - Need to re-enable with better error handling")
    print("  - TextGrid ID extraction logic is correct")
    print()
    
    # Test the extraction logic
    msg_id = real_textgrid_example.get("MessageSid") or real_textgrid_example.get("TextGridId")
    print(f"🔬 EXTRACTION TEST:")
    print(f"  Extracted ID: {msg_id}")
    print(f"  Type: {type(msg_id)}")
    print(f"  Length: {len(msg_id) if msg_id else 'None'}")
    
if __name__ == "__main__":
    analyze_textgrid_payload()