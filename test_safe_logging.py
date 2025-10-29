#!/usr/bin/env python3
"""
Test script to verify safe_log_conversation works with timeout protection.
"""

import os
import sys
import time
from unittest.mock import Mock, patch

# Add project root to path so we can import sms modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_timeout_protection():
    """Test that timeout protection works properly."""
    print("🧪 Testing timeout protection for safe_log_conversation...")
    
    # Import after path setup
    from sms.inbound_webhook import safe_log_conversation, timeout_context
    
    # Test 1: Test timeout context directly
    print("\n1️⃣ Testing timeout context...")
    try:
        with timeout_context(1):  # 1 second timeout
            print("   Sleeping for 2 seconds (should timeout)...")
            time.sleep(2)
        print("   ❌ FAILED: Should have timed out!")
    except TimeoutError as e:
        print(f"   ✅ SUCCESS: Caught timeout as expected: {e}")
    except Exception as e:
        print(f"   ⚠️ Unexpected error: {e}")
    
    # Test 2: Test with mock Airtable that hangs
    print("\n2️⃣ Testing safe_log_conversation with hanging Airtable...")
    
    # Mock the convos table to simulate hanging
    def hanging_create(data):
        print("   Simulating hanging Airtable operation...")
        time.sleep(5)  # Hang for 5 seconds
        return {"id": "fake_id"}
    
    mock_convos = Mock()
    mock_convos.create = hanging_create
    
    with patch('sms.inbound_webhook.convos', mock_convos):
        test_payload = {
            "TextGrid ID": "SMIOv7MB7dIQDBtIjPsAinpHA==",
            "Message": "Test message",
            "Direction": "INBOUND",
            "Seller Phone Number": "+1234567890"
        }
        
        start_time = time.time()
        result = safe_log_conversation(test_payload, timeout_seconds=2)
        elapsed = time.time() - start_time
        
        print(f"   Result: {result}")
        print(f"   Elapsed time: {elapsed:.2f} seconds")
        
        if not result and elapsed < 3:  # Should fail quickly due to timeout
            print("   ✅ SUCCESS: Timeout protection worked!")
        else:
            print("   ❌ FAILED: Timeout protection didn't work properly")
    
    # Test 3: Test with normal Airtable operation (mocked success)
    print("\n3️⃣ Testing safe_log_conversation with successful Airtable...")
    
    def quick_create(data):
        return {"id": "successful_id"}
    
    mock_convos = Mock()
    mock_convos.create = quick_create
    
    with patch('sms.inbound_webhook.convos', mock_convos):
        test_payload = {
            "TextGrid ID": "SMIOv7MB7dIQDBtIjPsAinpHA==",
            "Message": "Test message", 
            "Direction": "INBOUND",
            "Seller Phone Number": "+1234567890"
        }
        
        result = safe_log_conversation(test_payload)
        
        if result:
            print("   ✅ SUCCESS: Normal operation worked!")
        else:
            print("   ❌ FAILED: Normal operation should have succeeded")

if __name__ == "__main__":
    test_timeout_protection()
    print("\n🎯 Test completed!")