"""
Debugging version of webhook handler to identify hanging issues
"""
import os
import time
import traceback
from typing import Any, Dict, Optional

def debug_webhook_handler(payload: dict, max_execution_time: int = 30):
    """Debug version of webhook handler with step-by-step timing"""
    start_time = time.time()
    
    def check_timeout(step_name: str):
        elapsed = time.time() - start_time
        print(f"⏱️ {step_name}: {elapsed:.2f}s elapsed")
        if elapsed > max_execution_time:
            raise TimeoutError(f"Handler timed out after {elapsed:.2f}s at step: {step_name}")
    
    try:
        print(f"🚀 Starting webhook handler with payload keys: {list(payload.keys())}")
        check_timeout("Initial setup")
        
        # Step 1: Basic validation
        from_number = payload.get("From")
        to_number = payload.get("To") 
        body = payload.get("Body")
        msg_id = payload.get("MessageSid") or payload.get("TextGridId")
        
        print(f"📞 From: {from_number}, To: {to_number}, MsgID: {msg_id}")
        print(f"💬 Body: {body[:50] if body else 'None'}...")
        check_timeout("Basic validation")
        
        if not from_number or not body:
            print(f"❌ Missing required fields - From: {bool(from_number)}, Body: {bool(body)}")
            return {"error": "Missing required fields"}
        
        # Step 2: Import checks (potential hanging point)
        print("🔄 Importing modules...")
        try:
            from sms.airtable_schema import conversations_field_map
            check_timeout("Import airtable_schema")
            
            # Import other modules that might hang
            from pyairtable import Table
            check_timeout("Import pyairtable")
            
        except Exception as e:
            print(f"❌ Import error: {e}")
            return {"error": f"Import failed: {e}"}
        
        # Step 3: Airtable connection test
        print("🔗 Testing Airtable connection...")
        try:
            AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
            BASE_ID = os.getenv("LEADS_CONVOS_BASE") or os.getenv("AIRTABLE_LEADS_CONVOS_BASE_ID")
            
            if not AIRTABLE_API_KEY or not BASE_ID:
                print(f"❌ Missing Airtable config - API_KEY: {bool(AIRTABLE_API_KEY)}, BASE_ID: {bool(BASE_ID)}")
                return {"error": "Missing Airtable config"}
            
            # Try to create table instance (might hang here)
            conversations_table = Table(AIRTABLE_API_KEY, BASE_ID, "Conversations")
            check_timeout("Airtable table creation")
            
            # Quick test - try to get first record (with timeout)
            print("🔍 Testing Airtable read...")
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Airtable read timed out")
            
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(5)  # 5 second timeout for Airtable test
            
            try:
                # Test read with limit
                records = conversations_table.all(max_records=1, view="Grid view")
                print(f"✅ Airtable test successful, got {len(records)} records")
            except Exception as e:
                print(f"⚠️ Airtable read test failed: {e}")
            finally:
                signal.alarm(0)  # Cancel alarm
            
            check_timeout("Airtable connectivity test")
            
        except Exception as e:
            print(f"❌ Airtable test error: {e}")
            traceback.print_exc()
            return {"error": f"Airtable test failed: {e}"}
        
        # If we get here, the basic functions are working
        elapsed = time.time() - start_time
        print(f"✅ Debug handler completed successfully in {elapsed:.2f}s")
        
        return {
            "status": "debug_success",
            "elapsed_time": elapsed,
            "from": from_number,
            "to": to_number,
            "msg_id": msg_id,
            "body_length": len(body) if body else 0
        }
        
    except TimeoutError as e:
        elapsed = time.time() - start_time
        print(f"⏰ Timeout: {e} (total time: {elapsed:.2f}s)")
        return {"error": str(e), "elapsed_time": elapsed}
    
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Unexpected error after {elapsed:.2f}s: {e}")
        traceback.print_exc()
        return {"error": str(e), "elapsed_time": elapsed}

if __name__ == "__main__":
    # Test locally
    test_payload = {
        "MessageSid": "TEST123",
        "From": "+15551234567", 
        "To": "+19725551234",
        "Body": "Test message"
    }
    
    print("Testing debug webhook handler locally...")
    result = debug_webhook_handler(test_payload)
    print(f"Result: {result}")