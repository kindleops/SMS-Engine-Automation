#!/usr/bin/env python3
"""
Debug the IDEM.seen() method to understand why it's always returning True
"""
import os
import sys
import time
import random
import requests
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def debug_upstash_directly():
    """Test Upstash REST API directly to understand the behavior"""
    
    UPSTASH_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
    UPSTASH_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
    
    if not UPSTASH_REST_URL or not UPSTASH_REST_TOKEN:
        print("❌ Missing Upstash credentials")
        return
    
    print("🔍 Testing Upstash REST API directly...")
    print(f"🌐 URL: {UPSTASH_REST_URL}")
    print(f"🔑 Token: {UPSTASH_REST_TOKEN[:20]}...")
    
    # Generate unique test key
    test_timestamp = int(time.time() * 1000)
    test_key = f"inbound:msg:DEBUG-TEST-{test_timestamp}"
    
    print(f"🗝️ Test key: {test_key}")
    
    try:
        # Test SET with NX (should succeed for new key)
        print("\n1️⃣ Testing SET with NX for new key...")
        resp = requests.post(
            UPSTASH_REST_URL,
            headers={"Authorization": f"Bearer {UPSTASH_REST_TOKEN}"},
            json=["SET", test_key, "1", "EX", "86400", "NX"],  # Fixed: array format instead of command object
            timeout=5,
        )
        
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.text}")
        
        if resp.ok:
            data = resp.json()
            print(f"   JSON: {data}")
            result = data.get("result")
            print(f"   Result: {result}")
            print(f"   Should be duplicate? {result != 'OK'}")
        else:
            print(f"   ❌ Request failed: {resp.status_code}")
        
        # Test SET with NX again (should fail for existing key)
        print("\n2️⃣ Testing SET with NX for existing key...")
        resp2 = requests.post(
            UPSTASH_REST_URL,
            headers={"Authorization": f"Bearer {UPSTASH_REST_TOKEN}"},
            json=["SET", test_key, "1", "EX", "86400", "NX"],  # Fixed: array format
            timeout=5,
        )
        
        print(f"   Status: {resp2.status_code}")
        print(f"   Response: {resp2.text}")
        
        if resp2.ok:
            data2 = resp2.json()
            print(f"   JSON: {data2}")
            result2 = data2.get("result")
            print(f"   Result: {result2}")
            print(f"   Should be duplicate? {result2 != 'OK'}")
        
        # Clean up
        print("\n3️⃣ Cleaning up test key...")
        resp3 = requests.post(
            UPSTASH_REST_URL,
            headers={"Authorization": f"Bearer {UPSTASH_REST_TOKEN}"},
            json=["DEL", test_key],  # Fixed: array format
            timeout=5,
        )
        print(f"   Cleanup result: {resp3.text if resp3.ok else 'Failed'}")
        
    except Exception as e:
        print(f"❌ Error testing Upstash: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_upstash_directly()