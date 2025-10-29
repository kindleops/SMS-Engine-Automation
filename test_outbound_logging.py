#!/usr/bin/env python3
"""
Test outbound conversation logging by calling the outbound batcher directly
"""

import sys
import json
from sms.outbound_batcher import send_batch

print("🚀 Testing outbound conversation logging...")

try:
    # Run the outbound batcher to process any pending messages
    result = send_batch(limit=1)  # Process only 1 message to keep test focused
    
    print("\n✅ Outbound batch result:")
    print(json.dumps(result, indent=2, default=str))
    
    if result.get("total_sent", 0) > 0:
        print("\n🎉 Messages were sent! Check your conversations table for new outbound records.")
    else:
        print("\n💭 No messages were sent. This could mean:")
        print("   • No pending messages in drip queue")
        print("   • Messages are in quiet hours")
        print("   • All eligible messages already sent")
        print("   • Rate limits preventing sends")
        
except Exception as e:
    print(f"\n❌ Error running outbound batch: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)