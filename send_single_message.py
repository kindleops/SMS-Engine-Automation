#!/usr/bin/env python3
"""
Quick script to send a single text message
"""
import os
import sys
from sms.textgrid_sender import send_message

def send_text_message(from_number, to_number, message):
    """Send a single text message"""
    print(f"🔄 Sending message...")
    print(f"   From: {from_number}")
    print(f"   To: {to_number}")
    print(f"   Message: {message}")
    
    try:
        # Send the message using the textgrid_sender module
        result = send_message(
            from_number=from_number,
            to=to_number,
            message=message
        )
        
        if result and (result.get('status') == 'sent' or result.get('success')):
            print(f"✅ Message sent successfully!")
            print(f"   TextGrid ID: {result.get('sid', result.get('textgrid_id', 'N/A'))}")
            print(f"   Status: {result.get('status', 'N/A')}")
            return True
        else:
            print(f"❌ Failed to send message: {result}")
            return False
            
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        return False

if __name__ == "__main__":
    # Message details - Tony response message (using default SMS number)
    from_number = "+18329063669"  # Default SMS-enabled number from .env
    to_number = "+19043868085"    # To number  
    message = "Thanks for your response Tony. I am not looking to waste your time and am a serious investor. Are you open to an offer on the property?"
    
    print(f"📱 Attempting to send from {from_number} to {to_number}")
    print(f"📝 Message: {message}")
    print(f"⚠️  Note: Using default SMS number instead of +19045124091 due to blacklist rule")
    
    # Send the message
    success = send_text_message(from_number, to_number, message)
    
    if success:
        print("\n🎉 Message delivery initiated successfully!")
    else:
        print("\n💥 Message delivery failed!")
        print("   Note: If the number is not SMS-capable, it may need to be")
        print("   added to your TextGrid account or provisioned for SMS.")
        sys.exit(1)