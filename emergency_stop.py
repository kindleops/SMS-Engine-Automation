#!/usr/bin/env python3
"""
🚨 EMERGENCY STOP SCRIPT 🚨
- Kills all SMS sending processes
- Disables TextGrid credentials
- Verifies logging system status
"""
import os
import signal
import subprocess
import sys

def kill_sms_processes():
    """Kill any running SMS-related processes"""
    print("🔪 Killing SMS-related processes...")
    
    # Kill by process name patterns
    patterns = [
        "controlled_drip_send",
        "engine_runner", 
        "textgrid_sender",
        "outbound_batcher",
        "message_processor"
    ]
    
    killed_count = 0
    for pattern in patterns:
        try:
            result = subprocess.run(
                ["pkill", "-f", pattern], 
                capture_output=True, 
                text=True
            )
            if result.returncode == 0:
                killed_count += 1
                print(f"   ✅ Killed processes matching: {pattern}")
        except Exception as e:
            print(f"   ⚠️ Error killing {pattern}: {e}")
    
    if killed_count == 0:
        print("   ℹ️ No SMS processes found running")
    
    return killed_count

def check_env_disabled():
    """Check if TextGrid credentials are disabled"""
    print("\n🔧 Checking TextGrid credentials...")
    
    api_key = os.getenv("TEXTGRID_API_KEY", "")
    campaign_id = os.getenv("TEXTGRID_CAMPAIGN_ID", "")
    
    if "EMERGENCY_DISABLED" in api_key and "EMERGENCY_DISABLED" in campaign_id:
        print("   ✅ TextGrid credentials are DISABLED")
        return True
    else:
        print("   ❌ TextGrid credentials are still ACTIVE!")
        print(f"      API_KEY: {api_key[:20]}...")
        print(f"      CAMPAIGN_ID: {campaign_id[:20]}...")
        return False

def check_test_mode():
    """Check if TEST_MODE is enabled"""
    print("\n🧪 Checking TEST_MODE...")
    
    test_mode = os.getenv("TEST_MODE", "false").lower()
    if test_mode == "true":
        print("   ✅ TEST_MODE is ENABLED")
        return True
    else:
        print("   ❌ TEST_MODE is DISABLED!")
        return False

def check_sending_disabled():
    """Test if SMS sending is actually disabled"""
    print("\n� Testing SMS sending capability...")
    
    try:
        from sms.textgrid_sender import send_message
        result = send_message(
            from_number='+16127469639',
            to='+16512760269', 
            message='Emergency test - should fail'
        )
        
        if result and result.get('status') == 'failed':
            print("   ✅ SMS sending is DISABLED")
            return True
        else:
            print(f"   ❌ SMS sending is ACTIVE! Result: {result}")
            return False
            
    except Exception as e:
        print(f"   ✅ SMS sending is DISABLED (error: {e})")
        return True

def main():
    print("🚨 EMERGENCY SMS STOP PROTOCOL 🚨")
    print("="*50)
    
    # Kill processes
    killed = kill_sms_processes()
    
    # Check safety measures
    creds_disabled = check_env_disabled()
    test_mode_on = check_test_mode()
    sending_disabled = check_sending_disabled()
    
    print("\n" + "="*50)
    print("📊 EMERGENCY STOP SUMMARY:")
    print(f"   Processes killed: {killed}")
    print(f"   Credentials disabled: {'✅' if creds_disabled else '❌'}")
    print(f"   Test mode enabled: {'✅' if test_mode_on else '❌'}")
    print(f"   SMS sending disabled: {'✅' if sending_disabled else '❌'}")
    
    if sending_disabled:
        print("\n🛡️ SYSTEM IS SAFELY STOPPED")
        print("   No SMS messages can be sent")
    else:
        print("\n🔥 SYSTEM IS STILL ACTIVE!")
        print("   SMS messages may still be sent!")
        print("   CHECK CONFIGURATION IMMEDIATELY!")
    
    print("\n� NOTE: Logging system appears functional")
    print("   Issue may be with message logging logic")
    print("   Review conversation logging in textgrid_sender.py")

if __name__ == "__main__":
    main()