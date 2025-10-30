#!/usr/bin/env python3
"""Debug quiet hours and from number issues."""

import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, '/Users/ryankindle/Desktop/Projects/REI Automation - SMS Engine/rei-sms-engine-1')

def debug_quiet_hours_and_numbers():
    """Debug quiet hours enforcement and from number availability."""
    try:
        from sms.outbound_batcher import is_quiet_hours_local, _pick_number_for_market, get_numbers
        from sms import settings
        
        print("🔍 Debugging Quiet Hours and From Number Issues...")
        
        # Check quiet hours configuration
        print("\n📅 QUIET HOURS CONFIGURATION:")
        env_settings = settings()
        print(f"   QUIET_HOURS_ENFORCED: {env_settings.QUIET_HOURS_ENFORCED}")
        print(f"   QUIET_START_HOUR: {env_settings.QUIET_START_HOUR}")
        print(f"   QUIET_END_HOUR: {env_settings.QUIET_END_HOUR}")
        print(f"   Current timezone: {env_settings.QUIET_TZ}")
        
        # Check current time vs quiet hours
        is_quiet = is_quiet_hours_local()
        print(f"   Currently in quiet hours: {is_quiet}")
        current_time = datetime.now()
        print(f"   Current time: {current_time}")
        
        # Check available numbers
        print("\n📞 FROM NUMBER CONFIGURATION:")
        try:
            numbers = get_numbers()
            if numbers:
                print(f"   Available numbers from Campaign Control Base: {len(numbers)}")
                for num_record in numbers:
                    fields = num_record.get("fields", {})
                    number = fields.get("Number", "N/A")
                    market = fields.get("Market", "N/A")
                    print(f"     • {number} (Market: {market})")
            else:
                print("   ❌ No numbers found in Campaign Control Base")
        except Exception as e:
            print(f"   ❌ Error accessing numbers table: {e}")
        
        # Test number picking for common markets
        print("\n🎯 TESTING NUMBER SELECTION:")
        test_markets = ["Houston", "Miami", "Charlotte", "Default"]
        for market in test_markets:
            try:
                selected_number = _pick_number_for_market(market)
                print(f"   Market '{market}': {selected_number if selected_number else '❌ No number found'}")
            except Exception as e:
                print(f"   Market '{market}': ❌ Error - {e}")
        
        # Check environment variables
        print("\n🔧 ENVIRONMENT VARIABLES:")
        print(f"   AUTO_BACKFILL_FROM_NUMBER: {os.getenv('AUTO_BACKFILL_FROM_NUMBER', 'not set')}")
        print(f"   DEFAULT_FROM_NUMBER: {os.getenv('DEFAULT_FROM_NUMBER', 'not set')}")
        print(f"   AIRTABLE_COMPLIANCE_KEY: {'set' if os.getenv('AIRTABLE_COMPLIANCE_KEY') else 'not set'}")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        if env_settings.QUIET_HOURS_ENFORCED:
            print("   ✅ Quiet hours are properly enforced")
        else:
            print("   ⚠️  QUIET_HOURS_ENFORCED=false - messages will send during quiet hours!")
            print("       → Set QUIET_HOURS_ENFORCED=true to fix this")
            
        try:
            numbers = get_numbers()
            if not numbers:
                print("   ⚠️  No numbers available from Campaign Control Base")
                print("       → Check AIRTABLE_COMPLIANCE_KEY configuration")
                print("       → Verify Numbers table has records")
            else:
                print("   ✅ Numbers are available from Campaign Control Base")
        except Exception as e:
            print(f"   ❌ Numbers table access error: {e}")
            print("       → Check AIRTABLE_COMPLIANCE_KEY and permissions")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in debugging: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_quiet_hours_and_numbers()
    exit(0 if success else 1)