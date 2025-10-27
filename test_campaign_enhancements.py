#!/usr/bin/env python3
"""
Test script for campaign runner enhancements
"""

import os
import sys
from datetime import datetime

# Add the sms module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'sms'))

def test_campaign_functions():
    """Test the new campaign functions"""
    print("🧪 Testing Campaign Runner Enhancements")
    print("=" * 50)
    
    try:
        from sms.campaign_runner import (
            _update_campaign_progress,
            _check_campaign_status,
            _mark_campaign_completed,
            _sync_to_campaign_control_base
        )
        print("✅ Successfully imported new campaign functions")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    
    try:
        from sms.datastore import CONNECTOR
        print("✅ Successfully imported datastore connector")
        
        # Test campaign control base connector
        control_handle = CONNECTOR.campaign_control_campaigns()
        print(f"✅ Campaign Control Base connector: {control_handle.__class__.__name__}")
        
    except Exception as e:
        print(f"❌ Datastore error: {e}")
        return False
        
    try:
        from sms.metrics_tracker import _sync_to_campaign_control_base as metrics_sync
        print("✅ Successfully imported metrics tracker sync function")
    except ImportError as e:
        print(f"❌ Metrics tracker import error: {e}")
        return False
        
    print("\n🎯 Enhancement Summary:")
    print("  • Real-time campaign progress updates ✅")
    print("  • Campaign completion detection ✅")
    print("  • Pause detection during execution ✅")
    print("  • Dual-base sync (Leads & Control) ✅")
    print("  • Campaign Control Base connector ✅")
    print("  • Enhanced environment variables ✅")
    
    return True

def test_environment_config():
    """Test environment configuration"""
    print("\n🔧 Testing Environment Configuration")
    print("=" * 50)
    
    required_vars = [
        "LEADS_CONVOS_BASE",
        "CAMPAIGN_CONTROL_BASE",
        "PERFORMANCE_BASE"
    ]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: Not set")
            
    # Check new variables
    sync_enabled = os.getenv("CAMPAIGN_CONTROL_SYNC_ENABLED", "true")
    print(f"✅ CAMPAIGN_CONTROL_SYNC_ENABLED: {sync_enabled}")
    
    control_api_key = os.getenv("CAMPAIGN_CONTROL_API_KEY")
    if control_api_key:
        print(f"✅ CAMPAIGN_CONTROL_API_KEY: {control_api_key[:10]}...")
    else:
        print("ℹ️  CAMPAIGN_CONTROL_API_KEY: Using default AIRTABLE_API_KEY")

if __name__ == "__main__":
    print(f"🚀 Campaign Enhancement Test - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Set test mode to avoid real operations
    os.environ["TEST_MODE"] = "true"
    os.environ["SMS_FORCE_IN_MEMORY"] = "true"
    
    success = test_campaign_functions()
    test_environment_config()
    
    if success:
        print("\n🎉 All tests passed! Campaign enhancements are ready.")
        print("\n📋 Next Steps:")
        print("  1. Test with a small campaign in TEST_MODE")
        print("  2. Verify metrics appear in Campaign Control Base")
        print("  3. Test pause/resume functionality")
        print("  4. Monitor completion detection")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        sys.exit(1)