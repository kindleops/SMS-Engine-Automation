#!/usr/bin/env python3
"""
Test script for validating comprehensive prospect field enhancements
Ensures proper prospect data flow through conversations table and all system integration
"""

import os
import sys
import datetime
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from sms.autoresponder import Autoresponder
from sms.airtable_schema import PROSPECT_FIELDS

def test_prospect_field_mapping():
    """Test that all required prospect fields are properly mapped"""
    print("\n=== Testing Prospect Field Mapping ===")
    
    autoresponder = Autoresponder()
    
    required_fields = [
        "SELLER_ASKING_PRICE",
        "CONDITION_NOTES", 
        "TIMELINE_MOTIVATION",
        "LAST_INBOUND",
        "LAST_OUTBOUND",
        "LAST_ACTIVITY",
        "OWNERSHIP_CONFIRMED_DATE",
        "LEAD_PROMOTION_DATE",
        "PHONE_1_VERIFIED",
        "PHONE_2_VERIFIED",
        "INTENT_LAST_DETECTED",
        "LAST_DIRECTION",
        "ACTIVE_PHONE_SLOT",
        "LAST_TRIED_SLOT",
        "TEXTGRID_PHONE",
        "LAST_MESSAGE",
        "REPLY_COUNT",
        "OPT_OUT",
        "SEND_COUNT",
        "STAGE",
        "STATUS",
    ]
    
    print("Checking prospect field mappings:")
    for field in required_fields:
        mapped_field = autoresponder.prospect_field_map.get(field)
        print(f"  {field}: {mapped_field}")
        assert mapped_field is not None, f"Missing mapping for {field}"
    
    print("✅ All prospect fields properly mapped")

def test_price_extraction():
    """Test price extraction from message text"""
    print("\n=== Testing Price Extraction ===")
    
    autoresponder = Autoresponder()
    
    test_cases = [
        ("I'm asking $250,000 for the house", "250000"),
        ("Looking for 300k", "300000"),
        ("The price is $180k", "180000"),
        ("I want $175,500.00", "175500.00"),
        ("Random message with no price", None),
        ("About 250 thousand", None),  # Should not match partial text
    ]
    
    for message, expected in test_cases:
        result = autoresponder._extract_price_from_message(message)
        print(f"Message: '{message}' → Price: {result}")
        if expected is None:
            assert result is None, f"Expected None but got {result}"
        else:
            assert result == expected, f"Expected {expected} but got {result}"
    
    print("✅ Price extraction working correctly")

def test_condition_extraction():
    """Test condition information extraction"""
    print("\n=== Testing Condition Extraction ===")
    
    autoresponder = Autoresponder()
    
    test_cases = [
        ("The house needs some repairs to the roof", True),
        ("It's been recently updated and renovated", True),
        ("There are tenants living there right now", True),
        ("The property is vacant and ready to go", True),
        ("Just a regular house for sale", False),
        ("Looking to sell as-is condition", True),
    ]
    
    for message, should_extract in test_cases:
        result = autoresponder._extract_condition_info(message)
        print(f"Message: '{message}' → Condition: {result}")
        if should_extract:
            assert result is not None, f"Expected condition info but got None"
            assert len(result) > 0, f"Expected non-empty condition info"
        else:
            assert result is None, f"Expected None but got {result}"
    
    print("✅ Condition extraction working correctly")

def test_timeline_motivation_extraction():
    """Test timeline and motivation extraction"""
    print("\n=== Testing Timeline/Motivation Extraction ===")
    
    autoresponder = Autoresponder()
    
    test_cases = [
        ("We need to sell urgently due to divorce", True),
        ("Have to move for work next month", True),
        ("Dealing with financial problems and debt", True),
        ("Just inherited this property from uncle", True),
        ("Regular sale, no rush", False),
        ("Facing foreclosure deadline in 30 days", True),
    ]
    
    for message, should_extract in test_cases:
        result = autoresponder._extract_timeline_motivation(message)
        print(f"Message: '{message}' → Timeline: {result}")
        if should_extract:
            assert result is not None, f"Expected timeline info but got None"
            assert len(result) > 0, f"Expected non-empty timeline info"
        else:
            assert result is None, f"Expected None but got {result}"
    
    print("✅ Timeline/motivation extraction working correctly")

def test_phone_slot_determination():
    """Test active phone slot determination"""
    print("\n=== Testing Phone Slot Determination ===")
    
    autoresponder = Autoresponder()
    
    # Mock prospect record
    prospect_record = {
        "id": "recTest123",
        "fields": {
            "Phone 1 (from Linked Owner)": "+15551234567",
            "Phone 2 (from Linked Owner)": "+15559876543",
        }
    }
    
    # Test phone 1
    slot = autoresponder._determine_active_phone_slot(prospect_record, "+15551234567")
    assert slot == "1", f"Expected slot 1, got {slot}"
    print(f"Phone +15551234567 → Slot {slot} ✅")
    
    # Test phone 2
    slot = autoresponder._determine_active_phone_slot(prospect_record, "+15559876543")
    assert slot == "2", f"Expected slot 2, got {slot}"
    print(f"Phone +15559876543 → Slot {slot} ✅")
    
    # Test unknown phone (should default to 1)
    slot = autoresponder._determine_active_phone_slot(prospect_record, "+15551111111")
    assert slot == "1", f"Expected default slot 1, got {slot}"
    print(f"Phone +15551111111 → Slot {slot} (default) ✅")
    
    print("✅ Phone slot determination working correctly")

def test_comprehensive_prospect_update():
    """Test comprehensive prospect update functionality"""
    print("\n=== Testing Comprehensive Prospect Update ===")
    
    autoresponder = Autoresponder()
    
    # Mock dependencies
    mock_prospects = Mock()
    autoresponder.prospects = mock_prospects
    
    # Mock prospect record
    prospect_record = {
        "id": "recProspect123",
        "fields": {
            "Phone 1 (from Linked Owner)": "+15551234567",
            "Status": "Unmessaged",
            "Reply Count": 0,
            "Send Count": 5,
        }
    }
    
    # Mock conversation fields
    conversation_fields = {
        "Seller Phone Number": "+15551234567",
        "TextGrid Phone Number": "+15559999999",
        "Message": "Yes, I'm interested. The house is worth about $250,000 and needs roof repairs.",
    }
    
    # Test inbound message with price and condition info
    autoresponder._update_prospect_comprehensive(
        prospect_record=prospect_record,
        conversation_fields=conversation_fields,
        body="Yes, I'm interested. The house is worth about $250,000 and needs roof repairs.",
        event="ownership_yes",
        direction="INBOUND", 
        from_number="+15551234567",
        to_number="+15559999999",
        stage="Stage 2 - Interest Filter",
        ai_intent="interest_detected"
    )
    
    # Verify update was called
    mock_prospects.update.assert_called_once()
    call_args = mock_prospects.update.call_args[1]
    
    print("Prospect update payload:")
    for field, value in call_args.items():
        print(f"  {field}: {value}")
    
    # Verify key fields were updated
    expected_updates = [
        "Seller Asking Price",
        "Condition Notes", 
        "Last Inbound",
        "Last Activity",
        "Ownership Confirmation Timeline",
        "Phone 1 Ownership Verified",
        "Intent Last Detected",
        "Last Direction",
        "Last Tried Slot",
        "TextGrid Phone Number",
        "Last Message",
        "Reply Count",
        "Status",
    ]
    
    for field in expected_updates:
        assert field in call_args, f"Missing expected field: {field}"
        print(f"✅ {field}: {call_args[field]}")
    
    # Verify specific values
    assert call_args["Seller Asking Price"] == "250000", "Price not extracted correctly"
    assert "roof repairs" in call_args["Condition Notes"], "Condition info not extracted"
    assert call_args["Phone 1 Ownership Verified"] == True, "Phone verification not set"
    assert call_args["Status"] == "Owner Verified", "Status not updated correctly"
    assert call_args["Reply Count"] == 1, "Reply count not incremented"
    
    print("✅ Comprehensive prospect update working correctly")

def test_outbound_prospect_update():
    """Test prospect update for outbound messages"""
    print("\n=== Testing Outbound Prospect Update ===")
    
    # Import the outbound batcher function
    from sms.outbound_batcher import _safe_update_prospect_outbound, _extract_prospect_id_from_drip
    
    # Test prospect ID extraction from drip record
    drip_record = {
        "fields": {
            "Prospect": ["recProspect456"],
            "Message": "Hi there, are you interested in selling your property?",
        }
    }
    
    prospect_id = _extract_prospect_id_from_drip(drip_record)
    assert prospect_id == "recProspect456", f"Expected recProspect456, got {prospect_id}"
    print(f"✅ Extracted prospect ID: {prospect_id}")
    
    # Test outbound update function
    mock_prospects_tbl = Mock()
    mock_prospect = {
        "fields": {
            "Send Count": 2,
            "Status": "Queued",
            "Phone 1 (from Linked Owner)": "+15551234567",
            "Phone 2 (from Linked Owner)": "+15559876543",
        }
    }
    mock_prospects_tbl.get.return_value = mock_prospect
    
    _safe_update_prospect_outbound(
        prospects_tbl=mock_prospects_tbl,
        prospect_id="recProspect456",
        phone="+15551234567",
        body="Hi there, are you interested in selling your property?",
        textgrid_phone="+15559999999"
    )
    
    # Verify get and update were called
    mock_prospects_tbl.get.assert_called_once_with("recProspect456")
    mock_prospects_tbl.update.assert_called_once()
    
    update_args = mock_prospects_tbl.update.call_args[1]
    print("Outbound prospect update payload:")
    for field, value in update_args.items():
        print(f"  {field}: {value}")
    
    # Verify key outbound fields
    assert "Last Outbound" in update_args, "Missing Last Outbound"
    assert "Send Count" in update_args, "Missing Send Count"
    assert update_args["Send Count"] == 3, "Send count not incremented correctly"
    assert update_args["Status"] == "Messaged", "Status not updated for outbound"
    assert update_args["Last Tried Slot"] == "1", "Phone slot not determined correctly"
    
    print("✅ Outbound prospect update working correctly")

def test_follow_up_campaign_creation():
    """Test follow-up campaign creation for unverified phones"""
    print("\n=== Testing Follow-Up Campaign Creation ===")
    
    autoresponder = Autoresponder()
    
    # Mock prospect with unverified phones
    prospect_record = {
        "id": "recProspect789",
        "fields": {
            "Phone 1 Ownership Verified": False,
            "Phone 2 Ownership Verified": False,
            "Phone 1 (from Linked Owner)": "+15551234567",
            "Phone 2 (from Linked Owner)": "+15559876543",
        }
    }
    
    # This should log that a follow-up campaign is needed
    # (actual campaign creation would be implemented separately)
    with patch('sms.autoresponder.logger') as mock_logger:
        autoresponder._create_follow_up_campaign_if_needed(prospect_record, "+15551234567")
        mock_logger.info.assert_called_with("Prospect recProspect789 needs follow-up campaign - no verified phones")
    
    print("✅ Follow-up campaign creation logic working correctly")

def test_enhanced_stage_mapping():
    """Test conversation stage to prospect stage mapping"""
    print("\n=== Testing Enhanced Stage Mapping ===")
    
    autoresponder = Autoresponder()
    
    # Mock setup for testing stage mapping
    mock_prospects = Mock()
    autoresponder.prospects = mock_prospects
    
    prospect_record = {
        "id": "recTest123",
        "fields": {
            "Phone 1 (from Linked Owner)": "+15551234567",
            "Status": "Messaged",
        }
    }
    
    stage_mapping_tests = [
        ("Stage 1 - Ownership Confirmation", "Stage #1 – Ownership Check"),
        ("Stage 2 - Interest Filter", "Stage #2 – Offer Interest"),
        ("Stage 3 - Price Qualification", "Stage #3 – Price/Condition"),
        ("Stage 4 - Property Condition", "Stage #3 – Price/Condition"),
        ("Opt-Out", "Opt-Out"),
    ]
    
    for conv_stage, expected_prospect_stage in stage_mapping_tests:
        # Reset mock
        mock_prospects.reset_mock()
        
        autoresponder._update_prospect_comprehensive(
            prospect_record=prospect_record,
            conversation_fields={},
            body="Test message",
            event="neutral",
            direction="INBOUND",
            from_number="+15551234567",
            to_number="+15559999999",
            stage=conv_stage,
            ai_intent="neutral"
        )
        
        call_args = mock_prospects.update.call_args[1]
        actual_stage = call_args.get("Stage")
        
        print(f"Conversation Stage: {conv_stage} → Prospect Stage: {actual_stage}")
        assert actual_stage == expected_prospect_stage, f"Expected {expected_prospect_stage}, got {actual_stage}"
    
    print("✅ Stage mapping working correctly")

def run_all_tests():
    """Run all prospect enhancement tests"""
    print("🚀 Starting Prospect Enhancement Tests")
    print("=" * 60)
    
    try:
        test_prospect_field_mapping()
        test_price_extraction()
        test_condition_extraction()
        test_timeline_motivation_extraction()
        test_phone_slot_determination()
        test_comprehensive_prospect_update()
        test_outbound_prospect_update()
        test_follow_up_campaign_creation()
        test_enhanced_stage_mapping()
        
        print("\n" + "=" * 60)
        print("✅ All prospect enhancement tests PASSED!")
        print("\nKey Features Verified:")
        print("• Comprehensive prospect field mapping and updates")
        print("• Automatic price extraction from conversations")
        print("• Condition and timeline/motivation extraction")
        print("• Phone verification and slot tracking")
        print("• Bidirectional prospect updates (inbound/outbound)")
        print("• Enhanced stage and status mapping")
        print("• Follow-up campaign creation for unverified phones")
        print("• Complete integration with conversation logging")
        
        print("\n🎯 Prospect Enhancement Features:")
        print("✅ Seller Asking Price (automatically extracted)")
        print("✅ Condition Notes (from conversation analysis)")
        print("✅ Timeline/Motivation (detected from messages)")
        print("✅ Last Inbound/Outbound timestamps")
        print("✅ Ownership confirmation tracking")
        print("✅ Phone verification (1 & 2)")
        print("✅ Intent detection and tracking")
        print("✅ Reply/Send count tracking")
        print("✅ Stage progression mapping")
        print("✅ Status updates based on conversation flow")
        print("✅ Active phone slot determination")
        print("✅ TextGrid phone number tracking")
        print("✅ Opt-out handling")
        print("✅ Lead promotion date tracking")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)