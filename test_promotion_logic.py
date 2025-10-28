#!/usr/bin/env python3
"""
Test lead promotion logic
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

def test_promotion_logic():
    """Test the lead promotion logic specifically"""
    
    print("🧪 Testing lead promotion logic...")
    
    try:
        from sms.inbound_webhook import _should_promote, _stage_rank, PROMOTION_INTENTS, PROMOTION_AI_INTENTS
        
        # Test case from our message
        intent = "Positive"
        ai_intent = "interest_detected"
        stage = "STAGE 3 - PRICE QUALIFICATION"
        
        print(f"📊 Test inputs:")
        print(f"  Intent: '{intent}'")
        print(f"  AI Intent: '{ai_intent}'")
        print(f"  Stage: '{stage}'")
        
        print(f"\n🔍 Promotion criteria:")
        print(f"  PROMOTION_INTENTS: {PROMOTION_INTENTS}")
        print(f"  PROMOTION_AI_INTENTS: {PROMOTION_AI_INTENTS}")
        
        # Test each condition
        intent_match = intent.lower() in PROMOTION_INTENTS
        ai_intent_match = ai_intent in PROMOTION_AI_INTENTS
        stage_rank = _stage_rank(stage)
        stage_match = stage_rank >= 3
        
        print(f"\n✅ Condition checks:")
        print(f"  Intent match ('{intent.lower()}' in PROMOTION_INTENTS): {intent_match}")
        print(f"  AI Intent match ('{ai_intent}' in PROMOTION_AI_INTENTS): {ai_intent_match}")
        print(f"  Stage rank: {stage_rank}")
        print(f"  Stage match (rank >= 3): {stage_match}")
        
        should_promote = _should_promote(intent, ai_intent, stage)
        print(f"\n🚀 Final result: should_promote = {should_promote}")
        
        if should_promote:
            print("✅ Lead promotion should occur!")
        else:
            print("❌ Lead promotion should NOT occur - check logic!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_promotion_logic()