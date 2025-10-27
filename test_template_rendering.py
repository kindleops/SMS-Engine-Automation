#!/usr/bin/env python3
"""
Test script to verify template placeholder handling with spaces
"""

import re

def _squish(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s).strip()

def test_template_rendering():
    """Test that template placeholders with spaces render correctly"""
    print("\n=== Testing Template Placeholder Rendering ===")
    
    # Test personalization data
    personalization = {
        "First": "John",
        "Address": "123 Main St",
        # City aliases so any template style works:
        "Property City": "Dallas", 
        "Property_City": "Dallas",
        "PropertyCity": "Dallas",
    }
    
    # Test template with spaced placeholders
    template = "Hi {First}, this is Ryan, a local {Property City} investor. My wife and I drove by {Address} today, are you still the owner? Reply STOP to opt out."
    
    print(f"Original template: {template}")
    
    # Normalize spaced placeholders
    normalized = template.replace("{Property City}", "{Property_City}")
    normalized = normalized.replace("{Owner First Name}", "{First}")
    
    print(f"Normalized template: {normalized}")
    
    # Format with personalization
    try:
        result = normalized.format(**personalization)
        result = _squish(result)
        print(f"Rendered result: {result}")
        
        # Verify expected content
        assert "John" in result, "First name not rendered"
        assert "Dallas" in result, "Property City not rendered"
        assert "123 Main St" in result, "Address not rendered"
        assert "  " not in result, "Double spaces found"
        
        print("✅ Template rendering test PASSED!")
        
    except Exception as e:
        print(f"❌ Template rendering failed: {e}")
        return False
    
    # Test edge case with empty city
    print("\n--- Testing empty city case ---")
    personalization_empty_city = {
        "First": "Jane", 
        "Address": "456 Oak Ave",
        "Property City": "",
        "Property_City": "",
        "PropertyCity": "",
    }
    
    try:
        result_empty = normalized.format(**personalization_empty_city)
        result_empty = _squish(result_empty)
        print(f"Empty city result: {result_empty}")
        
        # Should not have double spaces
        assert "  " not in result_empty, "Double spaces found with empty city"
        assert "Jane" in result_empty, "First name not rendered"
        assert "456 Oak Ave" in result_empty, "Address not rendered"
        
        print("✅ Empty city test PASSED!")
        
    except Exception as e:
        print(f"❌ Empty city test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_template_rendering()
    print(f"\n{'✅ All tests PASSED!' if success else '❌ Tests FAILED!'}")