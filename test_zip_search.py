#!/usr/bin/env python3
"""
Test script for ZIP code search functionality with enhanced error handling.
This script tests three ZIP codes with the enhanced search_zip function and
prints detailed results for each.
"""

import time
from scraper.login_utils import get_driver, login
from scraper.zip_search import search_zip

def main():
    print("🔍 Starting ZIP code search test with enhanced error handling")
    driver = None
    try:
        # Initialize webdriver and login
        print("\n🌐 Initializing webdriver and logging in...")
        driver = get_driver()
        login_success = login(driver)
        
        if not login_success:
            print("❌ Login failed, cannot proceed with tests")
            return
        
        print("\n✅ Login successful, beginning ZIP code tests")
        
        # ZIP codes to test
        zip_codes = ["90210", "33139", "10001"]
        
        # Test results tracking
        results = {}
        
        # Test each ZIP code
        for zip_code in zip_codes:
            print(f"\n{'='*30}")
            print(f"Testing ZIP code: {zip_code}")
            print(f"{'='*30}")
            
            # Allow a little pause between searches
            time.sleep(2)
            
            # Search for properties in this ZIP code
            start_time = time.time()
            search_result = search_zip(driver, zip_code, max_retries=2, wait_time=30)
            end_time = time.time()
            
            # Record and display results
            duration = round(end_time - start_time, 2)
            results[zip_code] = {
                "success": search_result,
                "duration": duration
            }
            
            print(f"\n📊 ZIP code {zip_code} search completed:")
            print(f"   - Success: {'✅ Yes' if search_result else '❌ No'}")
            print(f"   - Duration: {duration} seconds")
            
        # Display summary of all results
        print("\n" + "="*50)
        print("📋 ZIP CODE SEARCH TEST SUMMARY")
        print("="*50)
        for zip_code, result in results.items():
            status = "✅ PASSED" if result["success"] else "❌ FAILED"
            print(f"ZIP {zip_code}: {status} (took {result['duration']} seconds)")
            
    except Exception as e:
        print(f"❌ An error occurred during testing: {str(e)}")
    finally:
        # Ensure proper cleanup
        if driver:
            print("\n🧹 Cleaning up - closing browser")
            driver.quit()
            print("✓ Browser closed successfully")

if __name__ == "__main__":
    main()

