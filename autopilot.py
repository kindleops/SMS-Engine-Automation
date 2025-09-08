import time
from scraper.login_utils import get_driver, login
from scraper.zip_search import search_zip
from scraper.filters import apply_quick_filters, apply_advanced_filters
from scraper.property_scraper import scroll_and_scrape_properties
from core.table_router import route_and_upload

# Import a specific market config
from markets.miami import ZIP_FILTER_MAP  # Swap this for any other market

def autopilot_run():
    driver = get_driver()
    if not driver:
        print("❌ Failed to initialize driver.")
        return

    if not login(driver):
        print("❌ Login failed.")
        driver.quit()
        return

    for zip_code, filters in ZIP_FILTER_MAP.items():
        print(f"===== Processing ZIP: {zip_code} =====")
        if not search_zip(driver, zip_code):
            print(f"[!] Skipping ZIP {zip_code} due to search failure")
            continue

        # Apply filters
        quick_filters = filters.get("quick", [])
        advanced_filters = filters.get("advanced", [])

        if quick_filters:
            apply_quick_filters(driver, quick_filters)

            # If multiple quick filters: click "Apply All That Match" if available
            if len(quick_filters) > 1:
                try:
                    from selenium.webdriver.common.by import By
                    from selenium.webdriver.support.ui import WebDriverWait
                    from selenium.webdriver.support import expected_conditions as EC
                    match_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Apply All That Match')]"))
                    )
                    match_button.click()
                    print("✅ Clicked 'Apply All That Match'")
                    time.sleep(1)
                except:
                    print("⚠️ Could not find 'Apply All That Match' button")

        if advanced_filters:
            apply_advanced_filters(driver, advanced_filters)

        # Scrape all properties
        properties = scroll_and_scrape_properties(driver)

        print(f"🚀 Uploading {len(properties)} properties to Airtable...")
        for prop in properties:
            route_and_upload(prop)

    driver.quit()
    print("✅ Autopilot completed for all ZIPs.")

if __name__ == "__main__":
    autopilot_run()