from scraper.login_utils import get_driver, login
from scraper.zip_search import search_zip
from scraper.scraper_core import scroll_and_scrape_properties
from airtable.table_router import route_and_upload
from filters import apply_basic_filters

# List of ZIPs to process
ZIP_CODES = ["90210", "33139", "10001"]  # Replace with your real targets

def main():
    driver = get_driver()

    if not driver:
        print("[!] Driver failed to initialize")
        return

    try:
        if not login(driver):
            print("[!] Login failed")
            return

        for zip_code in ZIP_CODES:
            print(f"\n===== Processing ZIP: {zip_code} =====")
            success = search_zip(driver, zip_code)

            if not success:
                print(f"[!] Skipping ZIP {zip_code} due to search failure")
                continue

            apply_basic_filters(driver, quick_filters=["Vacant", "High Equity"])

            # Scrape all sidebar property cards
            property_cards = scroll_and_scrape_properties(driver)

            if not property_cards:
                print(f"[!] No properties found in ZIP {zip_code}")
                continue

            for i, prop_data in enumerate(property_cards, start=1):
                print(f"\n--- Uploading Property #{i}: {prop_data.get('full_address')} ---")

                # Wrap scraped card into a fake full record for now
                fake_record = {
                    "property": {
                        "full_address": prop_data.get("full_address"),
                        "seller_name": prop_data.get("owner_name"),
                        "status": prop_data.get("status")
                    },
                    "seller": {},
                    "mortgage": [],
                    "company": {},
                    "company_contacts": [],
                    "phones": [],
                    "emails": [],
                    "aod": [],
                    "probate": [],
                    "liens": [],
                    "foreclosures": []
                }

                route_and_upload(fake_record)

            print(f"[✓] Completed ZIP: {zip_code} — {len(property_cards)} properties uploaded")

    finally:
        driver.quit()
        print("[✓] Driver closed")

if __name__ == "__main__":
    main()