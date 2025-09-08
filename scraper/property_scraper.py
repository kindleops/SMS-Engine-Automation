
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def scroll_and_scrape_properties(driver):
    print("📥 Scraping properties from filtered results...")
    scraped = []

    try:
        cards = WebDriverWait(driver, 12).until(
            EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'deal-scroll')]//div[contains(@class, 'deal-wrapper')]"))
        )
    except Exception as e:
        print(f"❌ No property cards found: {e}")
        return scraped

    print(f"🔍 Found {len(cards)} properties. Scraping each...")

    for index, card in enumerate(cards):
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", card)
            time.sleep(0.5)
            card.click()
            print(f"➡️ Opened property {index+1}")
            time.sleep(2)

            prop = {}

            # Example fields (you'll customize based on what exists on the detail view)
            try:
                prop['address'] = driver.find_element(By.XPATH, "//h2[contains(@class, 'address')]").text
            except:
                prop['address'] = "N/A"

            try:
                prop['owner_name'] = driver.find_element(By.XPATH, "//div[contains(text(), 'Owner')]/following-sibling::div").text
            except:
                prop['owner_name'] = "N/A"

            try:
                prop['estimated_value'] = driver.find_element(By.XPATH, "//div[contains(text(), 'Est. Value')]/following-sibling::div").text
            except:
                prop['estimated_value'] = "N/A"

            # Add more fields as needed...

            scraped.append(prop)

            # Optional: Close the detail view (if a close button exists)
            try:
                close_btn = driver.find_element(By.XPATH, "//button[contains(., 'Close')]")
                close_btn.click()
                time.sleep(1)
            except:
                pass

        except Exception as e:
            print(f"❌ Error scraping property {index+1}: {e}")
            continue

    return scraped