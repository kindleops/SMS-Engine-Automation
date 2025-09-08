from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

def search_zip(driver, zip_code):
    print(f"\n🔍 Searching ZIP: {zip_code}")
    zip_code = str(zip_code).strip()

    # Try to clear previous search results first (if this is not the first search)
    try:
        # Look for a "clear" button, "x" button, or similar to clear search
        clear_selectors = [
            "//button[contains(@aria-label, 'clear') or contains(@aria-label, 'Clear')]",
            "//button[contains(@class, 'clear')]",
            "//div[contains(@class, 'clear')]",
            "//svg[contains(@class, 'clear')]",
            "//input[@type='search']/following-sibling::*[contains(@class, 'clear')]"
        ]
        
        for selector in clear_selectors:
            try:
                clear_button = driver.find_element(By.XPATH, selector)
                driver.execute_script("arguments[0].click();", clear_button)
                print("🧹 Cleared previous search")
                time.sleep(1)
                break
            except:
                continue
    except Exception as e:
        print(f"Note: Could not clear previous search: {e}")

    # Try to scroll to the top of the page to make search input visible
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)
    except:
        pass

    # Try known search input selectors
    search_selectors = [
        '//input[contains(@placeholder, "Search")]',
        '//input[@type="search"]',
        '//input[@role="searchbox"]',
        '//input[contains(@name, "search")]'
    ]

    search_input = None
    for selector in search_selectors:
        try:
            search_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, selector))
            )
            # Try to scroll the search input into view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_input)
            time.sleep(0.5)
            break
        except TimeoutException:
            continue

    if not search_input:
        print("❌ Search input not found.")
        return False

    try:
        # Try multiple approaches to set the search value and submit
        
        # First approach: Try direct JavaScript to fully clear and set the field
        driver.execute_script("""
            const input = arguments[0];
            input.focus();
            input.value = '';
            const event = new Event('input', { bubbles: true });
            input.dispatchEvent(event);
        """, search_input)
        time.sleep(0.5)
        
        # Try to click on it directly if it's not intercepted
        try:
            search_input.click()
            time.sleep(0.5)
        except:
            print("Note: Direct click intercepted, using JavaScript focus instead")
        
        # Set the value using JavaScript
        driver.execute_script(f"""
            const input = arguments[0];
            input.value = '{zip_code}';
            input.dispatchEvent(new Event('input', {{ bubbles: true }}));
            input.dispatchEvent(new Event('change', {{ bubbles: true }}));
        """, search_input)
        time.sleep(0.5)
        
        # Try different approaches to submit the search
        submit_success = False
        
        # 1. Try to press Enter on the input element
        try:
            search_input.send_keys(Keys.ENTER)
            submit_success = True
        except Exception as e:
            print(f"Note: Could not send Enter key: {e}")
        
        # 2. If Enter key failed, try to find and click a search button
        if not submit_success:
            try:
                search_button = driver.find_element(By.XPATH, "//button[contains(@aria-label, 'search') or contains(@type, 'submit')]")
                driver.execute_script("arguments[0].click();", search_button)
                submit_success = True
            except Exception as e:
                print(f"Note: Could not click search button: {e}")
        
        # 3. If both failed, try to submit the form using JavaScript
        if not submit_success:
            try:
                driver.execute_script("""
                    const form = arguments[0].closest('form');
                    if (form) form.submit();
                """, search_input)
                submit_success = True
            except Exception as e:
                print(f"Note: Could not submit form: {e}")
        
        print("✅ ZIP submitted")
        time.sleep(2)  # Wait longer for results to load

        # Confirm results are showing
        sidebar_property_selector = "//div[contains(@class, 'deal-scroll')]//div[contains(@class, 'deal-wrapper')]"

        WebDriverWait(driver, 15).until(  # Increased timeout
            EC.presence_of_element_located((By.XPATH, sidebar_property_selector))
        )
        print("✅ Property cards detected in sidebar.")

        print("✅ Properties loaded")
        return True

    except Exception as e:
        print(f"❌ ZIP search failed: {e}")
        return False
