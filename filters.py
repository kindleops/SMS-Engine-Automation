from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

def apply_basic_filters(driver, quick_filters=None, advanced_filters=None):
    """Apply both quick and advanced filters to the search results"""
    if quick_filters:
        print(f"🎯 Applying Basic Filters: {' + '.join(quick_filters)}")
        apply_quick_filters(driver, quick_filters)
    else:
        print("🎯 Applying Basic Filters: None specified")
    
    if advanced_filters:
        apply_advanced_filters(driver, advanced_filters)
    
    # Allow time for filters to apply
    time.sleep(2)
    return True

def apply_quick_filters(driver, filters):
    print(f"🎯 Applying quick filters: {filters}")
    
    # Wait for page to be fully loaded
    time.sleep(3)
    
    # First, try to find and click on a "Filter" button to open the filters panel
    filter_button_clicked = False
    filter_button_selectors = [
        "//button[contains(text(), 'Filter')]",
        "//button[contains(text(), 'Filters')]", 
        "//button[contains(@aria-label, 'filter')]",
        "//button[contains(@class, 'filter')]",
        "//div[contains(text(), 'Filter')]",
        "//span[contains(text(), 'Filter')]",
        "//button[contains(text(), 'Sort')]",  # Sometimes filters are under "Sort & Filter"
        "//i[contains(@class, 'filter')]/..",  # Parent of a filter icon
        "//img[contains(@alt, 'filter')]/.."   # Parent of a filter image
    ]
    
    print("🔍 Looking for filter panel button...")
    for selector in filter_button_selectors:
        try:
            filter_button = driver.find_element(By.XPATH, selector)
            print(f"Found potential filter button: '{filter_button.text}'")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", filter_button)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", filter_button)
            print(f"✅ Clicked filter button: '{filter_button.text}'")
            filter_button_clicked = True
            time.sleep(2)  # Wait for filter panel to open
            break
        except Exception as e:
            pass
    
    if filter_button_clicked:
        print("Filter panel should now be open")
    else:
        print("⚠️ Could not find a filter button - filters may already be visible or have a different UI")
    
    # Print page structure to help debug
    print("\n📄 Page Text Content (to help identify filter elements):")
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        # Print just a limited amount to avoid overwhelming the log
        print(body_text[:500] + "..." if len(body_text) > 500 else body_text)
    except Exception as e:
        print(f"Error getting page text: {e}")
    
    # Debug: Print all available filter buttons on the page
    try:
        print("\n🔍 Looking for available filter buttons...")
        # Try to find all buttons and filter-like elements
        all_buttons = driver.find_elements(By.XPATH, "//button")
        all_spans = driver.find_elements(By.XPATH, "//span[contains(@class, 'filter') or contains(@class, 'tag')]")
        all_divs = driver.find_elements(By.XPATH, "//div[contains(@class, 'filter') or contains(@class, 'tag')]")
        
        # Look for potential filter checkboxes and labels
        all_checkboxes = driver.find_elements(By.XPATH, "//input[@type='checkbox']")
        all_labels = driver.find_elements(By.XPATH, "//label")
        
        print(f"Found {len(all_buttons)} buttons, {len(all_spans)} filter spans, {len(all_divs)} filter divs")
        print(f"Found {len(all_checkboxes)} checkboxes, {len(all_labels)} labels")
        
        # Print the text of each potential filter element
        print("Button texts:")
        for btn in all_buttons:
            btn_text = btn.text.strip()
            if btn_text:
                print(f"  - '{btn_text}'")
        
        print("Checkbox labels:")
        for label in all_labels:
            label_text = label.text.strip()
            if label_text:
                print(f"  - '{label_text}'")
        
        print("Filter span texts:")
        for span in all_spans:
            span_text = span.text.strip()
            if span_text:
                print(f"  - '{span_text}'")
                
        print("Filter div texts:")
        for div in all_divs:
            div_text = div.text.strip()
            if div_text:
                print(f"  - '{div_text}'")
    except Exception as e:
        print(f"Error while looking for filter elements: {e}")
    
    # Try to apply each filter
    for label in filters:
        applied = False
        
        # Try different selector strategies
        selector_strategies = [
            # Case-sensitive exact match on button text
            f"//button[text()='{label}']",
            # Case-insensitive contains on button text
            f"//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')]",
            # Match on button with any descendant containing text
            f"//button[contains(., '{label}')]",
            # Try matching on spans with filter-like classes
            f"//span[contains(@class, 'filter') or contains(@class, 'tag')][contains(text(), '{label}')]",
            # Try matching on divs with filter-like classes
            f"//div[contains(@class, 'filter') or contains(@class, 'tag')][contains(text(), '{label}')]",
            # Try buttons with aria-label
            f"//button[contains(@aria-label, '{label}')]",
            # Try checkboxes and their labels
            f"//label[contains(text(), '{label}')]",
            f"//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')]",
            # Try elements within a modal dialog or dropdown
            f"//div[contains(@class, 'modal') or contains(@class, 'dropdown') or contains(@class, 'popover')]//*[contains(text(), '{label}')]",
            # Try any clickable element with the filter text
            f"//*[contains(text(), '{label}')]"
        ]
        
        for selector in selector_strategies:
            try:
                print(f"  → Trying to find '{label}' with selector: {selector}")
                filter_element = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                
                # Try to scroll the element into view before clicking
                driver.execute_script("arguments[0].scrollIntoView(true);", filter_element)
                time.sleep(0.5)
                
                filter_element.click()
                print(f"✅ Applied filter: {label}")
                applied = True
                time.sleep(1.5)  # Wait longer for filter to apply
                break
            except Exception as e:
                # Continue to the next selector strategy
                pass
        
        if not applied:
            print(f"⚠️ Could not apply filter '{label}': No matching element found")

def open_advanced_filters(driver):
    try:
        more_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'More')]"))
        )
        more_button.click()
        print("✅ Opened advanced filters panel")
        time.sleep(1)
        return True
    except TimeoutException:
        print("❌ Could not open advanced filters")
        return False

def apply_advanced_filters(driver, advanced_filters):
    if not open_advanced_filters(driver):
        return

    for label in advanced_filters:
        try:
            checkbox = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, f"//label[contains(., '{label}')]"))
            )
            checkbox.click()
            print(f"✅ Enabled advanced filter: {label}")
            time.sleep(0.5)
        except TimeoutException:
            print(f"⚠️ Advanced filter not found: {label}")

    try:
        apply_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Apply')]"))
        )
        apply_btn.click()
        print("✅ Applied all advanced filters")
    except TimeoutException:
        print("❌ Could not click 'Apply' for advanced filters")