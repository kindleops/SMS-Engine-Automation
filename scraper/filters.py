
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

def apply_quick_filters(driver, filters):
    print(f"🎯 Applying quick filters: {filters}")
    for label in filters:
        try:
            filter_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{label}')]"))
            )
            filter_button.click()
            print(f"✅ Applied quick filter: {label}")
            time.sleep(1)
        except TimeoutException:
            print(f"⚠️ Quick filter not found: {label}")

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
