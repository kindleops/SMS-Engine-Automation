from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
EMAIL = os.getenv("DEALMACHINE_EMAIL")
PASSWORD = os.getenv("DEALMACHINE_PASSWORD")
BRAVE_PATH = os.getenv("BRAVE_PATH")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH")

def get_driver():
    options = Options()
    options.binary_location = BRAVE_PATH
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver

def login(driver):
    try:
        driver.get("https://app.dealmachine.com/login")
        print("[>] Opened DealMachine login page")
        time.sleep(2)

        # Email field
        email_input = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@placeholder='Email Address']"))
        )
        email_input.clear()
        email_input.send_keys(EMAIL)
        print("[+] Email entered")

        # Password field
        password_input = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@placeholder='Password']"))
        )
        password_input.clear()
        password_input.send_keys(PASSWORD)
        print("[+] Password entered")

        # Login button is actually a styled div, not a <button>
        login_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//div[text()='Continue With Email']"))
        )

        # Scroll and click
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", login_button)
            time.sleep(0.5)
            login_button.click()
            print("[+] Clicked login button normally (div element)")
        except:
            driver.execute_script("arguments[0].click();", login_button)
            print("[+] Clicked login button with JS fallback")

        # Wait for redirect (dashboard)
        WebDriverWait(driver, 20).until(
            lambda d: "login" not in d.current_url and "app.dealmachine.com" in d.current_url
        )
        print("[✅] Login successful")
        driver.save_screenshot("login_success.png")
        return True

    except TimeoutException:
        print("[!] Login timeout or error")
        driver.save_screenshot("login_error.png")
        return False