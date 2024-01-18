from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options



def create_chrome_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    return webdriver.Chrome(options=options)

def get_cookie(driver, url: str):
    driver.get(url)
    allow = driver.find_element(by=By.CLASS_NAME, value='cookie-allow')
    allow.click()
    cookie_data = driver.get_cookies()
    cookie_string = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in reversed(cookie_data)])
    return cookie_string

def cookie(base_url: str) -> str:
    driver = create_chrome_driver()
    cookie = get_cookie(driver, base_url)
    driver.quit()
    return cookie
