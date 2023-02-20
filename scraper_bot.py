from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')

driver = webdriver.Chrome(
    service=Service('/usr/local/bin/chromedriver'),
    options=options
)

driver.get('https://google.com/')
print(driver.title)
driver.quit()