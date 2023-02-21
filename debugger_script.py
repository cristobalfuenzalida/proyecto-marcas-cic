from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from datetime import date, timedelta
from datetime import time as dtime
from time import sleep, time
import select
import sys
import os

def timeout_input(timeout, prompt="", timeout_value=None):
    sys.stdout.write(prompt)
    sys.stdout.flush()
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if ready:
        return sys.stdin.readline().rstrip('\n')
    else:
        sys.stdout.write('\n')
        sys.stdout.flush()
        return timeout_value

options = Options()
download_directory = (os.getcwd())
prefs = {'download.default_directory' : download_directory}
options.add_experimental_option("prefs", prefs)
# options.add_argument('--headless')
# options.add_argument('--no-sandbox')

driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'),
                          options=options)

USERNAME = '14228822-2'
PASSWORD = 'Andrea040188.'
URL = 'https://talana.com/es/remuneraciones/login-vue?next=/es/asistencia/#/'

driver.get(URL)

wait = WebDriverWait(driver=driver, timeout=10)
driver.implicitly_wait(15)

# Fill username field
user_xpath = ('/html/body/div/div/main/div/div/div[1]'
            '/div/div/div[2]/form/div[1]/div[1]/input')

user_field = wait.until(EC.presence_of_element_located((By.XPATH, user_xpath)))
user_field.send_keys(USERNAME)

# Fill password field
pass_xpath = ('/html/body/div/div/main/div/div/div[1]'
            '/div/div/div[2]/form/div[3]/div[1]/div[1]/input')

pass_field = wait.until(EC.presence_of_element_located((By.XPATH, pass_xpath)))
pass_field.send_keys(PASSWORD)

# Click login button (after a 5 second delay)
login_xpath = ('/html/body/div/div/main/div/div/div[1]'
               '/div/div/div[2]/form/div[3]/div[2]/button[2]')

login_btn = wait.until(EC.element_to_be_clickable((By.XPATH, login_xpath)))
sleep(5)
login_btn.click()
sleep(5)
print(f'Logged into Talana as user {USERNAME}\n')

# Go into section 'Reportes' from dashboard
driver.get('https://talana.com/es/asistencia/reportes/')

# Go into 'Avanzados' subsection of 'Reportes'
avnz_xpath = ('/html/body/section[2]/section/div[2]'
              '/div[9]/div/div/div/div/div/ul/li[3]/a')

avnz_btn = wait.until(EC.presence_of_element_located((By.XPATH, avnz_xpath)))
avnz_btn.click()

# Open options to download 'Reporte semanal por rut'
rspr_xpath = ('/html/body/section[2]/section/div[2]'
              '/div[9]/div/div/div/div/div/div/div[3]'
              '/div[2]/table/tbody/tr[2]/td/span')

rspr_btn = wait.until(EC.presence_of_element_located((By.XPATH, rspr_xpath)))
rspr_btn.click()

# Unmark unnecessary checkboxes

for (i, j) in [(3, 1), (3, 2), (5, 1), (5, 2), (5, 4), (5, 5), (6, 2)]:
    sleep(0.4)
    box_xpath = ('/html/body/section[2]/section/div[2]/div[1]/div/div/div[2]'
                + f'/form/div[1]/div[{i}]/div[{j}]/label/input')
    checkbox = wait.until(EC.presence_of_element_located((By.XPATH, box_xpath)))
    checkbox.click()


# Replace default date range input with custom range
date_xpath = ('/html/body/section[2]/section/div[2]'
              '/div[1]/div/div/div[2]/form/div[2]/div[1]/div/input')

today = date.today()
delta = timedelta(days=15)

DATE_RANGE = f"{today - delta} - {today}"

date_field = wait.until(EC.presence_of_element_located((By.XPATH, date_xpath)))
date_field.clear()
date_field.send_keys(DATE_RANGE)
date_field.send_keys(Keys.ENTER)
date_field.send_keys(Keys.TAB)

# Filter by 'Razón Social'
rs_list_xpath = ('/html/body/section[2]/section/div[2]'
                 '/div[1]/div/div/div[2]/form/div[3]/div[1]/div/a/span[2]')
rs_coll = wait.until(EC.presence_of_element_located((By.XPATH, rs_list_xpath)))
rs_coll.click()

rs_select_xpath = ('/html/body/div[21]/ul/li[2]')
razon_social = wait.until(
    EC.presence_of_element_located((By.XPATH, rs_select_xpath)))
razon_social.click()
