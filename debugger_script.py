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
from aux_functions import timeout_input
import sys
import os

DEFAULT_FILE_NAME = 'ReporteAvanzado.xlsx'
CURRENT_DIRECTORY = os.getcwd()
USERNAME = '14228822-2'
PASSWORD = 'Andrea040188.'
DAYS = 1
DATE_RANGE = f"{date.today() - timedelta(days=DAYS)} - {date.today()}"
RAZONES_SOCIALES = ['CIC_RETAIL_SPA', 'COMPAÑIAS_CIC_SA']

def replace_previous_file(filename):
    if not os.path.exists(DEFAULT_FILE_NAME):
        return
    if os.path.exists(filename):
        print(f'Removing file {filename}...\n')
        os.remove(filename)
    else:
        print("There is no previous file. Renaming new file in its place...")

    os.rename(f"{CURRENT_DIRECTORY}/{DEFAULT_FILE_NAME}",
              f"{CURRENT_DIRECTORY}/{filename}")

XPATHS = {
    'username'          : ('/html/body/div/div/main/div/div/div[1]'
                           '/div/div/div[2]/form/div[1]/div[1]/input'),
    'password'          : ('/html/body/div/div/main/div/div/div[1]'
                           '/div/div/div[2]/form/div[3]/div[1]/div[1]/input'),
    'login'             : ('/html/body/div/div/main/div/div/div[1]'
                           '/div/div/div[2]/form/div[3]/div[2]/button[2]'),
    'avanzados'         : ('/html/body/section[2]/section/div[2]'
                           '/div[9]/div/div/div/div/div/ul/li[3]/a'),
    'reporte_semanal'   : ('/html/body/section[2]/section/div[2]'
                           '/div[9]/div/div/div/div/div/div/div[3]'
                           '/div[2]/table/tbody/tr[2]/td/span'),
    'date_range_field'  : ('/html/body/section[2]/section/div[2]/div[1]'
                           '/div/div/div[2]/form/div[2]/div[1]/div/input'),
    'razon_social_list' : ('/html/body/section[2]/section/div[2]/div[1]'
                           '/div/div/div[2]/form/div[3]/div[1]/div/a/span[2]'),
    RAZONES_SOCIALES[0] : '/html/body/div[21]/ul/li[2]',
    RAZONES_SOCIALES[1] : '/html/body/div[21]/ul/li[3]',
    'download_button'   : ('/html/body/section[2]/section/div[2]/div[1]'
                           '/div/div/div[3]/button[3]'),
    'down_percentage'   : ('/html/body/section[2]/section/div[2]/div[10]'
                           '/div/div/div[2]/div/div/div[1]')
}

options = Options()
prefs = {'download.default_directory' : CURRENT_DIRECTORY}
options.add_experimental_option("prefs", prefs)
# options.add_argument('--headless')
# options.add_argument('--no-sandbox')

driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'),
                          options=options)
wait = WebDriverWait(driver=driver, timeout=10)
driver.implicitly_wait(15)

print("Opening Talana login page...\n")
driver.get('https://talana.com/es/remuneraciones/login-vue#/')

# Fill username and password fields, then press login button
print("Logging in...")
user_field = wait.until(EC.presence_of_element_located(
    (By.XPATH, XPATHS['username'])
))
pass_field = wait.until(EC.presence_of_element_located(
    (By.XPATH, XPATHS['password'])
))
user_field.send_keys(USERNAME)
pass_field.send_keys(PASSWORD)

login_btn = wait.until(EC.element_to_be_clickable(
    (By.XPATH, XPATHS['login'])
))
sleep(5)
login_btn.click()
sleep(5)
print(f'Logged into Talana as user {USERNAME}\n')

# Go into section 'Reportes' of website
print("Pointing driver to 'Reportes'...\n")
driver.get('https://talana.com/es/asistencia/reportes/')

# Go into 'Avanzados' subsection of 'Reportes'
print("Pointing driver to 'Avanzados'...\n")
avanzados_btn = wait.until(EC.presence_of_element_located(
    (By.XPATH, XPATHS['avanzados'])
))
avanzados_btn.click()

# Defining all components in 'Reporte' for the driver to use later
reporte_semanal_btn = wait.until(EC.presence_of_element_located(
    (By.XPATH, XPATHS['reporte_semanal'])
))
