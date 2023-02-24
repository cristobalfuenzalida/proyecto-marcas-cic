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
DAYS = 93
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
    RAZONES_SOCIALES[1] : '/html/body/div[23]/ul/li[3]',
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

# Gestionar archivos de descarga
for razon_social in RAZONES_SOCIALES:
    print("Opening panel 'Reporte semanal'...\n")
    reporte_semanal_btn.click()
    sleep(3)
    # ------------------------------------------------------------------------ 
    # Component definitions for driver
    date_range_field = wait.until(EC.presence_of_element_located(
        (By.XPATH, XPATHS['date_range_field'])
    ))
    razon_social_list = wait.until(EC.presence_of_element_located(
        (By.XPATH, XPATHS['razon_social_list'])
    ))
    down_btn = wait.until(EC.presence_of_element_located(
        (By.XPATH, XPATHS['download_button'])
    ))
    # ------------------------------------------------------------------------
    print("Unmarking unnecesary option checkboxes...\n")
    for (i, j) in [(3, 1), (3, 2), (5, 1), (5, 2), (5, 4),
                   (5, 5), (6, 2), (6, 3)]:
        cb_xpath = ('/html/body/section[2]/section/div[2]/div[1]/div/div/div[2]'
                    + f'/form/div[1]/div[{i}]/div[{j}]/label/input')
        checkbox = wait.until(EC.presence_of_element_located(
            (By.XPATH, cb_xpath)
        ))
        checkbox.click()
        sleep(0.5)

    print("Setting date range for data...\n")
    date_range_field.clear()
    date_range_field.send_keys(DATE_RANGE)
    date_range_field.send_keys(Keys.ENTER)
    date_range_field.send_keys(Keys.TAB)

    print("Filtering data by 'razón social'...\n")
    razon_social_list.click()
    razon_social_btn = wait.until(EC.presence_of_element_located(
        (By.XPATH, XPATHS[razon_social])
    ))
    razon_social_btn.click()
    sleep(3)

    print(f'Starting download...\n')
    down_btn.click()
    start = time()
    while True:
        percentage = wait.until(EC.presence_of_element_located(
            (By.XPATH, XPATHS['down_percentage'])
        )).text
        percentage = 0 if percentage == '' else int(percentage)

        if os.path.isfile(DEFAULT_FILE_NAME):
            print('File download complete!')
            break
        print(f'File download in progress   : {percentage}%')
        user_input = timeout_input(5, "To cancel download, press X : ", '')
        print()
        if user_input.upper() == 'X':
            print('Download canceled...')
            print('Program ended early')
            driver.quit()
            sys.exit(0)

    total_seconds = int(time() - start)

    current_time = str(dtime(
        hour=(total_seconds // 3600),
        minute=((total_seconds % 3600) // 60),
        second=(total_seconds % 60)
    ))
    print(f'Download took {current_time} hrs.\n')

    replace_previous_file(f'Reporte_{razon_social}.xlsx')

print('Program finished successfully')
driver.quit()
