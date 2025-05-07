import argparse
import os
import time

import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.loader import Loader


def get_partition(df: pd.DataFrame, partition:int, total_partitions: int=40):
  chunk_size = len(df) // total_partitions + 1

  return df[partition*chunk_size:(partition+1)*chunk_size]


def fill_form(driver, car_plate):
  wait = WebDriverWait(driver, 5)
  vehicle_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="vehicleNo"]')))
  checkbox = driver.find_element(By.XPATH, '//*[@id="agreeTCbox"]')
  submit_button = driver.find_element(By.XPATH, '//*[@id="button"]')

  vehicle_input.clear()
  vehicle_input.send_keys(car_plate)
  driver.execute_script("arguments[0].click();", checkbox)

  try:
    time.sleep(1)
    driver.execute_script("arguments[0].click();", submit_button)

    wait = WebDriverWait(driver, 5)
    status = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="pnlBdyVehicleList"]/div/div/div/div[1]/div[2]/p[2]'))).text
    decal = driver.find_element(By.XPATH, '//*[@id="pnlBdyVehicleList"]/div/div/div/div[2]/div[2]/p[2]').text

    button = driver.find_element(By.XPATH, '//*[@id="main-content"]/div[2]/div[2]/form/div[4]/div[1]/button[2]')
    driver.execute_script("arguments[0].click();", button)

    if status == 'Yes':
      return (1, decal)
    else:
      return (0, pd.NA)
  
  except UnexpectedAlertPresentException:
    alert = driver.switch_to.alert
    alert.accept()
    return (-1, pd.NA)

  except:
    return (-1, pd.NA)

def main(partition: int):
  load_dotenv()

  loader = Loader()

  loader.decrypt_file('drivers.csv')
  df = pd.read_csv('drivers.csv')

  drivers = get_partition(df, partition)

  options = webdriver.ChromeOptions()
  options.add_argument("--headless")
  options.add_argument("--window-size=1200x1200")
  options.add_argument("--no-sandbox")
  options.add_argument("--disable-dev-shm-usage")

  driver = webdriver.Chrome(options=options)

  try:
    driver.get(os.getenv('PH_URL'))

    for i, row in drivers.iterrows():
      status, decal = fill_form(driver, row['car_plate'])
      if status == -1:
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")
        
        # Use CDP for deeper cache clearing
        driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
        driver.execute_cdp_cmd('Network.clearBrowserCache', {})

        # Open a new tab
        driver.execute_script("window.open('about:blank', '_blank');")
        new_window = driver.window_handles[-1]
        driver.switch_to.window(new_window)

        # Close all other (older) windows
        for handle in driver.window_handles[:-1]:
          driver.switch_to.window(handle)
          driver.close()

        # Switch to the new window again
        driver.switch_to.window(new_window)

        # Navigate to the target page
        driver.get(os.getenv("PH_URL"))

        status, decal = fill_form(driver, row['car_plate'])

      drivers.loc[i,'phv'] = status
      if status == 1:
        drivers.loc[i,'decal'] = decal

      if (i + 1) % 50 == 0 or (i + 1) == len(drivers):
          print(f"[{i+1}/{len(drivers)}] Processed {row['car_plate']} → Status: {status}, Decal: {decal}")

    driver.quit()

  except Exception as e:
    print("An exception occurred:", e)
    driver.quit()
    raise e

  finally:
    drivers.to_csv(f'data/decal_check_{time.strftime("%b")}_{partition}.csv', index=False)

    loader = Loader()

    loader.encrypt_file(f'data/decal_check_{time.strftime("%b")}_{partition}.csv')

    driver.quit()


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--partition', type=int, help='Partition number')
  args = parser.parse_args()
  main(partition=args.partition)
