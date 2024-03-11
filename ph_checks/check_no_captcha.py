import argparse
import os

import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By

from utils.loader import Loader


def get_partition(df: pd.DataFrame, partition:int, total_partitions: int=40):
  chunk_size = len(df) // total_partitions + 1

  return df[partition*chunk_size:(partition+1)*chunk_size]

def fill_form(driver, car_plate):
  driver.get(os.getenv('PH_URL'))
  vehicle_input = driver.find_element(By.XPATH, '/html/body/section/div[3]/div[4]/div[2]/div[2]/form/div/div[3]/div[2]/div/div/div/div/div/p/input')
  checkbox = driver.find_element(By.XPATH, '/html/body/section/div[3]/div[4]/div[2]/div[2]/form/div/div[5]/div/label/span[3]')
  submit_button = driver.find_element(By.XPATH, '/html/body/section/div[3]/div[4]/div[2]/div[2]/form/div/div[6]/button')

  vehicle_input.send_keys(car_plate)
  checkbox.click()
  submit_button.click()

  try:
    err = driver.find_element(By.XPATH, '/html/body/section/div[3]/div[4]/div[2]/div[2]/form/div/div[6]/div/table/tbody/tr/td/ul/li').text
    return False
  except:
    return True

def main(df, partition):
  df = df.copy()

  options = webdriver.ChromeOptions()
  options.add_argument("--headless")
  options.add_argument("--no-sandbox")
  options.add_argument("--disable-dev-shm-usage")
  options.add_argument("--window-size=1920,1200")
  driver = webdriver.Chrome(options=options)
  driver.implicitly_wait(5)

  # fill in the form
  for i, row in df.iterrows():
    if not fill_form(driver, row['car_plate']):
      df.at[i,'phv'] = -1
      continue
    
    try:
      status = driver.find_element(By.XPATH, '/html/body/section/div[3]/div[4]/div[2]/div[2]/form/div[2]/div[2]/div/div/div/div[1]/div[2]/p[2]').text

      if status == 'Yes':
        df.at[i,'phv'] = 1
        df.at[i,'decal'] = driver.find_element(By.XPATH, '/html/body/section/div[3]/div[4]/div[2]/div[2]/form/div[2]/div[2]/div/div/div/div[2]/div[2]/p[2]').text
      else:
        df.at[i,'phv'] = 0
    except:
      df.at[i,'phv'] = -2

  driver.quit()

  # check if file exists
  if not os.path.isfile(f'data/ph_check_{partition}.csv'):
    df.to_csv(f'data/ph_check_{partition}.csv', index=False)
  else:
    df.to_csv(f'data/ph_check_{partition}.csv', mode='a', header=False, index=False)


if __name__ == '__main__':
  load_dotenv()

  loader = Loader()
  loader.decrypt_file('car_plates.csv')

  df = pd.read_csv('car_plates.csv')

  df = df[:4000]

  parser = argparse.ArgumentParser()
  parser.add_argument('--partition', type=int, help='Partition number')
  args = parser.parse_args()

  df = get_partition(df, args.partition)

  main(df, args.partition)

  loader.encrypt_file(f'data/ph_check_{args.partition}.csv')
