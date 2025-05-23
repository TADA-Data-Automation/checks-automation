import argparse
import os
import re
import time
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By

from utils.loader import Loader

STATUSES = ["Revoked", "Suspended", "Cancelled", "Application Lapsed", "Approved to Attend Course"]

def valid_nric(string):
  if type(string) != str:
    return False
  if match := re.match(r'^[STFG]\d{7}[A-Z]$', string):
    return True
  else:
    return False

def valid_dob(string):
  if type(string) != str:
    return False
  try:
    time.strptime(string, "%d-%m-%Y")
    return True
  except ValueError:
    return False

def retrieve_date(driver, nric: str, birthday: str, driver_type: str, car_type: int):
  if not (valid_nric(nric) and valid_dob(birthday)):
    return ("Invalid NRIC or DOB", pd.NA)
  reset_button = driver.find_element(By.XPATH, '//*[@id="license-search-form"]/table/tbody/tr[3]/td[2]/button[2]')
  driver.execute_script("arguments[0].click();", reset_button)

  ic = driver.find_element(By.XPATH, '//*[@id="nric"]')
  ic.send_keys(nric)
  dob = driver.find_element(By.XPATH, '//*[@id="dob"]')
  dob.send_keys(birthday)

  button = driver.find_element(By.XPATH, '//*[@id="proceedBtn"]')
  driver.execute_script("arguments[0].click();", button)

  while "Loading..." in BeautifulSoup(driver.page_source,'html.parser').select_one('div#license-results'):
    pass

  div = BeautifulSoup(driver.page_source,'html.parser').select_one('div#license-results')

  if "No record Found" in str(div):
    return ("No record found", pd.NA)
  else:
    try:
      table = pd.read_html(StringIO(str(div)))[0]
      return get_expiry(table, driver_type, car_type)
    except:
      return ("Unable to read table information", pd.NA)

def get_expiry(table: pd.DataFrame, driver_type: str, car_type: int):
  if driver_type in ['PRIVATE_HIRE','HOURLY_RENTAL']:
    if not pd.isna(car_type) and car_type == 3001:
      df = table[table['VL Type'] == "Bus Driver's Vocational Licence (BDVL)"]
    else:
      df = table[table['VL Type'].isin(["Taxi Driver's Vocational Licence (TDVL)", "Private Hire Car Driver's Vocational Licence (PDVL)"])]
  elif driver_type == 'TAXI':
    df = table[table['VL Type'] == "Taxi Driver's Vocational Licence (TDVL)"]
  else:
    return ("Invalid driver type", pd.NA)
  
  if df.empty:
    return ("No valid record of corresponding license", pd.NA)
  
  if not df[df.Status == "Valid"].empty:
    df = df[df.Status == "Valid"]
    return (pd.NA, df["Expiry Date"].max())
  
  for status in STATUSES:
    if not df[df.Status == status].empty:
      return (status, pd.NA)
  
  return ("Unknown status found", pd.NA)
  
def get_partition(df: pd.DataFrame, partition:int, total_partitions: int=40):
  chunk_size = len(df) // total_partitions + 1

  return df[partition*chunk_size:(partition+1)*chunk_size]

def main(partition: int):
  load_dotenv()

  loader = Loader()

  loader.decrypt_file('drivers.csv')
  df = pd.read_csv('drivers.csv', dtype={'car_type': 'Int64'})

  drivers = get_partition(df, partition)
  drivers.insert(7,'expiry',pd.NA)
  drivers.insert(8,'source',pd.NA)
  drivers.insert(9,'remarks',pd.NA)
  drivers.loc[drivers["birth"].isna(), "remarks"] = "Missing birth date"
  drivers.loc[drivers["nric"].isna(), "remarks"] = "Missing NRIC"

  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  options.add_argument("--no-sandbox")  # This make Chromium more stable in a container environment
  options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
  driver = webdriver.Chrome(options=options)
  driver.get(os.getenv('VL_URL'))

  try:
    for index, row in drivers.iterrows():
      status, expiry = retrieve_date(driver, row['nric'], row['birth'], row['type'], row['car_type'])
      drivers.loc[index, 'source'] = "LTA"
      if pd.isna(expiry):
        drivers.loc[index, 'remarks'] = status
      else:
        drivers.loc[index, 'expiry'] = expiry

  except Exception as error:
    print("An exception occurred:", error)

  finally:
    drivers.loc[(drivers.vl_expiry_date != drivers.expiry) & drivers.remarks.isna(), "remarks"] = 'Mismatched expiry'
    drivers.to_csv(f'data/vl_check_{time.strftime("%b")}_{partition}.csv', index=False)

    loader = Loader()

    loader.encrypt_file(f'data/vl_check_{time.strftime("%b")}_{partition}.csv')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--partition', type=int, help='Partition number')
  args = parser.parse_args()
  main(partition=args.partition)
