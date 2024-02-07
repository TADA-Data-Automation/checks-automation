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


def valid_nric(string):
  if match := re.match(r'^[STFG]\d{7}[A-Z]$', string):
    return True
  else:
    return False

def retrieve_date(driver, nric: str, birthday: str, driver_type: str):
  if not valid_nric(nric):
    return pd.NA
  ic = driver.find_element(By.XPATH, '//*[@id="nric"]')
  ic.clear()
  ic.send_keys(nric)
  dob = driver.find_element(By.XPATH, '//*[@id="dob"]')
  dob.clear()
  dob.send_keys(birthday)

  button = driver.find_element(By.XPATH, '//*[@id="proceedBtn"]')
  button.click()

  while "Loading..." in BeautifulSoup(driver.page_source,'html.parser').select_one('div#license-results'):
    pass

  div = BeautifulSoup(driver.page_source,'html.parser').select_one('div#license-results')

  if "No record Found" in str(div):
    return pd.NA
  else:
    try:
      table = pd.read_html(StringIO(str(div)))[0]
      return get_expiry(table, driver_type)
    except:
      return pd.NA

def get_expiry(table: pd.DataFrame, driver_type: str):
  if driver_type in ['PRIVATE_HIRE','HOURLY_RENTAL']:
    if not table[(table.Status == 'Revoked') & (table['VL Type'].isin(["Taxi Driver's Vocational Licence (TDVL)", "Private Hire Car Driver's Vocational Licence (PDVL)"]))].empty:
      return 0
    df = table[(table.Status == 'Valid') & (table['VL Type'].isin(["Taxi Driver's Vocational Licence (TDVL)", "Private Hire Car Driver's Vocational Licence (PDVL)"]))]
    return df["Expiry Date"].max()
  elif driver_type == 'TAXI':
    df = table[(table.Status == 'Valid') & (table['VL Type'] == "Taxi Driver's Vocational Licence (TDVL)")]
    return df['Expiry Date'].max()
  else:
    return -1
  
def get_partition(df: pd.DataFrame, partition:int, total_partitions: int=15):
  chunk_size = len(df) // total_partitions + 1

  return df[partition*chunk_size:(partition+1)*chunk_size]

def main(partition: int):
  load_dotenv()

  loader = Loader()

  loader.decrypt_file('drivers.csv')
  df = pd.read_csv('drivers.csv')

  drivers = get_partition(df, partition)
  drivers.insert(5,'expiry',pd.NA)
  drivers.insert(6,'remarks',pd.NA)
  drivers.loc[drivers["birth"].isna(), "remarks"] = "Missing birth date"
  drivers.loc[drivers["nric"].isna(), "remarks"] = "Missing NRIC"

  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  driver = webdriver.Chrome(options=options)
  driver.get(os.getenv('VL_URL'))

  try:
    for index, row in drivers.loc[drivers['remarks'].isna()].iterrows():
      expiry = retrieve_date(driver,row['nric'],row['birth'],row['type'])
      if pd.isna(expiry):
        drivers.loc[index, 'remarks'] = 'No record found'
      elif expiry == 0:
        drivers.loc[index, 'remarks'] = 'Revoked'
      else:
        drivers.loc[index, 'expiry'] = expiry

  finally:
    drivers.loc[(drivers.vl_expiry_date != drivers.expiry) & drivers.remarks.isna(), "remarks"] = 'Mismatched expiry'
    drivers.to_csv(f'data/vl_monthly_{time.strftime("%b")}_{partition}.csv', index=False)

    loader = Loader()

    loader.encrypt_file(f'data/vl_monthly_{time.strftime("%b")}_{partition}.csv')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--partition', type=int, help='Partition number')
  args = parser.parse_args()
  main(partition=args.partition)
