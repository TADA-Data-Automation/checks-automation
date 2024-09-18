import os
import re
import time
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By

from utils.slack import SlackBot


def valid_nric(string):
  if type(string) != str:
    return False
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
    df = table[(table.Status == 'Valid') & (table['VL Type'].isin(["Taxi Driver's Vocational Licence (TDVL)", "Private Hire Car Driver's Vocational Licence (PDVL)"]))]
    return df["Expiry Date"].max()
  elif driver_type == 'TAXI':
    df = table[(table.Status == 'Valid') & (table['VL Type'] == "Taxi Driver's Vocational Licence (TDVL)")]
    return df['Expiry Date'].max()
  else:
    return pd.NA

def retrieve_go(driver, vl_id: str, driver_type: str):
  driver.get(os.getenv('GO_URL'))

  driver.find_element(By.XPATH, '//*[@id="_ltalicenceenquiry_WAR_foblsportlet_licNumber"]').send_keys(vl_id)
  driver.find_element(By.XPATH, '//*[@id="_ltalicenceenquiry_WAR_foblsportlet_submit"]').click()
  try:
    driver.find_element(By.XPATH, '//*[@id="_ltalicenceenquiry_WAR_foblsportlet_driverDtlsesSearchContainerSearchContainer"]/table/tbody/tr[1]/td[2]/a').click()
    div = BeautifulSoup(driver.find_element(By.TAG_NAME, 'table').get_attribute('outerHTML'),'html.parser')
    table = pd.read_html(StringIO(str(div)))[0]

    table[['Issue Date','Expiry Date']] = table[['Issue Date','Expiry Date']].apply(pd.to_datetime)

    return get_expiry_go(table, driver_type)
  except:
    return pd.NA

def get_expiry_go(table: pd.DataFrame, driver_type: str):
  if driver_type in ['PRIVATE_HIRE','HOURLY_RENTAL']:
    df = table[(table.Status == 'Valid') & (table['Description'].isin(["Taxi Driver's Vocational Licence (TDVL)", "Private Hire Car Driver's Vocational Licence (PDVL)"]))]
    return df["Expiry Date"].max().strftime('%d-%m-%Y')
  elif driver_type == 'TAXI':
    df = table[(table.Status == 'Valid') & (table['Description'] == "Taxi Driver's Vocational Licence (TDVL)")]
    return df['Expiry Date'].max().strftime('%d-%m-%Y')
  else:
    return pd.NA

def main():
  load_dotenv()

  df = pd.read_csv('data/drivers.csv')

  drivers = df.sample(n=500, ignore_index=True)
  drivers.insert(6,'expiry',pd.NA)
  drivers.insert(7,'source',pd.NA)
  drivers.insert(8,'remarks',pd.NA)
  drivers.loc[drivers["birth"].isna(), "remarks"] = "Missing birth date"
  drivers.loc[drivers["nric"].isna(), "remarks"] = "Missing NRIC"

  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  options.add_argument("--no-sandbox")  # This make Chromium more stable in a container environment
  options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
  driver = webdriver.Chrome(options=options)
  driver.get(os.getenv('VL_URL'))

  try:
    for index, row in drivers.loc[drivers['remarks'].isna()].iterrows():
      expiry = retrieve_date(driver,row['nric'],row['birth'],row['type'])
      if pd.isna(expiry):
        drivers.loc[index, 'remarks'] = 'No record found'
      else:
        drivers.loc[index, 'expiry'] = expiry
        drivers.loc[index, 'source'] = "LTA"

  finally:
    for index, row in drivers.loc[drivers['expiry'].isna()].iterrows():
      expiry = retrieve_go(driver, row['vl_id'], row['type'])
      if not pd.isna(expiry):
        drivers.loc[index, 'expiry'] = expiry
        drivers.loc[index, 'source'] = "GoBusiness"
        drivers.loc[index, 'remarks'] = pd.NA

    drivers.loc[(drivers.vl_expiry_date != drivers.expiry) & drivers.remarks.isna(), "remarks"] = 'Mismatched expiry'

    drivers.to_csv(f'vl_daily_{time.strftime("%d_%m_%Y")}.csv')

    bot = SlackBot()

    bot.uploadFile(f'vl_daily_{time.strftime("%d_%m_%Y")}.csv', os.getenv('SLACK_CHANNEL'), f"VL Checks for {time.strftime('%d %b %Y')}")

if __name__ == '__main__':
  main()
