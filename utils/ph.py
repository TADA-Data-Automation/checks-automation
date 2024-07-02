import os

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def fill_form(driver, car_plate):
  wait = WebDriverWait(driver, 5)
  vehicle_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="vehicleNo"]')))
  checkbox = driver.find_element(By.XPATH, '//*[@id="agreeTCbox"]')
  submit_button = driver.find_element(By.XPATH, '//*[@id="button"]')

  vehicle_input.clear()
  vehicle_input.send_keys(car_plate)
  checkbox.click()
  submit_button.click()

  try:
    wait = WebDriverWait(driver, 5)

    status = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="pnlBdyVehicleList"]/div/div/div/div[1]/div[2]/p[2]'))).text
    decal = driver.find_element(By.XPATH, '//*[@id="pnlBdyVehicleList"]/div/div/div/div[2]/div[2]/p[2]').text

    button = driver.find_element(By.XPATH, '//*[@id="main-content"]/div[2]/div[2]/form/div[4]/div[1]/button[2]')

    button.click()

    if status == 'Yes':
      return (1, decal)
    else:
      return (0, pd.NA)
  except:
    return (-1, pd.NA)

def preprocess(df, checked):
  if checked is not None:
    checked = checked[checked['phv'] >= 0].drop(columns=['id'])
    checked.loc[checked['phv'] < 1, 'decal'] = pd.NA

    merged = df.merge(checked, on='car_plate', how='left')
    merged['phv'] = merged['phv'].astype('Int64')
    merged = merged.drop_duplicates().reset_index(drop=True)

    df = merged[merged['phv'].isna()].reset_index(drop=True)

    df_checked = merged[(~merged['phv'].isna()) & (merged['phv'] >= 0)]
    df_checked = df_checked.sort_values(by='phv').reset_index(drop=True)

    if len(df) < 30:
      remaining = 30 - len(df)
      to_check = df_checked.sort_values(['phv', 'updated_at']).drop(columns=['updated_at'])

      df = pd.concat([df, to_check[:remaining]])
    
    return df.copy(), df_checked.copy()
  
  return df.copy(), df.copy()

def process_results(df, prev):
  df['phv'] = df['phv'].astype('Int64')
  df['updated_at'] = pd.to_datetime('today').date()

  known  = df[df['phv'] >= 0]
  df1 = pd.concat([prev, known])

  # convert updated_at to date
  df1['updated_at'] = pd.to_datetime(df1['updated_at']).dt.date

  df1 = df1.sort_values('updated_at')
  df1 = df1.drop_duplicates(['id', 'car_plate'], keep='last')

  df1 = df1.sort_values(['phv', 'updated_at'], ascending=[True, False]).reset_index(drop=True)
  df1.loc[df1['phv'] < 1, 'decal'] = pd.NA

  return df1