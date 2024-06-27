import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def fill_form(driver, car_plate):
  wait = WebDriverWait(driver, 5)
  vehicle_input = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="vehicleNo"]')))
  checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="agreeTCbox"]')))
  submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="button"]')))

  vehicle_input.clear()
  vehicle_input.send_keys(car_plate)
  checkbox.click()
  submit_button.click()

  try:
    wait = WebDriverWait(driver, 5)

    status = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="pnlBdyVehicleList"]/div/div/div/div[1]/div[2]/p[2]'))).text
    decal = driver.find_element(By.XPATH, '//*[@id="pnlBdyVehicleList"]/div/div/div/div[2]/div[2]/p[2]').text

    button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main-content"]/div[2]/div[2]/form/div[4]/div[1]/button[2]')))

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
    merged = merged.drop_duplicates()

    df = merged[merged['phv'].isna()]

    df_checked = merged[(~merged['phv'].isna()) & (merged['phv'] >= 0)]
    df_checked = df_checked.sort_values(by='phv')

    if len(df) < 50:
      remaining = 50 - len(df)
      to_check = df_checked.sort_values(['phv', 'updated_at']).drop(columns=['updated_at'])

      df = pd.concat([df, to_check[:remaining]])
  
  return df.copy(), checked if checked is not None else df.copy()

def process_results(df, prev):
  unknown = df[df['phv'] < 0]
  known  = df[df['phv'] >= 0]

  df1 = pd.concat([unknown, prev])

  df1 = df1.sort_values(['phv', 'updated_at'])
  df1 = df1.drop_duplicates(['id', 'car_plate'], keep='last')

  df1 = pd.concat([df1, known])

  # convert updated_at to date
  df1['updated_at'] = pd.to_datetime(df1['updated_at']).dt.date

  df1 = df1.sort_values('updated_at')
  df1 = df1.drop_duplicates(['id', 'car_plate'], keep='last')

  df1 = df1.sort_values(['phv', 'updated_at'], ascending=[True, False]).reset_index(drop=True)
  df1.loc[df1['phv'] < 1, 'decal'] = pd.NA

  return df1