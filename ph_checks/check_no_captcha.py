import argparse
import os

import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver

from utils.loader import Loader
from utils.ph import fill_form


def get_partition(df: pd.DataFrame, partition:int, total_partitions: int=40):
  chunk_size = len(df) // total_partitions + 1

  return df[partition*chunk_size:(partition+1)*chunk_size]

def main(df, partition):
  df = df.copy()

  options = webdriver.ChromeOptions()
  options.add_argument("--headless")
  options.add_argument("--no-sandbox")
  options.add_argument("--disable-dev-shm-usage")
  driver = webdriver.Chrome(options=options)
  driver.get(os.getenv('PH_URL'))

  for i, row in df.iterrows():
    status, decal = fill_form(driver, row['car_plate'])

    df.at[i,'phv'] = status
    if status == 1:
      df.at[i,'decal'] = decal

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

  df = df[:1000]

  parser = argparse.ArgumentParser()
  parser.add_argument('--partition', type=int, help='Partition number')
  args = parser.parse_args()

  df = get_partition(df, args.partition)

  main(df, args.partition)

  loader.encrypt_file(f'data/ph_check_{args.partition}.csv')
