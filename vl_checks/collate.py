import glob
import os
import time

import pandas as pd
from dotenv import load_dotenv

from utils.loader import Loader
from utils.slack import SlackBot


def collate_csv():
  pattern = '**/vl_monthly*.csv'
  files = glob.glob(pattern, recursive=True)

  for file in files:
    loader = Loader()
    loader.decrypt_file(file)

  return pd.concat([pd.read_csv(file) for file in files])

def main():
  load_dotenv()

  output_file = f'data/vl_monthly_{time.strftime("%m_%Y")}.csv'

  df = collate_csv(output_file)

  df.to_csv(output_file, index=False)

  bot = SlackBot()

  bot.uploadFile(output_file, os.getenv('SLACK_CHANNEL'), f"VL Checks for {time.strftime('%b %Y')}")

if __name__ == '__main__':
  main()

