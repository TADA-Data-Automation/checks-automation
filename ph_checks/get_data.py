import os
import time

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.loader import Loader
from utils.slack import SlackBot


def main():
  loader = Loader()
  bot = SlackBot()

  redash = Redash(os.getenv('REDASH_API_KEY'), os.getenv('REDASH_BASE_URL'))
  redash.run_query(Query(2984))
  df = redash.get_result(2984)

  df = df.dropna()

  checked = bot.getLatestFile(os.getenv('CACHE_CHANNEL'))

  if checked is not None:
    checked = checked[checked['phv'] >= 0].drop(columns=['id'])
    checked.loc[checked['phv'] < 1, 'decal'] = pd.NA

    merged = df.merge(checked, on='car_plate', how='left')
    merged['phv'] = merged['phv'].astype('Int64')
    merged = merged.drop_duplicates()

    df = merged[merged['phv'].isna()]

    df_checked = merged[(~merged['phv'].isna()) & (merged['phv'] >= 0)]
    df_checked = df_checked.sort_values(by='phv')
    df_checked.to_csv('data/checked.csv', index=False)

    bot.deleteLatestMessage(os.getenv('CACHE_CHANNEL'))
    bot.uploadFile('data/checked.csv', os.getenv('CACHE_CHANNEL'), f'Last Updated: {time.strftime("%Y-%m-%d %H:%M:%S")}')

    if len(df) < 1200:
      remaining = 1200 - len(df)
      to_check = df_checked.sort_values(['phv', 'updated_at']).drop(columns=['updated_at'])

      df = pd.concat([df, to_check[:remaining]])

    df.to_csv('data/car_plates.csv', index=False)
  else:
    df.to_csv('data/car_plates.csv', index=False)

  loader.encrypt_file('data/car_plates.csv')


if __name__ == '__main__':
  load_dotenv()
  main()
