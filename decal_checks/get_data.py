import argparse
import os

from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.loader import Loader


def main(encrypt:bool):
  load_dotenv()

  redash = Redash(os.getenv('REDASH_API_KEY'), os.getenv('REDASH_BASE_URL'))
  redash.run_query(Query(2984))
  df = redash.get_result(2984)

  df.to_csv('data/drivers.csv', index=False)

  if encrypt:
    loader = Loader()
    loader.encrypt_file('data/drivers.csv')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--encrypt', action='store_true', default=False)
  args = parser.parse_args()

  main(args.encrypt)