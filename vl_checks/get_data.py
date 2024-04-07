import argparse

from dotenv import load_dotenv

from utils.helpers import Postgres
from utils.loader import Loader


def main(encrypt:bool):
  load_dotenv()

  pg = Postgres()

  query = """
  SELECT
      id,
      type,
      nric,
      TO_CHAR(birth, 'DD-MM-YYYY') AS birth,
      TO_CHAR(vl_expiry_date, 'DD-MM-YYYY') AS vl_expiry_date,
      vl_id
  FROM tada_member_service.driver
  WHERE approved = true
    AND banned = false
    AND test_account = false
    AND region='SG'
    AND type in ('TAXI','HOURLY_RENTAL','PRIVATE_HIRE')
  ORDER BY last_approved_at DESC
  """

  df = pg.to_pandas(query)

  df.to_csv('data/drivers.csv', index=False)

  if encrypt:
    loader = Loader()
    loader.encrypt_file('data/drivers.csv')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--encrypt', action='store_true', default=False)
  args = parser.parse_args()

  main(args.encrypt)