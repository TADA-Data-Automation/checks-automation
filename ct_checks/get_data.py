from dotenv import load_dotenv

from utils.helpers import Postgres

load_dotenv()

pg = Postgres()

query = """
WITH driver AS
  (SELECT
    id,
    type,
    approved_at
  FROM tada_member_service.driver
  WHERE banned = false
    AND test_account = false
    AND approved = true
    AND region = 'SG')
, doc AS
  (SELECT
    doc.owner_user_id,
    d.approved_at,
    d.type as driver_type,
    doc.fields ->> 'car_maker' as car_maker,
    doc.fields ->> 'car_model' as car_model,
    doc.fields ->> 'engine_type' as engine_type,
    doc.fields ->> 'car_type' as car_type
  FROM tada_member_service.driver_document_fields doc join driver d on (d.id = doc.owner_user_id))

SELECT
  owner_user_id,
  driver_type,
  LOWER(car_maker) AS car_maker,
  LOWER(car_model) AS car_model,
  car_type
FROM doc
WHERE driver_type in ('TAXI','PRIVATE_HIRE')
ORDER BY approved_at DESC
"""

df = pg.to_pandas(query)

df.to_csv('data/drivers.csv', index=False)
