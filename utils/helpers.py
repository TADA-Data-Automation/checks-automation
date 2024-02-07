import os

import pandas as pd
import psycopg2


class Postgres():
  def __init__(self) -> None:
    self.__conn = psycopg2.connect(
      host=os.getenv("POSTGRES_HOST"),
      port = 5432,
      database=os.getenv("POSTGRES_DB"),
      user=os.getenv("POSTGRES_USER"),
      password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Open a cursor to perform database operations
    self.__cur = self.__conn.cursor()
  
  def to_pandas(self, query:str) -> pd.DataFrame:
    self.__cur.execute(query)
    df = pd.DataFrame(self.__cur.fetchall(), columns=[desc[0] for desc in self.__cur.description])

    # change decimal to float
    for col in df.columns:
      if df[col].dtype == 'object':
        try:
          df[col] = df[col].astype(float)
        except:
          pass
    return df

  def from_file(self, path:str, params:dict) -> pd.DataFrame:
    with open(path, 'r') as f:
      query = f.read()
    
    query = query.format(**params)

    return self.to_pandas(query)
