import os
import time
from collections import OrderedDict

import pandas as pd
from dotenv import load_dotenv
from thefuzz import fuzz, process

from utils.slack import SlackBot

load_dotenv()

cars = pd.read_csv('ct_checks/car_types.csv')

CAR_LIST = cars['car'].to_list()

TAXI_TYPES = {
  0: 10000,
  1: 10001,
  4: 10003,
  13: 10002,
}

df = pd.read_csv('data/drivers.csv')

df['car_maker'] = df['car_maker'].str.replace('/',' ')
df['car_model'] = df['car_model'].str.replace('/',' ')
df['make_model'] = df.car_maker.str.cat(df.car_model, sep=' ', na_rep='')
df['make_model'] = df['make_model'].apply(lambda x: ' '.join(list(OrderedDict.fromkeys(str(x).split()))))
df['car_type'] = df['car_type'].astype('Int64')

df.drop(['car_maker','car_model'],axis=1,inplace=True)


for index, row in df.loc[df['make_model'] != ''].iterrows():
  match, score = process.extractOne(row['make_model'],CAR_LIST,scorer=fuzz.token_sort_ratio)
  matched_car = cars.loc[cars['car'] == match]
  df.loc[index, 'matched_model'] = match
  df.loc[index, 'matched_type'] = matched_car['type'].values[0] if row['driver_type'] == 'PRIVATE_HIRE' else TAXI_TYPES[matched_car['type'].values[0]]
  df.loc[index, 'confidence'] = score

df.loc[(df['car_type'] != df['matched_type']) | (df['car_type'].isna()), 'mismatch'] = 1

matched_correct = df[(df['confidence'] == 100) & (df['mismatch'].isna())].drop(['confidence','mismatch'],axis=1)
matched_incorrect = df[(df['confidence'] == 100) & (df['mismatch'] == 1)].drop(['confidence','mismatch'],axis=1)

unmatched = df[df['confidence'] < 100]
unmatched = unmatched.sort_values('confidence', ascending=False)

# Path to the Excel file you want to create
file_path = f'data/ct_checks_{time.strftime("%m_%Y")}.xlsx'

# Using ExcelWriter to write to multiple sheets
with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
  unmatched.to_excel(writer, sheet_name='unmatched', index=False)
  matched_incorrect.to_excel(writer, sheet_name='matched_incorrect', index=False)
  matched_correct.to_excel(writer, sheet_name='matched_correct', index=False)

# Upload the file to Slack
bot = SlackBot()

bot.uploadFile(file_path, os.getenv('SLACK_CHANNEL'), f"Car Type Checks for {time.strftime('%b %Y')}")

