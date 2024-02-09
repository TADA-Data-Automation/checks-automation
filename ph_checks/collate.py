import os
import time

from dotenv import load_dotenv

from utils.slack import SlackBot


def main():
  bot = SlackBot()

  output_file = f'data/ph_monthly_{time.strftime("%m_%Y")}.csv'

  df = bot.getLatestFile(os.getenv('CACHE_CHANNEL'))

  df.to_csv(output_file, index=False)

  bot.uploadFile(output_file, os.getenv('SLACK_CHANNEL'), f"PH Checks for {time.strftime('%b %Y')}")

if __name__ == '__main__':
  load_dotenv()
  main()
