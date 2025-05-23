import os

import requests
from dotenv import load_dotenv

from utils.helpers import Query, Redash

STATUSES = ["Revoked", "Suspended", "Cancelled", "Application Lapsed", "Approved to Attend Course"]

def process_driver_vl_status(json_response, driver_type, car_type=None):
    status = json_response.get("status", {})

    if status.get("statusCode") == 0:
        return ("No record found", None)

    vl_infos = json_response.get("vlInfos", [])
    if not vl_infos:
        return ("No VL record found", None)

    if driver_type in ['PRIVATE_HIRE', 'HOURLY_RENTAL']:
        if car_type == 3001:
            relevant_vls = [vl for vl in vl_infos if vl.get("type") == "Bus Driver's Vocational Licence (BDVL)"]
        else:
            relevant_vls = [
                vl for vl in vl_infos if vl.get("type") in [
                    "Taxi Driver's Vocational Licence (TDVL)",
                    "Private Hire Car Driver's Vocational Licence (PDVL)"
                ]
            ]
    elif driver_type == 'TAXI':
        relevant_vls = [vl for vl in vl_infos if vl.get("type") == "Taxi Driver's Vocational Licence (TDVL)"]
    else:
        return ("Invalid driver type", None)

    if not relevant_vls:
        return ("No valid record of corresponding license", None)

    valid_vls = [vl for vl in relevant_vls if vl.get("status") == "Valid"]
    if valid_vls:
        expiry_dates = [vl.get("expiryDate") for vl in valid_vls if vl.get("expiryDate") and vl.get("expiryDate") != "N.A."]
        if expiry_dates:
            return (None, max(expiry_dates))
        return (None, None)

    for status in STATUSES:
        if any(vl.get("status") == status for vl in relevant_vls):
            return (status, None)

    return ("Unknown status found", None)

def get_driver_data(nric: str, birthday: str, driver_type: str, car_type: int = None):
  url = os.getenv('LTA_URL')
  headers = {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "Origin": os.getenv('LTA_ORIGIN'),
      "Referer": os.getenv('LTA_REFERER'),
      "User-Agent": os.getenv('LTA_USER_AGENT')
  }

  data = {
    "dateOfBirth": birthday,
    "identificationNumber": nric,
    "identificationType": 1
  }
  
  response = requests.post(url, headers=headers, data=data)

  try:
    result = response.json()
  except Exception as e:
    print("Error parsing response JSON:", e)
    return

  status, expiry = process_driver_vl_status(result, driver_type=driver_type, car_type=car_type)
  return status, expiry

def main():
  load_dotenv()

  redash = Redash(os.getenv('REDASH_API_KEY'), os.getenv('REDASH_BASE_URL'))
  redash.run_query(Query(2977))
  df = redash.get_result(2977)

  df.to_csv('data/drivers.csv', index=False)

  df = df[:50].copy()

  df["expiry"] = None
  df["remark"] = None

  for index, row in df.iterrows():
    print(f'Checking {row["nric"]} {row["birth"]} {row["type"]} {row["car_type"]}')
    try:
        status, expiry = get_driver_data(row['nric'], row['birth'], row['type'], row.get('car_type'))
        df.at[index, "remark"] = status
        df.at[index, "expiry"] = expiry
    except Exception as e:
        print(f"Error processing row {index}: {e}")
        df.at[index, "remark"] = "Error"

  df.to_csv('data/drivers_checked.csv', index=False)

if __name__ == '__main__':
  main()