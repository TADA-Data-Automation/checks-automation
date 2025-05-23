import argparse
import os
import re
import time

import pandas as pd
import requests
from dotenv import load_dotenv

from utils.loader import Loader

STATUSES = ["Revoked", "Suspended", "Cancelled", "Application Lapsed", "Approved to Attend Course",
            "Pending Licence Issuance", "Application Withdrawn", "Expired"]


def get_partition(df: pd.DataFrame, partition: int, total_partitions: int = 40):
    chunk_size = len(df) // total_partitions + 1

    return df[partition * chunk_size:(partition + 1) * chunk_size]


def valid_nric(string):
    if type(string) != str:
        return False
    if match := re.match(r'^[STFG]\d{7}[A-Z]$', string):
        return True
    else:
        return False


def valid_dob(string):
    if type(string) != str:
        return False
    try:
        time.strptime(string, "%d-%m-%Y")
        return True
    except ValueError:
        return False


def process_driver_vl_status(json_response, driver_type, car_type=None):
    status = json_response.get("status", {})

    if status.get("statusCode") == 0:
        return ("No record found", None)

    vl_infos = json_response.get("vlInfos", [])
    if not vl_infos:
        return ("No VL record found", None)

    if driver_type in ['PRIVATE_HIRE', 'HOURLY_RENTAL']:
        if not pd.isna(car_type) and car_type == 3001:
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


def main(partition: int):
    load_dotenv()

    loader = Loader()

    loader.decrypt_file('drivers.csv')
    df = pd.read_csv('drivers.csv', dtype={'car_type': 'Int64'})

    df = get_partition(df, partition).copy()

    df["expiry"] = None
    df["remarks"] = None

    for index, row in df.iterrows():
        if not valid_nric(row['nric']) or not valid_dob(row['birth']):
            df.at[index, "remarks"] = "Invalid NRIC or DOB"
            continue
        try:
            status, expiry = get_driver_data(row['nric'], row['birth'], row['type'], row.get('car_type'))
            df.at[index, "remarks"] = status
            df.at[index, "expiry"] = expiry
        except Exception as e:
            print(f"Error processing row {index}: {e}")
            df.at[index, "remarks"] = "Error"

    df.loc[(df.vl_expiry_date != df.expiry) & df.remarks.isna(), "remarks"] = 'Mismatched expiry'
    df.to_csv(f'data/vl_check_{time.strftime("%b")}_{partition}.csv', index=False)

    loader.encrypt_file(f'data/vl_check_{time.strftime("%b")}_{partition}.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--partition', type=int, help='Partition number')
    args = parser.parse_args()
    main(partition=args.partition)