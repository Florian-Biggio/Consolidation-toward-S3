#!/usr/bin/env python
# coding: utf-8

import sys
import json
import re
import argparse
from datetime import datetime
from io import BytesIO
import os

import pandas as pd
import boto3
from pymongo import MongoClient


"""
This script reads an Excel file from an S3 bucket, processes it, and inserts the data into a MongoDB collection.
The Excel file contains weather data from two weather stations: Ichtegem and La Madeleine.
The script can be run with the following command:
```
python xlsx.py [station_name]
```
where `station_name` is either `Ichtegem` or `Madeleine` (don't include the brackets).
If no station name is provided, the script will default to `Ichtegem`.
"""


# Function to load the secrets from the local file
def load_secrets(file_path):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found!")
        sys.exit(1)
    
    with open(file_path, 'r') as file:
        return json.load(file)

secrets = load_secrets('secrets.json')

aws_access_key_id = secrets['AWS_ACCESS_KEY_ID']
aws_secret_access_key = secrets['AWS_SECRET_ACCESS_KEY']
aws_region = secrets['AWS_REGION']

parser = argparse.ArgumentParser(description="Process an Excel file")
parser.add_argument(
    'files',
    nargs='*',  # Accept multiple files (or none)
    default=None,
    help='The name of the station to process. Only accepts Ichtegem or Madeleine'
)
args = parser.parse_args()

# After parsing arguments
print(f"Arguments: {args.files}")  # Debug line to check the parsed arguments


if not args.files:
    args.files = ['Ichtegem']

s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key_id, 
                  aws_secret_access_key=aws_secret_access_key, 
                  region_name=aws_region)


bucket_name = 'greencoop-airbyte'
if args.files[0] == 'Ichtegem':
    file_key = "greencoop-airbyte/Ichtegem.xlsx"
elif args.files[0] == 'Madeleine':
    file_key = "greencoop-airbyte/La8Madeleine8FR.xlsx"

s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
file_content = s3_object['Body'].read()

# Charger le fichier Excel avec pandas
excel_file = pd.ExcelFile(BytesIO(file_content), engine='openpyxl')



# Define column renaming dictionary (with explicit units)
column_mapping = {
    "Time": "time",
    "Temperature": "temperature_°F",
    "Dew Point": "dew_point_°F",
    "Humidity": "humidity_%",
    "Wind": "wind_dir",
    "Speed": "wind_speed_mph",
    "Gust": "wind_gust_mph",
    "Pressure": "pressure_inHg",
    "Precip. Rate.": "precip_rate_in/hr",
    "Precip. Accum.": "precip_accum_in",
    "UV": "uv_index",
    "Solar": "solar_w/m²"
}

def clean_value(value):
    if isinstance(value, str):
        match = re.search(r"[-+]?\d*\.?\d+", value)  # Extract numeric part
        return float(match.group()) if match else None  # Keep None for non-numeric strings
    return value  # Return unchanged if not a string (including NaN)


# List to store processed DataFrames
dfs = []

# Loop through all sheets
for sheet_name in excel_file.sheet_names:
    # Read the current sheet
    df = excel_file.parse(sheet_name, na_values=["", "None", "NA", "NaN"])
    # Rename columns for consistency
    df.rename(columns=column_mapping, inplace=True)
    
    # Apply cleaning function to all columns (except 'time' and 'wind_dir')
    for col in df.columns:
        if col not in ["time", "wind_dir"]:  # Exclude categorical columns
            df[col] = df[col].apply(clean_value)

    df = df.dropna(how='all')

    # Convert sheet name (DDMMAAAA) to date (YYYY-MM-DD)
    date_formatted = pd.to_datetime(sheet_name, format="%d%m%y")
    
    # Add the date column
    df.insert(0, "date", date_formatted)
    df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S").dt.time
    df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    df = df.drop(columns=["date", "time"])
    df = df[["datetime"] + [col for col in df.columns if col != "datetime"]]

    # Append cleaned DataFrame
    dfs.append(df)

# Combine all sheets into one DataFrame
final_df = pd.concat(dfs, ignore_index=True)


def convertToMetric(df):
    """
    Convert to metric and to other small ajustements for all data to be formated the same way
    """
    df = df.copy() 
    	
    df['dew_point_°C'] = ((df['dew_point_°F'] - 32) * 5/9).round(1)
    df['temperature_°C'] = ((df['temperature_°F'] - 32) * 5/9).round(1)
    df['wind_speed_kph'] = (df['wind_speed_mph'] * 1.60934).round(1)
    df['wind_gust_kph'] = (df['wind_gust_mph'] * 1.60934).round(1)
    df['pressure_hPa'] = (df['pressure_inHg'] * 33.8639).round(1)
    df['precip_rate_mm/hr'] = (df['precip_rate_in/hr'] * 25.4).round(1)
    df['precip_accum_mm'] = (df['precip_accum_in'] * 25.4).round(1)
    df['temperature_°C'] = ((df['temperature_°F'] - 32) * 5/9).round(1)

    df['humidity_%'] = df['humidity_%'].astype(int)

    df.drop(columns=['temperature_°F', 'wind_speed_mph', 'wind_gust_mph', 'pressure_inHg', 
                     'precip_rate_in/hr', 'precip_accum_in', 'dew_point_°F'], inplace=True)
    return df


final_df2 = convertToMetric(final_df)

final_df2["precip_rate_mm/hr"].unique()

final_df2["precip_accum_mm"].unique()

final_df2["wind_dir"].unique()   

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["weather_data2"]
collection = db["weather_station2"]

collection.insert_many(final_df2.to_dict(orient='records'))

print("Data successfully inserted into MongoDB!")

