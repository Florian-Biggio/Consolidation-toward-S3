#!/usr/bin/env python
# coding: utf-8

import sys
import json
import re
import argparse
from datetime import datetime
from io import BytesIO
import os
import hashlib
import logging

import pandas as pd
import boto3
from pymongo import MongoClient, errors
from bson import ObjectId


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
parser = argparse.ArgumentParser(description="Process an Excel file")
parser.add_argument(
    'file',
    default='Ichtegem',
    help='The name of the station to process. Only accepts Ichtegem or Madeleine'
)

parser.add_argument(
    "--mongodb_address", 
    default="mongodb://localhost:27017/", 
    help="The MongoDB address (default: mongodb://localhost:27017/)"
)

def upper_case(string):
    return string.upper()
parser.add_argument(
    "-v", "--verbosity",
    type=upper_case,
    default="INFO", 
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    help="Set the logging verbosity level (default: INFO)"
)

args = parser.parse_args()

log_level = getattr(logging, args.verbosity)
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)


def load_secrets(file_path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, file_path)
    with open(full_path, 'r') as file:
        return json.load(file)

secrets = load_secrets('secrets.json')

aws_access_key_id = secrets['AWS_ACCESS_KEY_ID']
aws_secret_access_key = secrets['AWS_SECRET_ACCESS_KEY']
aws_region = secrets['AWS_REGION']

mongodb_address = args.mongodb_address
# After parsing arguments
logger.info(f"Station: {args.file}")
logger.info(f"MongoDB adress: {mongodb_address}")  # Debug lines to check the parsed arguments

if not args.file:
    args.file = ['Ichtegem']

if args.file == 'Ichtegem':
    file_key = "greencoop-airbyte/Ichtegem.xlsx"
elif args.file == 'Madeleine':
    file_key = "greencoop-airbyte/La8Madeleine8FR.xlsx"

s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key_id, 
                  aws_secret_access_key=aws_secret_access_key, 
                  region_name=aws_region)


bucket_name = 'greencoop-airbyte'


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

    df['humidity_%'] = df['humidity_%'].astype(int)

    df.drop(columns=['temperature_°F', 'wind_speed_mph', 'wind_gust_mph', 'pressure_inHg', 
                     'precip_rate_in/hr', 'precip_accum_in', 'dew_point_°F'], inplace=True)
    return df


final_df2 = convertToMetric(final_df)

def wind_dir_to_angle(df):
    df = df.copy()
    dir_to_angle = {
        'N': 0,
        'NNE': 22.5,
        'NE': 45,
        'ENE': 67.5,
        'E': 90,
        'ESE': 112.5,
        'SE': 135,
        'SSE': 157.5,
        'S': 180,
        'SSW': 202.5,
        'SW': 225,
        'WSW': 247.5,
        'W': 270,
        'WNW': 292.5,
        'NW': 315,
        'NNW': 337.5,
        'North': 0,
        'South': 180,
        'East': 90,
        'West': 270   
    }
    df['wind_dir'] = df['wind_dir'].map(dir_to_angle)
    return df
final_df2 = wind_dir_to_angle(final_df2)

final_df2["precip_rate_mm/hr"].unique()

final_df2["precip_accum_mm"].unique()

final_df2["wind_dir"].unique()   

final_df2['station'] = args.file

# Function to generate ObjectId from a unique key
def generate_objectid(unique_str):
    hash_hex = hashlib.md5(unique_str.encode()).hexdigest()[:24]  # Ensure 24 chars
    return ObjectId(hash_hex)

final_df2['_id'] = final_df2.apply(lambda row: generate_objectid(str(row['datetime']) + row['station']), axis=1)

final_df2 = final_df2[['station', 'datetime', 'temperature_°C', 'dew_point_°C', 'humidity_%', 'wind_dir', 'wind_speed_kph', 
         'wind_gust_kph', 'pressure_hPa', 'precip_rate_mm/hr', 'precip_accum_mm',  
         'uv_index', 'solar_w/m²',  '_id']]   

migration_tag = f"{datetime.now().strftime('%Y-%m-%d_%Hh%M')}_{args.file}"
final_df2["migrated"] = migration_tag

# MongoDB setup
client = MongoClient(mongodb_address)
db = client["weather_data"]
collection = db["weather_station"]


records = final_df2.to_dict(orient='records')

# Prepare info to make sure the data is rightly migrated
columns_of_interest = ["temperature_°C", "humidity_%", "pressure_hPa"]
metrics = {
    "migration_tag": migration_tag,
    "mongodb_address": mongodb_address,
    "row_count": len(final_df2),
    "columns": final_df2.columns.tolist(),
}
for col in columns_of_interest:
    if col in final_df2.columns:
        metrics[f"median_{col}"] = float(final_df2[col].median())
        metrics[f"min_{col}"] = float(final_df2[col].min())
        metrics[f"max_{col}"] = float(final_df2[col].max())

script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(script_dir, "..", "tests","test_data")
file_path = os.path.join(data_dir, f"expected_{args.file}_metrics.json")



# Insert documents
try:
    # Insert documents, set 'ordered=False' to continue on duplicate key error
    result = collection.insert_many(records, ordered=False)

    # Log the number of inserted documents
    inserted_count = len(result.inserted_ids)
    logger.info(f"Successfully inserted {inserted_count} documents into MongoDB at {mongodb_address} !")

except errors.BulkWriteError as e:
    # Extract useful summary info without dumping full error
    inserted_count = e.details.get('nInserted', 0)
    write_errors = e.details.get('writeErrors', [])
    duplicate_count = 0
    validation_count = 0
    other_count = 0

    # Separate errors into categories
    for error in write_errors:
        if error.get('code') == 11000:  # Duplicate key error
            duplicate_count += 1
        elif error.get('code') == 121:  # Validation error
            validation_count += 1
        else:  # Other errors
            other_count += 1

    # Log the counts of different error types
    logger.warning(f"Duplicate key error: {duplicate_count} documents were skipped.")
    logger.warning(f"Validation error: {validation_count} documents failed validation.")
    logger.warning(f"Other errors: {other_count} documents encountered other errors.")

    # Successfully inserted documents
    logger.info(f"{inserted_count} documents were successfully inserted despite this error.")

    # Log the first 3 duplicate _id values
    duplicate_key_errors_handled = 0
    for error in write_errors:
        if error.get('code') == 11000 and duplicate_key_errors_handled < 3:  # Duplicate key error
            errmsg = error.get('errmsg', 'No detailed message available')
            logger.info(f"Duplicate key error: {errmsg}")

            # Extract the duplicate key information from the error details
            dup_id = error.get('keyValue', {}).get('_id', 'unknown')
            logger.debug(f"Duplicate _id: {dup_id}")

            # Log the failed document data for duplicate key errors
            logger.info(f"Failed document data for duplicate key: {error.get('op', {})}")
            duplicate_key_errors_handled += 1

    if duplicate_count > 3:
        logger.info(f"...and {duplicate_count - 3} more duplicates were skipped.")

    # Log the first 3 validation errors
    validation_errors_handled = 0
    for error in write_errors:
        if error.get('code') == 121 and validation_errors_handled < 3:  # Validation error
            errmsg = error.get('errmsg', 'No detailed message available')
            logger.info(f"Validation failed for document: {errmsg}")
            logger.info(f"Failed document data for validation error: {error.get('op', {})}")
            validation_errors_handled += 1

    if validation_count > 3:
        logger.debug(f"...and {validation_count - 3} more validation errors occurred.")

    # If there are any other errors (non-validation, non-duplicate), log them
    if other_count > 0:
        logger.debug(f"...and {other_count} other errors occurred.")
