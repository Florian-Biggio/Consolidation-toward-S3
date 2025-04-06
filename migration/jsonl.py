import sys
import boto3
import json
import pandas as pd
from io import BytesIO
import argparse
import logging
from pymongo import MongoClient, errors
from bson import ObjectId
from datetime import datetime
import hashlib
import copy

def load_secrets(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
secrets = load_secrets('secrets.json')

aws_access_key_id = secrets['AWS_ACCESS_KEY_ID']
aws_secret_access_key = secrets['AWS_SECRET_ACCESS_KEY']
aws_region = secrets['AWS_REGION']

s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key_id, 
                  aws_secret_access_key=aws_secret_access_key, 
                  region_name=aws_region)

parser = argparse.ArgumentParser(description="Process an Excel file")
parser.add_argument(
    'file',
    default='InfoClimat',
    help='The name of the station to process. Only accepts InfoClimat'
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


mongodb_address = args.mongodb_address
# After parsing arguments
logger.info(f"Station: {args.file}")  
logger.info(f"MongoDB adress: {mongodb_address}")  # Debug lines to check the parsed arguments


if not args.file:
    args.file = ['InfoClimat']

if args.file == 'InfoClimat':
    file_key = "greencoop-airbyte/Stations_meteorologiques_du_reseau_InfoClimat_(Bergues,_Hazebrouck,_Armentieres,_Lille-Lesquin)/2025_03_14_1741977939508_0.jsonl"


bucket_name = 'greencoop-airbyte'
s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
file_content = s3_object['Body'].read().decode('utf-8')


# Read the JSONL file from S3
response = s3.get_object(Bucket=bucket_name, Key=file_key)
content = response["Body"].read().decode("utf-8")

# Convert JSONL to Pandas DataFrame
json_list = [json.loads(line) for line in content.splitlines()]
df = pd.DataFrame(json_list)

import pandas as pd
from pandas import json_normalize

# Assuming `df_clean` is the output of json_normalize
df_clean = pd.json_normalize(df["_airbyte_data"])

# Expand the 'stations' field
stations_df = pd.json_normalize(df_clean["stations"].explode())

# Expand the 'hourly' fields
hourly_dfs = {}
hourly_cols = [col for col in df_clean.columns if col.startswith("hourly.")]
for col in hourly_cols:
    hourly_dfs[col] = pd.json_normalize(df_clean[col].explode())

data_to_insert = df_clean.to_dict(orient="records")

# ✅ 1. Connect to MongoDB
client = MongoClient(mongodb_address)  # Update if needed
db = client["weather_data"]  # Your database name
collection = db["weather_station"]  # Collection name


# ✅ 2. Load the JSON data (assuming it's in a file or a variable)
weather_data = data_to_insert

# ✅ 3. Extract Relevant Data
stations = weather_data[0].get("stations", [])  # Extract station metadata
hourly_data = {
    key: weather_data[0][key]
    for key in weather_data[0]
    if key.startswith("hourly.") and key != "hourly._params"
}

# ✅ 4. Flatten Hourly Data
flattened_data = []
for station_id, records in hourly_data.items():
    for record in records:
        flattened_data.append(record)  # Convert list of dicts into single list


Id_to_station = {station['id']: station['name'] for station in data_to_insert[0]["stations"]}
for doc in flattened_data:
    doc['station'] = Id_to_station.get(doc['id_station'], 'Unknown')


def process_data(document):
    document = copy.deepcopy(document)
    # Convert strings to appropriate types
    document['dh_utc'] = datetime.strptime(document['dh_utc'], '%Y-%m-%d %H:%M:%S')  # Convert to datetime, must start as str
    # Ensure all fields are converted properly, handling missing keys
    document['temperature'] = float(document['temperature']) if document.get('temperature') else None
    document['pression'] = float(document['pression']) if document.get('pression') else None
    document['humidite'] = int(document['humidite']) if document.get('humidite') else None
    document['point_de_rosee'] = float(document['point_de_rosee']) if document.get('point_de_rosee') else None
    document['visibilite'] = int(document.get('visibilite', 0)) if document.get('visibilite') else None
    document['vent_moyen'] = float(document['vent_moyen']) if document.get('vent_moyen') else None
    document['vent_rafales'] = float(document['vent_rafales']) if document.get('vent_rafales') else None
    document['vent_direction'] = int(document['vent_direction']) if document.get('vent_direction') else None
    document['pluie_3h'] = float(document['pluie_3h']) if document.get('pluie_3h') else None
    document['pluie_1h'] = float(document['pluie_1h']) if document.get('pluie_1h') else None
    document['neige_au_sol'] = float(document.get('neige_au_sol', 0)) if document.get('neige_au_sol') else None
    document['nebulosite'] = str(document.get('nebulosite', '')) if document.get('nebulosite') else ''
    document['temps_omm'] = float(document.get('temps_omm', 0)) if document.get('temps_omm') else None
    

    return document


def process_data2(document):
    # Define a mapping of fields to their conversion functions
    conversion_map = {
        'dh_utc': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if x else None,
        'temperature': lambda x: float(x) if x else None,
        'pression': lambda x: float(x) if x else None,
        'humidite': lambda x: int(x) if x else None,
        'point_de_rosee': lambda x: float(x) if x else None,
        'visibilite': lambda x: int(x) if x else None,
        'vent_moyen': lambda x: float(x) if x else None,
        'vent_rafales': lambda x: float(x) if x else None,
        'vent_direction': lambda x: int(x) if x else None,
        'pluie_3h': lambda x: float(x) if x else None,
        'pluie_1h': lambda x: float(x) if x else None,
        'neige_au_sol': lambda x: float(x) if x else None,
        'nebulosite': lambda x: str(x) if x else '',
        'temps_omm': lambda x: float(x) if x else None
    }

    # Use deepcopy to avoid mutating the original document
    document = copy.deepcopy(document)

    # Loop over the conversion map and apply the transformation if the field exists in the document
    for field, conversion_func in conversion_map.items():
        if field in document:
            document[field] = conversion_func(document[field])

    return document


def rename_columns(document):
    # Column renaming and translation mapping
    column_mapping = {
        'dh_utc': 'datetime',
        'temperature': 'temperature_°C',  # In Celsius
        'pression': 'pressure_hPa',  # In hPa
        'humidite': 'humidity_%',
        'point_de_rosee': 'dew_point_°C',  # In Celsius
        'visibilite': 'visibility_m',  # Assuming meters for visibility
        'vent_moyen': 'wind_speed_kph',  # In km/h
        'vent_rafales': 'wind_gust_kph',  # In km/h
        'vent_direction': 'wind_dir',  # Wind direction (unchanged)
        'pluie_3h': 'precip_rate_mm/hr (3hrs)',  # In mm/hr
        'pluie_1h': 'precip_rate_mm/hr',  # In mm/hr
        'neige_au_sol': 'snow_depth_mm',  # In mm
        'nebulosite': 'cloud_coverage',  # General cloud coverage (string or percentage)
        'temps_omm': 'solar_w/m²'  # Assuming temperature is related to solar irradiance
    }

    # Rename keys based on the mapping
    for old_key, new_key in column_mapping.items():
        if old_key in document:
            document[new_key] = document.pop(old_key)

    return document


# ✅ 5.1. Convert into correct types
for i in flattened_data:
    i = process_data(i)

processed_data = [process_data(doc) for doc in flattened_data]

# ✅ 5.1.5 Convert the column names into the general ones
processed_data = [rename_columns(doc) for doc in processed_data] 

# Function to generate ObjectId from a unique key
def generate_objectid(unique_str):
    hash_hex = hashlib.md5(unique_str.encode()).hexdigest()[:24]  # Ensure 24 chars
    return ObjectId(hash_hex)

# Add the ObjectId to each document in the list
for doc in processed_data:
    unique_str = str(doc['datetime']) + doc['station']
    doc['_id'] = generate_objectid(unique_str)


# Insert documents
try:
    # Insert documents, set 'ordered=False' to continue on duplicate key error
    result = collection.insert_many(processed_data, ordered=False)

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
            logger.debug(f"Duplicate key error: {errmsg}")

            # Extract the duplicate key information from the error details
            dup_id = error.get('keyValue', {}).get('_id', 'unknown')
            logger.debug(f"Duplicate _id: {dup_id}")

            # Log the failed document data for duplicate key errors
            logger.debug(f"Failed document data for duplicate key: {error.get('op', {})}")
            duplicate_key_errors_handled += 1

    if duplicate_count > 3:
        logger.debug(f"...and {duplicate_count - 3} more duplicates were skipped.")

    # Log the first 3 validation errors
    validation_errors_handled = 0
    for error in write_errors:
        if error.get('code') == 121 and validation_errors_handled < 3:  # Validation error
            errmsg = error.get('errmsg', 'No detailed message available')
            logger.debug(f"Validation failed for document: {errmsg}")
            logger.debug(f"Failed document data for validation error: {error.get('op', {})}")
            validation_errors_handled += 1

    if validation_count > 3:
        logger.debug(f"...and {validation_count - 3} more validation errors occurred.")

    # If there are any other errors (non-validation, non-duplicate), log them
    if other_count > 0:
        logger.debug(f"...and {other_count} other errors occurred.")

