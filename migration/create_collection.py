import os
from pymongo import MongoClient, errors
import argparse
import json
import logging

logging.basicConfig(level=logging.INFO)  # You can adjust the logging level (e.g., DEBUG, INFO, ERROR)
logger = logging.getLogger()

parser = argparse.ArgumentParser(description="Create a mongoDB collection with schema validation. Drop the collection if it already exists.")
parser.add_argument(
    "--mongodb_address", 
    default="mongodb://localhost:27017/", 
    help="The MongoDB address (default: mongodb://localhost:27017/)"
)
args = parser.parse_args()

client = MongoClient(args.mongodb_address)
db = client["weather_data"]
collection_name = "weather_station"

try:
    # Drop the collection if it already exists
    db.drop_collection(collection_name)
    logger.info(f"Collection '{collection_name}' dropped (if it existed).")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_dir, 'schema.json')
    # Load schema from schema.json
    with open(schema_path, 'r', encoding='utf-8') as schema_file:
        validation_schema = json.load(schema_file)

    # Create the collection with schema validation
    db.create_collection(
        collection_name,
        validator=validation_schema,
        validationLevel="moderate"  # accept but file a warning, "strict" for full enforcement
    )

    logger.info(f"Collection '{collection_name}' created with schema validation.")
except errors.PyMongoError as e:
    logger.error(f"An error occurred: {e}")
