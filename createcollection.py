from pymongo import MongoClient
import json

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')  # Change the URI if needed
db = client['weather_data2']  # Use your database name
collection_name = 'weather_stations2'

# Define the schema validation
with open('schema.json', 'r') as schema_file:
    validation_schema = json.load(schema_file)
    
# Create the collection with schema validation
db.create_collection(collection_name, validator=validation_schema)

print(f"Collection {collection_name} created with schema validation.")
