# tests/test_migration.py

import pytest

# Teste si les colonnes attendues sont présentes
def test_columns_present(final_df2):
    required_columns = ['datetime', 'temperature_°C', 'humidity_%', 'pressure_hPa', 'wind_speed_kph', 'station']
    for col in required_columns:
        assert col in final_df2.columns, f"Column {col} is missing in the dataset"

# Teste si les types de données sont corrects
def test_data_types(final_df2):
    assert pd.api.types.is_datetime64_any_dtype(final_df2['datetime']), "Datetime column is not in the correct format"
    assert pd.api.types.is_numeric_dtype(final_df2['temperature_°C']), "Temperature column is not numeric"
    assert pd.api.types.is_numeric_dtype(final_df2['humidity_%']), "Humidity column is not numeric"

# Teste si aucune valeur manquante (NaN) n'est présente
def test_no_missing_values(final_df2):
    assert final_df2.notnull().all().all(), "There are missing values in the dataset"

# Teste si les données sont uniques (sans doublons)
def test_no_duplicates(final_df2):
    assert final_df2.duplicated().sum() == 0, "There are duplicate rows in the dataset"

# Teste si les valeurs sont dans les plages valides
def test_valid_data_ranges(final_df2):
    assert final_df2['humidity_%'].between(0, 100).all(), "Humidity values are out of range"
    assert final_df2['temperature_°C'].between(-50, 50).all(), "Temperature values are out of range"




from pymongo import MongoClient

@pytest.fixture
def mongo_collection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["weather_data2"]
    collection = db["weather_station2"]
    return collection

# Teste si les documents ont bien été insérés
def test_documents_inserted(mongo_collection, final_df2):
    # Insertion des données dans MongoDB
    mongo_collection.insert_many(final_df2.to_dict(orient='records'))

    # Vérifie que les documents ont été insérés
    count = mongo_collection.count_documents({})
    assert count == len(final_df2), f"Expected {len(final_df2)} documents, but found {count} in MongoDB"

# Teste si l'ID des documents est unique
def test_unique_ids(mongo_collection, final_df2):
    ids = [str(doc['_id']) for doc in final_df2.to_dict(orient='records')]
    assert len(ids) == len(set(ids)), "There are duplicate _id values in the inserted documents"




def test_data_retrieval(mongo_collection, final_df2):
    # Insertion des données
    mongo_collection.insert_many(final_df2.to_dict(orient='records'))

    # Récupération des données
    retrieved_data = list(mongo_collection.find())

    # Vérifie que le nombre de documents récupérés correspond à celui inséré
    assert len(retrieved_data) == len(final_df2), "The number of documents retrieved doesn't match the number inserted"
    
    # Vérifie que les champs importants sont présents dans les documents récupérés
    for doc in retrieved_data:
        assert 'datetime' in doc, "Missing datetime field"
        assert 'temperature_°C' in doc, "Missing temperature field"
        assert 'humidity_%' in doc, "Missing humidity field"




def test_data_integrity_after_migration(mongo_collection, final_df2):
    # Insertion des données dans MongoDB
    mongo_collection.insert_many(final_df2.to_dict(orient='records'))

    # Récupérer les données à partir de MongoDB
    migrated_data = list(mongo_collection.find())

    # Vérifie que le nombre de documents dans MongoDB est correct
    assert len(migrated_data) == len(final_df2), "Mismatch in document count after migration"

    # Compare les valeurs avant et après migration
    for i, doc in enumerate(migrated_data):
        assert doc['datetime'] == final_df2.iloc[i]['datetime'], f"Mismatch in datetime for row {i}"
        assert doc['temperature_°C'] == final_df2.iloc[i]['temperature_°C'], f"Mismatch in temperature for row {i}"
        assert doc['humidity_%'] == final_df2.iloc[i]['humidity_%'], f"Mismatch in humidity for row {i}"
