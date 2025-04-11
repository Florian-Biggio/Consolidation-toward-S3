import pytest
import pymongo
import statistics
import os
from utils import load_expected_metrics

@pytest.fixture
def expected_metrics(request):
    input_name = request.config.getoption("--input")
    if input_name is None:
        raise ValueError("Missing required argument: --input")
    
    # Build path relative to the current file location
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "test_data", f"expected_{input_name}_metrics.json")

    return load_expected_metrics(file_path)

@pytest.fixture
def mongo(expected_metrics):
    client = pymongo.MongoClient(expected_metrics["mongodb_address"])
    db = client["weather_data"]
    yield db
    client.close()


@pytest.fixture
def migrated_docs(mongo, expected_metrics):
    collection = mongo["weather_station"]
    tag = expected_metrics["migration_tag"]
    return list(collection.find({"migrated": tag}))


def test_row_count(migrated_docs, expected_metrics):
    assert len(migrated_docs) == expected_metrics["row_count"], (
        f"Expected {expected_metrics['row_count']} rows, got {len(migrated_docs)}"
    )


def test_median_temperature(migrated_docs, expected_metrics):
    temps = [doc["temperature_°C"] for doc in migrated_docs if "temperature_°C" in doc]
    assert round(statistics.median(temps), 1) == round(expected_metrics["median_temperature_°C"], 1), (
        f"Expected median temp {expected_metrics['median_temperature_°C']}, got {statistics.median(temps)}"
    )


def test_min_temperature(migrated_docs, expected_metrics):
    temps = [doc["temperature_°C"] for doc in migrated_docs if "temperature_°C" in doc]
    assert min(temps) == expected_metrics["min_temperature_°C"], (
        f"Expected min temp {expected_metrics['min_temperature_°C']}, got {min(temps)}"
    )


def test_max_temperature(migrated_docs, expected_metrics):
    temps = [doc["temperature_°C"] for doc in migrated_docs if "temperature_°C" in doc]
    assert max(temps) == expected_metrics["max_temperature_°C"], (
        f"Expected max temp {expected_metrics['max_temperature_°C']}, got {max(temps)}"
    )
