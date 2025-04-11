import json

def load_expected_metrics(metrics_file: str):
    """
    Load the expected migration metrics from a JSON file.

    Args:
        metrics_file (str): Path to the JSON file containing expected metrics.
    
    Returns:
        dict: A dictionary containing the migration metrics and mongodb_address.
    """
    with open(metrics_file, encoding="utf-8") as f:
        metrics = json.load(f)
    
    mongodb_address = metrics.get("mongodb_address")
    if not mongodb_address:
        raise ValueError(f"Missing 'mongodb_address' in {metrics_file}")

    return metrics
