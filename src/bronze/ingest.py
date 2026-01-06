import requests
import json
from datetime import datetime
from google.cloud import storage

# Setup Config
BUCKET_NAME = "crypto-lake-carlo-2026-v1" 
COINS = ["bitcoin", "ethereum", "solana"]
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

def fetch_prices():
    coingecko_params = {
        "ids": ",".join(COINS),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }

    try:
        response = requests.get(COINGECKO_URL, params=coingecko_params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as request_error:
        print(f"API Error: {request_error}")
        return None

def upload_to_gcs(data, bucket_name):
    # Create a filename with a timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"raw_data/prices_{timestamp}.json"

    # Connect to the client
    storage_client = storage.Client()
    client_bucket = storage_client.bucket(bucket_name)
    blob = client_bucket.blob(filename)

    # Upload the data
    blob.upload_from_string(
        data=json.dumps(data, indent=4),
        content_type="application/json"
    )

    print(f"Data uploaded to: gs://{bucket_name}/{filename}")

if __name__ == "__main__":
    print(f"Starting ingestion process.")
    price_data = fetch_prices()

    if price_data:
        try:
            upload_to_gcs(price_data, BUCKET_NAME)
        except Exception as upload_error:
            print(f"Error uploading to GCS: {upload_error}")
    else:
        print("No data to save.")