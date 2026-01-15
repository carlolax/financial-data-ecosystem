import functions_framework
from google.cloud import storage
import requests
import json
from datetime import datetime
import os

# Setup config
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "crypto-bronze-crypto-platform-carlo-2026")

@functions_framework.http
def ingest_bronze(request):
    print(f"Starting Bronze Ingestion at {datetime.now()}")

    # Fetch data from CoinGecko
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana",
        "vs_currencies": "usd",
        "include_24hr_vol": "true"
    }

    try:
        response = requests.get(url, params=params, timeout=10)

        # Checks if status_code is 400 or 500 and raises an error
        if response.status_code != 200:
             response.raise_for_status() 

        data = response.json()
        print("Data fetched successfully from CoinGecko")

        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"raw_prices_{timestamp}.json"
        blob = bucket.blob(blob_name)

        blob.upload_from_string(
            data=json.dumps(data),
            content_type="application/json"
        )

        print(f"Uploaded to gs://{BUCKET_NAME}/{blob_name}")
        return f"Success: {blob_name}", 200

    except Exception as error:
        print(f"Error: {error}")
        raise error
    