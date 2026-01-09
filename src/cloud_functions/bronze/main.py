import os
import json
import requests
from google.cloud import storage
from datetime import datetime

# Setup configuration
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME")
API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = "bitcoin,ethereum,solana"

def ingest_bronze(request):
    print(f"Starting Bronze Ingestion at {datetime.now()}")

    # 1. Fetch data
    params = {
        "ids": COINS,
        "vs_currencies": "usd",
        "include_24hr_vol": "true"
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"Data fetched successfully from CoinGecko")
    except Exception as error:
        print(f"API error: {error}")
        return f"API error: {error}", 500
    
    # 2. Add timestamp (Metadata)
    data['_metadata'] = {
        "ingested_at": datetime.now().isoformat(),
        "source": "coingecko"
    }

    # 3. Save to Google Cloud Storage (GCS)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # Create a unique filename
        filename = f"raw_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        blob = bucket.blob(filename)

        # Update string content directly
        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )
        print(f"✅ Uploaded to gs://{BUCKET_NAME}/{filename}")

        return "Ingestion successful", 200

    except Exception as error:
        print(f"Storage error: {error}")
        return f"Storage error: {error}", 500
