import functions_framework
from google.cloud import storage
import requests
import json
import time
import math
import os
from datetime import datetime, timezone
from typing import Tuple

# --- CONFIGURATION ---
BRONZE_BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME")

# --- CONSTANTS ---
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
BATCH_INGEST_DATA = 50
BATCH_RATE_LIMIT = 5

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def batch_ingest_data(coin_ids: list) -> list:
    """
    Fetches market data for a specific list of Coin IDs from CoinGecko.

    ERROR HANDLING STRATEGY: "Graceful Degradation"
    -----------------------------------------------
    In the Cloud environment, I return an empty list [] instead of raising 
    an exception when I hit a Rate Limit (429).

    How this works:
    1. Prevent Retry Storms: If I raise an Exception, Google Cloud (Scheduler/PubSub)
       interprets it as a 'System Failure' and automatically retries the function immediately.
       This would hit the API again, extending the ban duration.
    2. Partial Success: I want to save the batches I *did* successfully fetch, 
       rather than discarding everything because one batch failed.
    """
    ingest_params = {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d",
        "locale": "en"
    }

    # Added a slight timeout increase for cloud stability.
    coingecko_response = requests.get(COINGECKO_API_URL, params=ingest_params, timeout=15)

    if coingecko_response.status_code == 429:
        # Cloud: Return an empty list to avoid triggering an automatic Cloud Retry loop.
        print("🚨 Rate limit hits (429). Ingestion is going too fast.")
        return []

    coingecko_response.raise_for_status()
    return coingecko_response.json()

@functions_framework.http
def process_ingest_data(request) -> Tuple[str, int]:
    """
    Orchestrates the Bronze Layer ingestion process for the Cloud Environment.
    Triggered via HTTP request (Cloud Scheduler or manual Curl).

    WORKFLOW:
    1. Input Parsing (Dynamic Override):
       - Checks the HTTP Request for a 'coins' parameter (JSON body or URL Arg).
       - If found, ingests ONLY those coins (useful for backfilling specific assets).
       - If missing, defaults to the 'CRYPTO_COINS' environment variable.
    2. Batching & Fetching:
       - Logic: Graceful Degradation (returns empty list on error) to prevent Cloud Retry Storms.
    3. Lineage: Injects 'ingested_timestamp' (UTC) into every record.
    4. Storage: Uploads the final JSON directly to the Google Cloud Storage (GCS) Bronze Bucket.

    Args:
        request (flask.Request): The HTTP request object containing optional JSON/Args.

    Returns:
        Tuple[str, int]: A status message and an HTTP status code (e.g., 200, 500).
    """
    print(f"🚀 Starting Bronze Layer - Cloud Batch Ingestion.")

    if not BRONZE_BUCKET_NAME:
        print("❌ Error: BRONZE_BUCKET_NAME environment variable not set.")
        return "Error: Bucket Env Var Missing", 500

    capture_current_time = datetime.now(timezone.utc)
    ingest_timestamp = capture_current_time.strftime("%Y%m%d_%H%M%S")

    target_crypto_coins = TARGET_CRYPTO_COINS

    # Try parsing JSON body first.
    request_json = request.get_json(silent=True)

    if request_json and 'coins' in request_json:
        target_crypto_coins = request_json['coins']
        print(f"🔧 Manual Override Detected: Ingesting specific coins: {target_crypto_coins}")
    elif request.args and 'coins' in request.args:
        target_crypto_coins = request.args['coins']
        print(f"🔧 URL Parameter Detected: Ingesting specific coins: {target_crypto_coins}")

    # Clean and split the string into a list.
    crypto_coin_list = [c.strip() for c in target_crypto_coins.split(",")]
    total_crypto_coins = len(crypto_coin_list)

    # Calculate batches
    total_ingestion_batches = math.ceil(total_crypto_coins / BATCH_INGEST_DATA)

    print(f"📋 Total Coins: {total_crypto_coins} | Batches: {total_ingestion_batches}")

    all_ingest_data = []

    # Loop through in chunks
    for crypto_index in range(0, total_crypto_coins, BATCH_INGEST_DATA):
        current_chunk = crypto_coin_list[crypto_index : crypto_index + BATCH_INGEST_DATA]
        current_batch_count = (crypto_index // BATCH_INGEST_DATA) + 1

        print(f"🔄 Fetching batch {current_batch_count} of {total_ingestion_batches} ({len(current_chunk)} coins).")

        try:
            batch_data = batch_ingest_data(current_chunk)

            if batch_data:
                all_ingest_data.extend(batch_data)
                print(f"✅ Success. Ingested {len(batch_data)} records.")
            else:
                print(f"⚠️ Warning: Batch {current_batch_count} returned no data.")

            if current_batch_count < total_ingestion_batches:
                print(f"😴 Batch Rate Limit: {BATCH_RATE_LIMIT}s to respect API limits.")
                time.sleep(BATCH_RATE_LIMIT)

        except Exception as error:
            print(f"❌ Error ingesting batch: {error}.")
            return f"Error ingesting batch: {error}", 500

    # --- INJECT LINEAGE ---
    print("💉 Adding 'ingested_timestamp' into records.")
    for data in all_ingest_data:
        data['ingested_timestamp'] = capture_current_time.isoformat()

    # --- SAVE TO GCS ---
    print(f"📦 Total records collected: {len(all_ingest_data)}")

    if not all_ingest_data:
        return "⚠️ No ingest data collected.", 200

    try:
        ingest_storage_client = storage.Client()
        ingest_bucket = ingest_storage_client.bucket(BRONZE_BUCKET_NAME)

        ingest_file = f"raw_ingested_prices_{ingest_timestamp}.json"

        ingest_blob = ingest_bucket.blob(ingest_file)
        ingest_blob.upload_from_string(
            data=json.dumps(all_ingest_data, indent=4),
            content_type='application/json'
        )

        print(f"💾 Ingested rich data saved to gs://{BRONZE_BUCKET_NAME}/{ingest_file}.")
        return f"Success: {ingest_file}", 200

    except Exception as error:
        print(f"❌ Ingested storage error: {error}")
        return f"❌ Ingested storage error: {error}", 500
