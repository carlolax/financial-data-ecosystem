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
BATCH_INGESTION_SIZE = 50
BATCH_RATE_LIMIT = 5

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def batch_data_ingestion(coin_ids: list) -> list:
    # Helper to fetch a specific list of IDs from the API.
    ingestion_params = {
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
    coingecko_response = requests.get(COINGECKO_API_URL, params=ingestion_params, timeout=15)

    if coingecko_response.status_code == 429:
        print("🚨 Rate limit hits (429). Ingestion is going too fast.")
        # Return empty list to preserve partial data rather than crashing.
        return []

    coingecko_response.raise_for_status()
    return coingecko_response.json()

@functions_framework.http
def process_data_ingestion(request) -> Tuple[str, int]:
    print(f"🚀 Starting Bronze Layer - Cloud Batch Ingestion.")

    # Check if bucket name is set.
    if not BRONZE_BUCKET_NAME:
        print("❌ Error: BRONZE_BUCKET_NAME environment variable not set.")
        return "Error: Bucket Env Var Missing", 500

    # --- CAPTURE TIME ONCE ---
    capture_current_time = datetime.now(timezone.utc)
    ingested_timestamp_str = capture_current_time.strftime("%Y%m%d_%H%M%S")

    # Attempt to get 'coins' from JSON body (POST) or URL args (GET).
    target_coins_str = TARGET_CRYPTO_COINS  # Default fallback
    
    # Try parsing JSON body first.
    request_json = request.get_json(silent=True)
    
    if request_json and 'coins' in request_json:
        target_coins_str = request_json['coins']
        print(f"🔧 Manual Override Detected: Ingesting specific coins: {target_coins_str}")
    elif request.args and 'coins' in request.args:
        target_coins_str = request.args['coins']
        print(f"🔧 URL Parameter Detected: Ingesting specific coins: {target_coins_str}")
    
    # Clean and split the string into a list.
    crypto_coin_list = [c.strip() for c in target_coins_str.split(",")]
    total_crypto_coins = len(crypto_coin_list)

    # Calculate batches
    total_ingestion_batches = math.ceil(total_crypto_coins / BATCH_INGESTION_SIZE)

    print(f"📋 Total Coins: {total_crypto_coins} | Batches: {total_ingestion_batches}")

    all_market_data = []

    # Loop through in chunks
    for crypto_index in range(0, total_crypto_coins, BATCH_INGESTION_SIZE):
        current_chunk = crypto_coin_list[crypto_index : crypto_index + BATCH_INGESTION_SIZE]
        current_batch_count = (crypto_index // BATCH_INGESTION_SIZE) + 1

        print(f"🔄 Fetching batch {current_batch_count} of {total_ingestion_batches} ({len(current_chunk)} coins).")

        try:
            batch_data = batch_data_ingestion(current_chunk)

            if batch_data:
                all_market_data.extend(batch_data)
                print(f"✅ Success. Ingested {len(batch_data)} records.")
            else:
                print(f"⚠️ Warning: Batch {current_batch_count} returned no data.")

            if current_batch_count < total_ingestion_batches:
                print(f"😴 Rate Limit Sleep: {BATCH_RATE_LIMIT}s to respect API limits.")
                time.sleep(BATCH_RATE_LIMIT)

        except Exception as error:
            print(f"❌ Error ingesting batch: {error}.")
            return f"Error ingesting batch: {error}", 500

    # --- INJECT LINEAGE ---
    print("💉 Adding 'ingested_timestamp' into records.")
    for ingested_data in all_market_data:
        ingested_data['ingested_timestamp'] = capture_current_time.isoformat()

    # --- SAVE TO GCS ---
    print(f"📦 Total records collected: {len(all_market_data)}")

    if not all_market_data:
        return "⚠️ No data collected.", 200

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BRONZE_BUCKET_NAME)

        output_ingested_file = f"raw_prices_{ingested_timestamp_str}.json"

        blob = bucket.blob(output_ingested_file)
        blob.upload_from_string(
            data=json.dumps(all_market_data, indent=4),
            content_type='application/json'
        )

        print(f"💾 Ingested rich data saved to gs://{BRONZE_BUCKET_NAME}/{output_ingested_file}.")
        return f"Success: {output_ingested_file}", 200

    except Exception as error:
        print(f"❌ Storage Error: {error}")
        return f"Storage Error: {error}", 500
