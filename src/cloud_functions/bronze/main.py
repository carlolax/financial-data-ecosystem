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
BATCH_SIZE = 50
RATE_LIMIT_SECONDS = 10

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def fetch_market_data_batch(coin_ids: list) -> list:
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
    params = {
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
    response = requests.get(COINGECKO_API_URL, params=params, timeout=15)

    if response.status_code == 429:
        # Cloud: Return an empty list to avoid triggering an automatic Cloud Retry loop.
        print("🚨 Rate limit hit (429). Skipping batch to avoid retry loop.")
        return []

    response.raise_for_status()
    return response.json()

@functions_framework.http
def process_ingestion(request) -> Tuple[str, int]:
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
    print(f"🚀 Starting Bronze Layer - Cloud Ingestion.")

    if not BRONZE_BUCKET_NAME:
        return "Error: BRONZE_BUCKET_NAME missing.", 500

    # 1. Setup Time and Config
    capture_time = datetime.now(timezone.utc)
    file_timestamp = capture_time.strftime("%Y%m%d_%H%M%S")
    
    # 2. Dynamic Override Parsing
    target_coins_str = TARGET_CRYPTO_COINS
    request_json = request.get_json(silent=True)

    if request_json and 'coins' in request_json:
        target_coins_str = request_json['coins']
        print(f"🔧 Manual Override: {target_coins_str}")
    elif request.args and 'coins' in request.args:
        target_coins_str = request.args['coins']
        print(f"🔧 URL Override: {target_coins_str}")

    # Clean and split the string into a list.
    coin_list = [c.strip() for c in target_coins_str.split(",")]
    total_coins = len(coin_list)

    # Calculate batches
    total_batches = math.ceil(total_coins / BATCH_SIZE)

    print(f"📋 Targets: {total_coins} Coins | Batches: {total_batches}")

    all_market_data = []

    # 3. Batch Loop
    for i in range(0, total_coins, BATCH_SIZE):
        current_batch_ids = coin_list[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1

        print(f"🔄 Processing Batch {batch_num}/{total_batches} ({len(current_batch_ids)} coins).")

        try:
            batch_data = fetch_market_data_batch(current_batch_ids)

            if batch_data:
                all_market_data.extend(batch_data)
                print(f"   ✅ Success: {len(batch_data)} records.")
            else:
                print(f"   ⚠️ Warning: Batch {batch_num} empty (Rate Limit?).")

            if batch_num < total_batches:
                print(f"   😴 Sleeping {RATE_LIMIT_SECONDS}s.")
                time.sleep(RATE_LIMIT_SECONDS)

        except Exception as error:
            print(f"❌ Error on Batch {batch_num}: {error}")
            return f"Error: {error}", 500

    # 4. Lineage Injection
    print("💉 Injecting lineage timestamps.")
    for record in all_market_data:
        record['ingested_timestamp'] = capture_time.isoformat()

    # 5. Save to GCS
    if not all_market_data:
        return "Warning: No data collected.", 200

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BRONZE_BUCKET_NAME)

        output_filename = f"raw_prices_{file_timestamp}.json"
        blob = bucket.blob(output_filename)

        blob.upload_from_string(
            data=json.dumps(all_market_data, indent=4),
            content_type='application/json'
        )

        print(f"📦 Uploaded {len(all_market_data)} records to gs://{BRONZE_BUCKET_NAME}/{output_filename}")
        return f"Success: {output_filename}", 200

    except Exception as error:
        print(f"❌ Storage Error: {error}")
        return f"Storage Error: {error}", 500
