import os
import requests
import json
import time
import math
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

# --- SETUP ---
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
INGEST_DATA_DIR = BASE_DIR / "data" / "bronze"

# --- CONSTANTS ---
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
BATCH_SIZE = 50

# Mimic a browser to avoid immediate 403/429 blocks.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json"
}

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def fetch_market_data_batch(coin_ids: list) -> list:
    """
    Fetches market data for a specific list of Coin IDs from CoinGecko.

    STRATEGY: "Robust Retry" (Parity with Cloud Function)
    -----------------------------------------------------
    Previously "Fail Fast", this function now implements Exponential Backoff 
    to handle transient Rate Limits (429) or Network Jitters.

    Why Changed?
    1. Stealth: Uses Browser-like User-Agent headers to avoid bot detection.
    2. Resilience: Instead of crashing immediately on a 429 error, it waits 
       (5s, 10s, 20s) and retries up to 3 times.
    3. Integrity: If all retries fail, it raises an Exception to stop the 
       local process, ensuring we don't save incomplete datasets.
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

    # Exponential Backoff Retry Loop
    max_retries = 3

    for attempt in range(max_retries):
        try:
            # Added headers=HEADERS
            response = requests.get(COINGECKO_API_URL, params=params, headers=HEADERS, timeout=30)

            # Case A: Success
            if response.status_code == 200:
                return response.json()

            # Case B: Rate Limit (429) -> Wait and Retry
            if response.status_code == 429:
                wait_time = (2 ** attempt) * 5  # 5s, 10s, 20s
                print(f"   ⚠️ Rate limit (429). Sleeping {wait_time}s before retry {attempt+1}/{max_retries}...")
                time.sleep(wait_time)
                continue # Try again

            # Case C: Other Errors (404, 500) -> Raise Exception (Fail Fast for critical errors)
            response.raise_for_status()

        except requests.exceptions.RequestException as error:
            print(f"   ❌ Network Error (Attempt {attempt+1}): {error}")
            if attempt == max_retries - 1:
                raise error # Crash if we run out of retries
            time.sleep(5) # Small wait before retry on network error

    raise Exception(f"🚨 Failed to fetch batch after {max_retries} attempts.")

def process_ingestion() -> Path:
    """
    Orchestrates the Bronze Layer ingestion process for the Local Environment.

    WORKFLOW:
    1. Configuration: Reads target coins from the .env file.
    2. Batching: Splits the coin list into chunks to respect API limits.
    3. Robust Fetching: Calls CoinGecko API with "Stealth" headers and Retry Logic.
       - Behavior: Retries on 429 errors (up to 3 times) before giving up.
       - Integrity: Raises Exception ONLY if all retries fail, preventing partial data writes.
    4. Lineage: Injects 'ingested_timestamp' into every record.
    5. Storage: Saves the combined raw JSON to the local filesystem (data/bronze/).

    Returns:
        Path: The file path of the saved JSON file.

    Raises:
        Exception: If a batch fails after maximum retries, the entire process stops.
    """
    print(f"🚀 Starting Bronze Layer - Local Ingestion.")

    # 1. Setup Time and Config
    capture_time = datetime.now(timezone.utc)
    file_timestamp = capture_time.strftime("%Y%m%d_%H%M%S")

    coin_list = [c.strip() for c in TARGET_CRYPTO_COINS.split(",")]
    total_coins = len(coin_list)
    total_batches = math.ceil(total_coins / BATCH_SIZE)

    print(f"📋 Targets: {total_coins} Coins | Batches: {total_batches}")

    # 2. Prepare Output
    os.makedirs(INGEST_DATA_DIR, exist_ok=True)
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
                print(f"   ⚠️ Warning: Batch {batch_num} returned empty data.")

            # Small courtesy sleep between batches
            if batch_num < total_batches:
                time.sleep(2) 

        except Exception as error:
            print(f"❌ Critical Error on Batch {batch_num}: {error}")
            raise error # This allows crash if the Retry Logic fails completely.

    # 4. Lineage Injection
    print("💉 Injecting lineage timestamps.")
    for record in all_market_data:
        record['ingested_timestamp'] = capture_time.isoformat()

    # 5. Save to Disk
    output_filename = f"raw_prices_{file_timestamp}.json"
    output_path = INGEST_DATA_DIR / output_filename

    with open(output_path, "w") as f:
        json.dump(all_market_data, f, indent=4)

    print(f"📦 Saved {len(all_market_data)} records to: {output_path}")
    return output_path

if __name__ == "__main__":
    process_ingestion()
