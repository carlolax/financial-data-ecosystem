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
BATCH_INGEST_DATA = 50
BATCH_RATE_LIMIT = 5

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def batch_ingest_data(coin_ids: list) -> list:
    """
    Fetches market data for a specific list of Coin IDs from CoinGecko.

    Error Handling Strategy: "Fail Fast"
    ------------------------------------
    In the local environment, I raise an Exception immediately upon hitting
    a Rate Limit (429).

    Why: 
    1. Immediate Feedback: The developer is likely watching the terminal and 
       needs to know if the 'RATE_LIMIT_SLEEP' setting is too low.
    2. Data Integrity: Locally, I prefer 'All or Nothing'. If I can't 
       get all batches, I stop to prevent saving a half-complete dataset.
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

    coingecko_response = requests.get(COINGECKO_API_URL, params=ingest_params, timeout=10)

    if coingecko_response.status_code == 429:
        # Local: Crash the program to alert the developer.
        raise Exception("🚨 Rate limit hits (429). Ingestion is going too fast.")

    coingecko_response.raise_for_status()
    return coingecko_response.json()

def process_ingest_data() -> Path:
    """
    Orchestrates the Bronze Layer ingestion process for the Local Environment.

    Workflow:
    1. Configuration: Reads target coins from the .env file (via TARGET_CRYPTO_COINS).
    2. Batching: Splits the coin list into chunks (e.g., 50 coins) to respect API limits.
    3. Fetching: Calls CoinGecko API for each batch.
       - Logic: Fails Fast (raises Exception) if errors occur.
       - Rate Limiting: Sleeps between batches to avoid 429 errors.
    4. Lineage: Injects 'ingested_timestamp' into every record for data tracking.
    5. Storage: Saves the combined raw JSON to the local filesystem (data/bronze/).

    Returns:
        Path: The file path of the saved JSON file.

    Raises:
        Exception: If any batch fails (Fail Fast strategy), the entire process stops
        to prevent partial data writes.
    """
    print(f"🚀 Starting Bronze Layer - Local Batch Ingestion.")

    capture_current_time = datetime.now(timezone.utc)
    ingest_timestamp = capture_current_time.strftime("%Y%m%d_%H%M%S")

    crypto_coin_list = [crypto_coin.strip() for crypto_coin in TARGET_CRYPTO_COINS.split(",")]
    total_crypto_coins = len(crypto_coin_list)

    # Calculate the total chunks to log "Batch 1 of 5".
    total_batch_ingest = math.ceil(total_crypto_coins / BATCH_INGEST_DATA)

    print(f"📋 Total Cryptocurrency Coins: {total_crypto_coins} | Total Batch Ingestion: {total_batch_ingest}.")

    # Ensure data/bronze directory exists.
    os.makedirs(INGEST_DATA_DIR, exist_ok=True)
    all_ingest_data = []

    # Loop through in chunks.
    for crypto_index in range(0, total_crypto_coins, BATCH_INGEST_DATA):
        current_chunk = crypto_coin_list[crypto_index : crypto_index + BATCH_INGEST_DATA]
        current_batch_count = (crypto_index // BATCH_INGEST_DATA) + 1

        print(f"🔄 Fetching batch {current_batch_count} of {total_batch_ingest} ({len(current_chunk)} coins).")

        try:
            ingest_data = batch_ingest_data(current_chunk)
            all_ingest_data.extend(ingest_data)
            print(f"✅ Success. Ingested {len(ingest_data)} records.")

            # Pause execution to prevent hitting the API's rate limit (429 errors).
            if current_batch_count < total_batch_ingest:
                print(f"😴 Batch Rate Limit: {BATCH_RATE_LIMIT}s to respect API limits.")
                time.sleep(BATCH_RATE_LIMIT)

        except Exception as error:
            print(f"❌ Error ingesting batch: {error}.")
            raise error

    print("💉 Adding 'ingested_timestamp' into records.")
    for data in all_ingest_data:
        data['ingested_timestamp'] = capture_current_time.isoformat()

    # Save the combined data.
    print(f"📦 Total records collected: {len(all_ingest_data)}.")

    ingest_file = f"raw_ingested_prices_{ingest_timestamp}.json"
    ingest_file_path = INGEST_DATA_DIR / ingest_file

    with open(ingest_file_path, "w") as json_file:
        json.dump(all_ingest_data, json_file, indent=4)

    print(f"💾 Ingested rich data saved to: {ingest_file_path}.")
    return ingest_file_path
 
# --- ENTRY POINT ---
if __name__ == "__main__":
    process_ingest_data()
