import os
import requests
import json
import time
import math
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# --- SETUP ---
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "bronze"

# --- CONSTANTS ---
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
BATCH_INGESTION_SIZE = 50
RATE_LIMIT_SLEEP = 5

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def batch_data_ingestion(coin_ids: list) -> list:
    # Helper to fetch a specific list of IDs from the API.
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

    coingecko_response = requests.get(COINGECKO_API_URL, params=params, timeout=10)

    if coingecko_response.status_code == 429:
        raise Exception("🚨 Rate limit hits (429). Ingestion is going too fast.")

    coingecko_response.raise_for_status()
    return coingecko_response.json()

def process_data_ingestion() -> Path:
    # Ingests market data in batches to respect API limits.
    print(f"🚀 Starting Bronze Layer - Batch Ingestion.")

    # Prepare the list.
    crypto_coin_list = [crypto_coin.strip() for crypto_coin in TARGET_CRYPTO_COINS.split(",")]
    total_crypto_coins = len(crypto_coin_list)

    # Calculate the total chunks to log "Batch 1 of 5".
    total_ingestion_batches = math.ceil(total_crypto_coins / BATCH_INGESTION_SIZE)

    print(f"📋 Total Cryptocurrency Coins: {total_crypto_coins} | Total Ingestion Batches: {total_ingestion_batches}.")

    # Ensure data/bronze directory exists.
    os.makedirs(DATA_DIR, exist_ok=True)
    all_market_data = []

    # Loop through in chunks.
    for crypto_index in range(0, total_crypto_coins, BATCH_INGESTION_SIZE):
        current_chunk = crypto_coin_list[crypto_index : crypto_index + BATCH_INGESTION_SIZE]
        current_batch_count = (crypto_index // BATCH_INGESTION_SIZE) + 1

        print(f"🔄 Fetching batch {current_batch_count} of {total_ingestion_batches} ({len(current_chunk)} coins).")

        try:
            batch_data = batch_data_ingestion(current_chunk)
            all_market_data.extend(batch_data)
            print(f"✅ Success. Ingested {len(batch_data)} records.")

            # Pause execution to prevent hitting the API's rate limit (429 errors).
            if current_batch_count < total_ingestion_batches:
                print(f"😴 Rate Limit Sleep: {RATE_LIMIT_SLEEP}s to respect API limits.")
                time.sleep(RATE_LIMIT_SLEEP)

        except Exception as error:
            print(f"❌ Error ingesting batch: {error}.")
            raise error

    # Save the combined data.
    print(f"📦 Total records collected: {len(all_market_data)}")

    ingested_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_ingested_file = f"raw_prices_{ingested_timestamp}.json"
    file_path = DATA_DIR / output_ingested_file

    with open(file_path, "w") as json_file:
        json.dump(all_market_data, json_file, indent=4)

    print(f"💾 Ingested rich data saved to: {file_path}.")
    return file_path
 
# Entry point for running the bronze layer (data ingestion) locally.
if __name__ == "__main__":
    process_data_ingestion()
