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
RATE_LIMIT_SECONDS = 10

# Default to a safe list if env is missing.
DEFAULT_CRYPTO_COINS = "bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin,polkadot,matic-network,stellar,vechain"
TARGET_CRYPTO_COINS = os.getenv("CRYPTO_COINS", DEFAULT_CRYPTO_COINS)

def fetch_market_data_batch(coin_ids: list) -> list:
    """
    Fetches market data for a specific list of Coin IDs from CoinGecko.

    Error Handling Strategy: "Fail Fast"
    ------------------------------------
    In the local environment, I raise an Exception immediately upon hitting
    a Rate Limit (429).

    Why: 
    1. Immediate Feedback: The developer is likely watching the terminal and 
       needs to know if the 'RATE_LIMIT_SECONDS' setting is too low.
    2. Data Integrity: Locally, I prefer 'All or Nothing'. If I can't 
       get all batches, I stop to prevent saving a half-complete dataset.
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

    response = requests.get(COINGECKO_API_URL, params=params, timeout=10)

    if response.status_code == 429:
        raise Exception("🚨 Rate limit hit (429). Stop and adjust RATE_LIMIT_SECONDS.")

    response.raise_for_status()
    return response.json()

def process_ingestion() -> Path:
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
            all_market_data.extend(batch_data)
            print(f"   ✅ Success: {len(batch_data)} records.")

            # Rate Limit Sleep (skip for last batch)
            if batch_num < total_batches:
                print(f"   😴 Sleeping {RATE_LIMIT_SECONDS}s.")
                time.sleep(RATE_LIMIT_SECONDS)

        except Exception as error:
            print(f"❌ Critical Error on Batch {batch_num}: {error}")
            raise error

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
