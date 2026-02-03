import requests
import pandas as pd
import time
from datetime import datetime, timezone
from pathlib import Path
import os

# --- CONFIGURATION ---
# The specific CoinGecko IDs you want to backfill
COINS = [
    "bitcoin", 
    "ethereum", 
    "uniswap", 
    "litecoin", 
    "ripple", 
    "dogecoin", 
    "cardano", 
    "solana"
]
DAYS_BACK = 90  # Get 3 months of history
VS_CURRENCY = "usd"

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
OUTPUT_FILE = SILVER_DIR / "cleaned_market_data.parquet"

def fetch_historical_data(coin_id: str) -> list:
    """
    Fetches 90 days of daily OHLC/Price data from the CoinGecko API.
    Includes a retry logic to handle 429 Rate Limit errors.

    This function hits the `/coins/{id}/market_chart` endpoint to retrieve 
    historical data. It then standardizes the raw response into a format 
    compatible with our Silver Layer schema.

    Args:
        coin_id (str): The unique CoinGecko API ID (e.g., 'bitcoin', 'ripple').

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents 
                    one daily record containing price, market cap, and timestamps.
                    Returns an empty list if the API call fails.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": VS_CURRENCY,
        "days": DAYS_BACK,
        "interval": "daily"
    }

    # Retry Configuration
    MAX_RETRIES = 3
    WAIT_TIME = 60 # Seconds to wait if rate limited (CoinGecko is strict).

    for attempt in range(MAX_RETRIES):
        try:
            print(f"⏳ Fetching history for: {coin_id} (Attempt {attempt + 1}/{MAX_RETRIES}).")
            response = requests.get(url, params=params, timeout=10)

            # Handle Rate Limiting Explicitly
            if response.status_code == 429:
                print(f"⚠️ 429 Rate Limit hit for {coin_id}. Cooling down for {WAIT_TIME}s.")
                time.sleep(WAIT_TIME)
                continue # Try the loop again

            response.raise_for_status()
            data = response.json()

            # (Existing logic continues below.)
            prices = data.get("prices", [])
            market_caps = data.get("market_caps", [])

            records = []
            for i in range(len(prices)):
                ts_ms = prices[i][0]
                price = prices[i][1]
                mc = market_caps[i][1] if i < len(market_caps) else 0.0

                records.append({
                    "coin_id": coin_id,
                    "symbol": get_symbol(coin_id), 
                    "current_price": float(price),
                    "market_cap": float(mc),
                    "ath": 0.0,
                    "source_updated_at": datetime.fromtimestamp(ts_ms / 1000, timezone.utc),
                    "ingested_timestamp": datetime.now(timezone.utc),
                    "processed_at": datetime.now(timezone.utc)
                })

            return records

        except Exception as error:
            # If it's the last attempt, print the error.
            if attempt == MAX_RETRIES - 1:
                print(f"❌ Failed to fetch {coin_id} after retries: {error}")
                return []

            # Otherwise, wait a bit for generic errors.
            time.sleep(5) 

    return []

def get_symbol(coin_id: str) -> str:
    """
    Maps a CoinGecko Coin ID to its ticker symbol.

    The `/market_chart` endpoint used in `fetch_historical_data` does not 
    return the coin symbol (e.g., 'btc') in its response. This helper function 
    provides a manual mapping or a fallback truncation.

    Args:
        coin_id (str): The CoinGecko Coin ID.

    Returns:
        str: The corresponding ticker symbol (e.g., 'bitcoin' -> 'btc').
    """
    # Quick mapping for the symbol since the history endpoint excludes it.
    mapping = {
        "bitcoin": "btc", "ethereum": "eth", "uniswap": "uni",
        "litecoin": "ltc", "ripple": "xrp", "dogecoin": "doge",
        "cardano": "ada", "solana": "sol"
    }
    return mapping.get(coin_id, coin_id[:3])

def main():
    """
    Main execution entry point for the backfill script.

    Orchestration Steps:
    1. Iterates through the list of configured `COINS`.
    2. Fetches historical data for each coin via `fetch_historical_data`.
    3. Aggregates all records into a single Pandas DataFrame.
    4. Saves the DataFrame to the Silver Layer path as a Parquet file, 
       overwriting any existing data to ensure a clean historical baseline.
    """
    print(f"🚀 Starting Backfill: {DAYS_BACK} days history.")

    all_data = []

    # 1. Fetch Data
    for coin in COINS:
        coin_data = fetch_historical_data(coin)
        all_data.extend(coin_data)
        time.sleep(5) 

    if not all_data:
        print("❌ No data fetched. Exiting.")
        return

    # 2. Create DataFrame
    df = pd.DataFrame(all_data)

    # 3. Ensure Directory Exists
    os.makedirs(SILVER_DIR, exist_ok=True)

    # 4. Save to Silver Layer
    # I overwrite the existing file so I start with a clean, sorted history.
    try:
        df.to_parquet(OUTPUT_FILE, engine="fastparquet", compression="snappy")
        print(f"\n✅ Success! Backfilled {len(df)} rows.")
        print(f"📂 Saved to: {OUTPUT_FILE}")
        print("👉 Next Step: Run 'python src/pipeline/gold/analyze.py' to generate indicators.")
    except Exception as error:
        print(f"❌ Error saving file: {error}")

if __name__ == "__main__":
    main()
