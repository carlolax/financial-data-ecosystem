import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# --- SETUP ---
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "bronze"

# --- CONSTANTS ---
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"
# Default to a safe list if env is missing
TARGET_COINS = os.getenv("COINS_TO_FETCH", "bitcoin,ethereum,solana,cardano")

def process_data_ingestion() -> Path:
    """
    Fetches current crypto prices from CoinGecko and saves them as a raw JSON file.

    Process:
    1. Reads the list of target coins from the environment (TARGET_COINS).
    2. Requests real-time price and volume data from the CoinGecko API.
    3. Generates a timestamped filename (e.g., raw_prices_20260116.json).
    4. Saves the raw JSON response to 'data/bronze/'.

    Returns:
        Path: The absolute path to the saved JSON file.

    Raises:
        requests.HTTPError: If the API call fails.
        IOError: If the file cannot be written.
    """
    print(f"🚀 Starting Bronze Layer - Data Ingestion for: {TARGET_COINS}")
    
    # Ensure data/bronze directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    params = {
        "ids": TARGET_COINS,
        "vs_currencies": "usd",
        "include_24hr_vol": "true"
    }

    try:
        response = requests.get(COINGECKO_API_URL, params=params, timeout=10) # Added timeout
        response.raise_for_status() # Raises error for 404, 500, etc.
        
        coingecko_data = response.json()
        print("✅ CoinGecko data fetched successfully.")

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_prices_{timestamp}.json"
        file_path = DATA_DIR / filename

        # Save to disk
        with open(file_path, "w") as json_file:
            json.dump(coingecko_data, json_file, indent=4)

        print(f"💾 Ingested data was saved to: {file_path}")
        
        return file_path # Return the path for other scripts to use it

    except Exception as error:
        print(f"❌ Critical error in Bronze Layer: {error}")
        # Re-raise the error to stop the pipeline
        raise error
 
# Entry point for running the bronze layer (data ingestion) locally
if __name__ == "__main__":
    process_data_ingestion()
