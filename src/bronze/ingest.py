import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# SETUP:
# Load variables from .env file into the environment
load_dotenv()

# Calculates the project root: /Users/<NAME>/Developer/crypto-project/
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directory that points to: /Users/<NAME>/Developer/crypto-project/data/bronze
DATA_DIR = BASE_DIR / "data" / "bronze"

# CONFIGURATION:
# Define the CoinGecko URL as a constant
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"

# Check .env for specific coins, otherwise, it will use the default list
env_coins = os.getenv("COINS_TO_FETCH")
TARGET_COINS = env_coins if env_coins else "bitcoin,ethereum,solana"

# Main function
def ingest_bronze_local():
    print(f"Starting Bronze Layer Ingestion.")
    print(f"  Target Coins: {TARGET_COINS}")

    # Creates the directory automatically
    os.makedirs(DATA_DIR, exist_ok=True)

    # Parameters that will be only extracted from CoinGecko API
    params = {
        "ids": TARGET_COINS,
        "vs_currencies": "usd",
        "include_24hr_vol": "true"
    }

    # Attempt to request on getting the data from CoinGecko, if it fails, it will show an error message
    try:
        response = requests.get(COINGECKO_API_URL, params=params)
        response.raise_for_status()
        
        # Variable for storing the response as JSON
        coingecko_api_data = response.json()
        print("CoinGecko data was fetched successfully.")

        # Timestamp (format as YYYYMMDD_HHMMSS) to add as metadata on filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_prices_{timestamp}.json"

        # Variable on where to store the JSON file
        file_path = DATA_DIR / filename

        # Stored the extracted data to a JSON file
        with open(file_path, "w") as f:
            json.dump(coingecko_api_data, f, indent=4)

        print(f"Saved to: {file_path}")

    except Exception as error:
        print(f"Error: {error}")
 
# Entry point for running the bronze layer (ingestion) locally
if __name__ == "__main__":
    ingest_bronze_local()
