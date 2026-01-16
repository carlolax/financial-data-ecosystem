import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

def process_data_cleaning() -> Path:
    """
    Normalizes raw JSON data from the Bronze layer and converts it to a flattened Parquet file.

    Process:
    1. Reads all JSON files in 'data/bronze'.
    2. Extracts coin_id, price_usd, volume_24h, and timestamp.
        - Flattens data into a tabular format.
    3. Saves as a single Parquet file in 'data/silver'.

    Returns:
        Path: The absolute path to the generated Parquet file.

    Raises:
        ValueError: If no data is found in Bronze.
    """
    print("🚀 Starting Silver Layer - Data Cleaning")

    # Ensure data/silver directory exists
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Reads all JSON files
    json_files = list(BRONZE_DIR.glob("*.json"))

    if not json_files:
        raise ValueError("❌ No JSON file found. Please run 'ingest.py' first.")

    print(f"📦 Found {len(json_files)} raw files to process.")

    data_list = []

    # 2. Extracts data
    for file_path in json_files:
        try:
            with open(file_path, "r") as json_file:
                json_data = json.load(json_file)

                # Metadata extraction (Lineage)
                filename_parts = file_path.stem.split("_")
                # Fallback if filename format is unexpected
                timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                if len(filename_parts) >= 3:
                     timestamp_str = f"{filename_parts[2]}_{filename_parts[3]}"

                # Flattens data
                for coin_id, metrics in json_data.items():
                    row = {
                        "coin_id": coin_id,
                        "price_usd": float(metrics.get("usd", 0)),
                        "volume_24h": float(metrics.get("usd_24h_vol", 0)),
                        "extraction_timestamp": timestamp_str,
                        "source_file": file_path.name
                    }
                    data_list.append(row)

        except Exception as error:
            print(f"⚠️ Warning: Skipping corrupt file {file_path.name}: {error}")
            continue

    # 3. SAVE DATA
    if data_list:
        df = pd.DataFrame(data_list)

        # Enforce types to floating point
        df["price_usd"] = df["price_usd"].astype(float)

        output_file = SILVER_DIR / "cleaned_crypto_prices.parquet"
        df.to_parquet(output_file, index=False)

        print(f"✅ Processed {len(df)} rows.")
        print(f"💾 Saved to: {output_file}")

        return output_file # Return the path

    else:
        raise ValueError("❌ No valid data could be extracted from the files.")

# Entry point for running the silver layer (data cleaning) locally
if __name__ == "__main__":
    process_data_cleaning()
