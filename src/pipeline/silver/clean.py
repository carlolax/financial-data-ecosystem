import duckdb
import os
from pathlib import Path
from datetime import datetime, timezone

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

# --- CONSTANTS ---
SOURCE_PATTERN = "raw_prices_*.json"

def process_cleaning() -> Path:
    """
    Orchestrates the Silver Layer cleaning process for the Local Environment.

    Workflow:
    1. Validation: Checks if Bronze data exists (raw_prices_*.json).
    2. Configuration: Sets up DuckDB to read the local JSON files.
    3. Transformation (SQL):
       - Deduplication: Selects DISTINCT records to handle overlapping data.
       - Schema Handling: Extracts 'Safe FDV' for coins with infinite supply.
       - Metadata: Injects 'processed_at' timestamp.
    4. Storage: Saves the result as a single optimized Parquet file in data/silver/.

    Returns:
        Path: The file path of the saved Parquet file.
    """
    print("🚀 Starting Silver Layer - Schema Cleaning.")

    # 1. Setup and Output Directory
    processing_time = datetime.now(timezone.utc).isoformat()
    os.makedirs(SILVER_DIR, exist_ok=True)

    # 2. Find all matching files
    files_found = list(BRONZE_DIR.glob(SOURCE_PATTERN))

    if not files_found:
        print(f"❌ No files found in {BRONZE_DIR} matching '{SOURCE_PATTERN}'.")
        return

    print(f"🔎 Found {len(files_found)} raw files. Processing.")

    # 3. Iterate through each file (Just like Cloud Function triggers once per file)
    for input_file in files_found:

        # Generate Unique Output Name
        # Input:  raw_prices_20250209.json 
        # Output: clean_prices_20250209.parquet
        filename = input_file.name
        output_filename = filename.replace("raw_", "clean_").replace(".json", ".parquet")
        output_path = SILVER_DIR / output_filename

        print(f"   🔄 Processing: {filename} -> {output_filename}")

        # 4. Define Query for this specific file
        query = f"""
            SELECT DISTINCT
                id as coin_id,
                symbol,
                name,
                current_price,
                market_cap,
                market_cap_rank,
                
                -- Safe FDV Calculation
                CASE 
                    WHEN max_supply IS NULL THEN (current_price * total_supply)
                    ELSE (current_price * max_supply)
                END as fully_diluted_valuation,

                total_volume,
                high_24h,
                low_24h,
                price_change_percentage_24h,
                circulating_supply,
                total_supply,
                max_supply,
                ath,
                ath_change_percentage,
                ath_date,
                last_updated as source_updated_at,
                ingested_timestamp,
                '{processing_time}' as processed_at

            FROM read_json_auto('{input_file}')
        """

        try:
            # 5. Save Individual Parquet File
            duckdb.execute(f"""
                COPY ({query}) 
                TO '{output_path}' 
                (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
            """)
        except Exception as error:
            print(f"❌ Error processing {filename}: {error}")
            continue # Skip to next file on error

    print(f"✅ Silver Layer Complete. Processed {len(files_found)} files.")

if __name__ == "__main__":
    process_cleaning()
