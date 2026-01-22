import duckdb
import os
from pathlib import Path
from datetime import datetime, timezone

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
INGEST_DATA_DIR = BASE_DIR / "data" / "bronze"
CLEAN_DATA_DIR = BASE_DIR / "data" / "silver"

def validate_ingested_files(ingest_file_directory: Path, ingest_file_pattern: str = "raw_prices_*.json") -> str:
    # Verifies that source files actually exist before running DuckDB.
    ingest_files_found = list(ingest_file_directory.glob(ingest_file_pattern))
    ingest_file_count = len(ingest_files_found)

    if ingest_file_count == 0:
        raise FileNotFoundError(f"❌ No files found in {ingest_file_directory} matching '{ingest_file_pattern}'. Run ingest.py first.")

    print(f"🔎 Found {ingest_file_count} files to process. Processing.")
    return str(ingest_file_directory / ingest_file_pattern)

def process_data_cleaning() -> Path:
    # Reads all JSON files from ingestion, deduplicates data, handles nulls, and saves to a parquet file.
    print("🚀 Starting Silver Layer - Schema Cleaning.")

    processing_time = datetime.now(timezone.utc).isoformat()

    # Ensure output directory exists.
    os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

    # Validate first instead of blindly creating the string.
    try:
        ingest_file_pattern = validate_ingested_files(INGEST_DATA_DIR, "raw_prices_*.json")
    except FileNotFoundError as file_not_found_error:
        print(file_not_found_error)
        raise file_not_found_error

    print(f"📂 Processing pattern: {ingest_file_pattern}.")

    # Define the query using 'DISTINCT' to prevent duplicate rows.
    query_to_clean = f"""
        SELECT DISTINCT
            id as coin_id,
            symbol,
            name,
            current_price,
            market_cap,
            market_cap_rank,

            -- SAFE FDV CALCULATION
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

        FROM read_json_auto('{ingest_file_pattern}')
        ORDER BY source_updated_at DESC
    """

    # Execute and Save
    clean_file_output = CLEAN_DATA_DIR / "cleaned_market_data.parquet"

    print("⚙️ Cleaning historical data with DuckDB.")

    try:
        duckdb.execute(f"""
            COPY ({query_to_clean}) 
            TO '{clean_file_output}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)
        print(f"✅ Silver Layer Complete. Cleaned file saved to: {clean_file_output}.")
        return clean_file_output

    except Exception as error:
        print(f"❌ Error in Silver Layer - Schema Cleaning: {error}.")
        # If this fails, it might mean no files match the pattern
        if "No files found" in str(error) or "No such file" in str(error):
             print("💡 Hint: Make sure you have run 'ingest.py' at least once.")
        raise error

# Entry point for running the silver layer (data cleaning) locally
if __name__ == "__main__":
    process_data_cleaning()
