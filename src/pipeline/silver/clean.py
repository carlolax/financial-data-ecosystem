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
OUTPUT_FILENAME = "cleaned_market_data.parquet"

def get_source_files_pattern(directory: Path, pattern: str) -> str:
    """
    Validates that source files exist and returns the glob pattern string for DuckDB.
    
    Why:
    DuckDB needs a string pattern (like 'data/*.json') to load multiple files.
    This function ensures the directory isn't empty before I try to query it.
    """
    files_found = list(directory.glob(pattern))
    
    if not files_found:
        raise FileNotFoundError(f"❌ No files found in {directory} matching '{pattern}'. Run ingestion first.")

    print(f"🔎 Found {len(files_found)} raw files. Processing.")
    return str(directory / pattern)

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

    try:
        # Validate input files
        source_path_str = get_source_files_pattern(BRONZE_DIR, SOURCE_PATTERN)
    except Exception as error:
        print(error)
        raise error

    # 2. Define the Cleaning Query
    query = f"""
        SELECT DISTINCT
            id as coin_id,
            symbol,
            name,
            current_price,
            market_cap,
            market_cap_rank,

            -- Safe FDV Calculation (Handle infinite supply coins like ETH)
            -- If max_supply is missing, I calculate FDV using total_supply instead.
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

        FROM read_json_auto('{source_path_str}')
        ORDER BY source_updated_at DESC
    """

    # 3. Execute Transformation and Save
    output_path = SILVER_DIR / OUTPUT_FILENAME
    print("⚙️  Executing DuckDB transformation.")

    try:
        # COPY ... TO ... is the fastest way to export data in DuckDB.
        # I use snappy compression to keep the file size small.
        duckdb.execute(f"""
            COPY ({query}) 
            TO '{output_path}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)
        print(f"✅ Silver Layer Complete. Saved to: {output_path}")
        return output_path

    except Exception as error:
        print(f"❌ Error in Silver Layer: {error}")
        raise error

if __name__ == "__main__":
    process_cleaning()
