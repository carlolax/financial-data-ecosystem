import duckdb
import os
from pathlib import Path
from datetime import datetime

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

def process_data_cleaning():
    # Reads all JSON files from ingestion, deduplicates data, handles nulls, and saves to a parquet file.
    print("🚀 Starting Silver Layer - Schema Cleaning.")
    
    # Ensure output directory exists
    os.makedirs(SILVER_DIR, exist_ok=True)
    
    # Input Pattern - taking all files starting with 'raw_prices_'
    input_pattern = str(BRONZE_DIR / "raw_prices_*.json")
    
    print(f"📂 Processing pattern: {input_pattern}")

    # Define the query using 'DISTINCT' to prevent duplicate rows if ingested the same data twice.
    query = f"""
        SELECT DISTINCT
            id as coin_id,
            symbol,
            name,
            current_price,
            market_cap,
            market_cap_rank,
            
            -- SAFE FDV CALCULATION
            -- If max_supply is NULL (Infinite), use total_supply as the proxy
            -- This prevents "NaN" errors in dashboards for ETH, DOGE, SOL
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
            current_timestamp as ingested_at
            
        -- CHANGE: We read the glob pattern to get ALL history
        FROM read_json_auto('{input_pattern}')
        
        -- Order by time to keep it organized (Newest data first)
        ORDER BY source_updated_at DESC
    """

    # Execute and Save
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = SILVER_DIR / f"cleaned_market_data_{timestamp}.parquet"
    
    print("⚙️ Cleaning historical data with DuckDB.")
    
    try:
        duckdb.execute(f"""
            COPY ({query}) 
            TO '{output_file}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)
        print(f"✅ Silver Layer Complete. Saved Master History to: {output_file}")
        return output_file
        
    except Exception as error:
        print(f"❌ Error in Silver Layer - Schema Cleaning: {error}")
        # If this fails, it might mean no files match the pattern
        if "No files found" in str(error) or "No such file" in str(error):
             print("💡 Hint: Make sure you have run 'ingest.py' at least once.")
        raise error

if __name__ == "__main__":
    process_data_cleaning()
