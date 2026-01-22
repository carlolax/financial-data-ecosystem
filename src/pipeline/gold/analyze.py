import duckdb
import os
from pathlib import Path
from datetime import datetime, timezone

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
CLEAN_DATA_DIR = BASE_DIR / "data" / "silver"
ANALYZE_DATA_DIR = BASE_DIR / "data" / "gold"

# Analysis Parameters
WINDOW_SIZE = 7

def validate_clean_file() -> Path:
    # Validate that the specific cleaned parquet file exists.
    clean_file = CLEAN_DATA_DIR / "cleaned_market_data.parquet"

    if not clean_file.exists():
        raise FileNotFoundError(f"❌ No Silver file found at {clean_file}. Run clean.py first.")

    return clean_file

def process_data_analytics() -> Path:
    print("🚀 Starting Gold Layer - Data Analysis.")

    analysis_time = datetime.now(timezone.utc).isoformat()

    # Get the specific file from silver layer directory.
    try:
        latest_clean_file = validate_clean_file()
        print(f"📖 Reading historical data from: {latest_clean_file.name}")
    except FileNotFoundError as file_not_found_error:
        print(f"❌ Pipeline stopped: {file_not_found_error}")
        raise file_not_found_error

    # Ensure gold directory exists.
    os.makedirs(ANALYZE_DATA_DIR, exist_ok=True)

    # SQL Analysis
    query_to_analyze = f"""
        WITH moved_data AS (
            SELECT 
                coin_id,
                symbol,
                name,
                current_price,
                market_cap,
                ath,
                source_updated_at,
                ingested_timestamp, 
                processed_at,

                AVG(current_price) OVER (
                    PARTITION BY coin_id 
                    ORDER BY source_updated_at 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as sma_7d,

                STDDEV(current_price) OVER (
                    PARTITION BY coin_id 
                    ORDER BY source_updated_at 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as volatility_7d
            FROM '{latest_clean_file}'
        )

        SELECT 
            -- Business Data
            coin_id,
            symbol,
            name,
            current_price,
            market_cap,
            ath,
            sma_7d,
            volatility_7d,

            -- The Signal
            CASE 
                WHEN current_price < sma_7d AND volatility_7d > 0 THEN 'BUY'
                WHEN current_price > sma_7d THEN 'SELL'
                ELSE 'WAIT'
            END as signal,

            -- The Lineage Block
            source_updated_at,
            ingested_timestamp,
            processed_at,
            '{analysis_time}' as analyzed_at

        FROM moved_data
        ORDER BY source_updated_at DESC, coin_id
    """

    # Execute and Save
    analyzed_file_output = ANALYZE_DATA_DIR / "analyzed_market_summary.parquet"

    print("⚙️ Running financial models in DuckDB.")

    try:
        duckdb.execute(f"""
            COPY ({query_to_analyze}) 
            TO '{analyzed_file_output}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        print(f"✅ Gold Layer Complete. Analyzed file saved to: {analyzed_file_output}.")

        # Print the latest signals (Previewing the new lineage columns too!)
        print("\n📊 Latest Signals Preview:")
        duckdb.sql(f"SELECT * FROM '{analyzed_file_output}' LIMIT 5").show()

        return analyzed_file_output

    except Exception as error:
        print(f"❌ Error during analysis: {error}.")
        raise error

# Entry point for running the gold layer (data analytics) locally.
if __name__ == "__main__":
    process_data_analytics()
