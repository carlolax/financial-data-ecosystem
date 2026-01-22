import duckdb
import os
from pathlib import Path
from datetime import datetime

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

# Analysis Parameters
WINDOW_SIZE = 7

def get_latest_file(directory: Path, pattern: str = '*.parquet') -> Path:
    # Finds the most recently created file in a directory.
    silver_layer_files = (directory.glob(pattern))

    if not silver_layer_files:
        raise FileNotFoundError(f"No files found in {directory} matching {pattern}")
    return max(silver_layer_files, key=os.path.getctime)

def process_data_analytics() -> Path:
    # Performs financial analysis on the Silver layer data. calculates SMA, Volatility, and Trading Signals.
    print("🚀 Starting Gold Layer - Data Analysis.")

    # Get the latest file from silver layer directory.
    try:
        latest_silver_file = get_latest_file(SILVER_DIR)
        print(f"📖 Reading historical data from: {latest_silver_file.name}")
    except FileNotFoundError:
        print("❌ No Silver data found. Please run clean.py first.")
        raise FileNotFoundError("Pipeline stopped: Missing silver data.")

    # Ensure gold directory exists.
    os.makedirs(GOLD_DIR, exist_ok=True)

    # SQL Analysis - Calculate moving averages based on the 'source_updated_at' (API time).
    query_to_analyze = f"""
        WITH moved_data AS (
            SELECT 
                coin_id,
                symbol,
                name,
                source_updated_at,
                current_price,
                market_cap,
                ath,
                
                -- Moving Average (7 Records ~ 7 Days if run daily)
                AVG(current_price) OVER (
                    PARTITION BY coin_id 
                    ORDER BY source_updated_at 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as sma_7d,
                
                -- Volatility (Standard Deviation)
                STDDEV(current_price) OVER (
                    PARTITION BY coin_id 
                    ORDER BY source_updated_at 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as volatility_7d
            FROM '{latest_silver_file}'
        )
        SELECT 
            *,
            -- Signal Logic (Mean Reversion Strategy)
            CASE 
                -- If price is below average & volatility is present = Buy the Dip
                WHEN current_price < sma_7d AND volatility_7d > 0 THEN 'BUY'
                -- If price is above average = Take Profit
                WHEN current_price > sma_7d THEN 'SELL'
                ELSE 'WAIT'
            END as signal,
            
            -- Metadata
            current_timestamp as analyzed_at
        FROM moved_data
        ORDER BY source_updated_at DESC, coin_id
    """

    # Execute and Save
    analyzed_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_analyzed_file = GOLD_DIR / f"analyzed_market_data_{analyzed_timestamp}.parquet"

    print("⚙️ Running financial models in DuckDB.")

    try:
        duckdb.execute(f"""
            COPY ({query_to_analyze}) 
            TO '{output_analyzed_file}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        print(f"✅ Gold Layer Complete. Analyzed file saved to: {output_analyzed_file}.")

        # Print the latest signals.
        print("\n📊 Latest Signals Preview:")
        duckdb.sql(f"SELECT symbol, current_price, sma_7d, signal FROM '{output_analyzed_file}' LIMIT 5").show()

        return output_analyzed_file

    except Exception as error:
        print(f"❌ Error during analysis: {error}.")
        raise error

# Entry point for running the gold layer (data analytics) locally.
if __name__ == "__main__":
    process_data_analytics()
