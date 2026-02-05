import duckdb
import os
from pathlib import Path
from datetime import datetime, timezone

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

# --- CONSTANTS ---
SOURCE_FILENAME = "cleaned_market_data.parquet"
OUTPUT_FILENAME = "analyzed_market_summary.parquet"
WINDOW_SIZE = 7
RSI_PERIOD = 14

def get_source_file(directory: Path, filename: str) -> str:
    """
    Validates that the source file exists before attempting to read it.

    Why:
    DuckDB needs a valid file path to run the query. If the file is missing
    (e.g., the Silver layer hasn't run), I want to fail fast with a clear error
    instead of letting DuckDB crash with a confusing message.
    """
    file_path = directory / filename

    if not file_path.exists():
        raise FileNotFoundError(f"❌ No Silver file found at {file_path}. Run cleaning first.")

    print(f"📖 Reading historical data from: {filename}")
    return str(file_path)

def process_analysis() -> Path:
    """
    Orchestrates the Gold Layer: Financial Analysis & Signal Generation (Local).

    Workflow:
    1. Validation: Checks if Silver Parquet data exists.
    2. Logic (DuckDB Window Functions):
       - Ensures chronological ordering of events (Defensive Sorting).
       - Calculates 7-Day Simple Moving Average (SMA).
       - Calculates Volatility (Standard Deviation).
       - Calculates 14-Day Relative Strength Index (RSI) for momentum.

    3. Signal Strategy (Mean Reversion):
       - BUY: Price < SMA (Dip) AND RSI < 30 (Oversold).
       - SELL: Price > SMA (Rally) AND RSI > 70 (Overbought).
       - WAIT: All other conditions.

    4. Storage: Saves the result as a single Parquet file in data/gold/.

    Returns:
        Path: The file path of the saved Parquet file.
    """
    print("🚀 Starting Gold Layer - Data Analysis.")

    # 1. Setup
    analysis_time = datetime.now(timezone.utc).isoformat()
    os.makedirs(GOLD_DIR, exist_ok=True)

    try:
        source_path_str = get_source_file(SILVER_DIR, SOURCE_FILENAME)
    except Exception as error:
        print(error)
        raise error

    # 2. Define the Analytical Query (Improved with Relative Strength Index (RSI))
    # I use Common Table Expressions (WITH clause) to calculate metrics first,
    # then use those metrics to generate the "Signal" in the final SELECT.
    query = f"""
        WITH price_changes AS (
            -- Step 1: Calculate the change from the previous timestamp (I will implement the sorting here.)
            SELECT 
                *,
                current_price - LAG(current_price) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                ) as price_diff
            FROM '{source_path_str}'
        ),

        rolling_stats AS (
            -- Step 2: Calculate SMA, Volatility, and RSI Components
            SELECT 
                *,

                -- Existing Metrics
                AVG(current_price) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as sma_7d,

                STDDEV(current_price) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as volatility_7d,

                -- RSI Components (Separate Gains and Losses)
                AVG(CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                    ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW
                ) as avg_gain,

                AVG(CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                    ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW
                ) as avg_loss

            FROM price_changes
        )

        SELECT 
            coin_id,
            symbol,
            current_price,
            market_cap,

            -- Financial Metrics
            sma_7d,
            volatility_7d,

            -- RSI Calculation (Avoid Division by Zero)
            CASE 
                WHEN avg_loss = 0 THEN 100
                ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
            END as rsi_14d,

            -- SMARTER Signals (Combination Strategy)
            CASE 
                -- BUY: Price is below trend (Dip) AND RSI is Oversold (< 30)
                WHEN current_price < sma_7d AND (100 - (100 / (1 + (avg_gain / avg_loss)))) < 30 THEN 'BUY'

                -- SELL: Price is above trend (Rally) AND RSI is Overbought (> 70)
                WHEN current_price > sma_7d AND (100 - (100 / (1 + (avg_gain / avg_loss)))) > 70 THEN 'SELL'

                ELSE 'WAIT'
            END as signal,

            -- Lineage
            source_updated_at,
            processed_at,
            '{analysis_time}' as analyzed_at

        FROM rolling_stats
        ORDER BY source_updated_at DESC, coin_id
    """

    # 3. Execute and Save
    output_path = GOLD_DIR / OUTPUT_FILENAME
    print("⚙️  Running financial models in DuckDB.")

    try:
        duckdb.execute(f"""
            COPY ({query}) 
            TO '{output_path}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        print(f"✅ Gold Layer Complete. Saved to: {output_path}")

        # Optional: Preview the results to prove it worked
        print("\n📊 Latest Signals Preview:")
        # I added 'rsi_14d' to the columns selected below
        duckdb.sql(f"SELECT symbol, current_price, rsi_14d, signal FROM '{output_path}' LIMIT 5").show()

        return output_path

    except Exception as error:
        print(f"❌ Error during analysis: {error}")
        raise error

if __name__ == "__main__":
    process_analysis()
