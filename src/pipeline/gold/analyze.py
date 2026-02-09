import duckdb
import os
from pathlib import Path
from datetime import datetime, timezone

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

# --- CONSTANTS ---
WINDOW_SIZE = 7
RSI_PERIOD = 14
STATE_FILENAME = "analyzed_market_summary.parquet"

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
    print("🚀 Starting Gold Layer - Financial Analysis.")

    # 1. Setup
    os.makedirs(GOLD_DIR, exist_ok=True)
    history_path = GOLD_DIR / STATE_FILENAME

    # 2. Find ALL Silver Files (New Data)
    # In a real run, I might only want 'new' files, but for local testing, 
    # I usually re-process everything to see how the logic holds up.
    silver_files = list(SILVER_DIR.glob("clean_prices_*.parquet"))

    if not silver_files:
        print(f"❌ No Silver data found in {SILVER_DIR}")
        return

    print(f"🔎 Found {len(silver_files)} silver files to process.")

    # 3. Configure DuckDB
    con = duckdb.connect(database=":memory:")

    # 4. Load Data (History + New)
    # Strategy: I load ALL silver files as "New Data" for the local test.
    # In a real incremental run, you'd be more selective, but this ensures full coverage.
    print("   Loading Silver Data.")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_silver AS 
        SELECT *, filename as ingested_file 
        FROM read_parquet('{SILVER_DIR}/clean_prices_*.parquet', filename=true)
    """)

    # 5. The Financial Query
    analysis_time = datetime.now(timezone.utc).isoformat()

    query = f"""
        WITH deduplicated_data AS (
            SELECT DISTINCT * FROM raw_silver
        ),

        price_changes AS (
            SELECT 
                *,
                current_price - LAG(current_price) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                ) as price_diff
            FROM deduplicated_data
        ),

        rolling_stats AS (
            SELECT 
                *,
                -- 7-Day SMA
                AVG(current_price) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as sma_7d,

                -- RSI Components
                AVG(CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                    ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW
                ) as avg_gain,

                AVG(CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END) OVER (
                    PARTITION BY coin_id ORDER BY source_updated_at
                    ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW
                ) as avg_loss
            FROM price_changes
        ),

        final_calculations AS (
            SELECT
                *,
                CASE 
                    WHEN avg_loss = 0 THEN 100
                    ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
                END as rsi_14d
            FROM rolling_stats
        )

        SELECT 
            coin_id, symbol, name, current_price, market_cap, ath, 
            sma_7d, rsi_14d,

            -- Generate Signal
            CASE 
                WHEN current_price < sma_7d AND rsi_14d < 30 THEN 'BUY'
                WHEN current_price > sma_7d AND rsi_14d > 70 THEN 'SELL'
                ELSE 'WAIT'
            END as signal,

            source_updated_at, ingested_file, processed_at,
            '{analysis_time}' as analyzed_at

        FROM final_calculations
        ORDER BY source_updated_at DESC, coin_id
    """

    print("⚙️  Executing DuckDB Financial Models.")

    # 6. Execute and Update State
    try:
        con.execute(f"""
            COPY ({query})
            TO '{history_path}'
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        print(f"✅ Gold Layer Success. Market State updated at: {history_path}")

        # 7. Peek at the results (Debug)
        print("\n🔎 Latest Signals (Top 3):")
        con.execute(f"SELECT symbol, current_price, rsi_14d, signal, source_updated_at FROM '{history_path}' ORDER BY source_updated_at DESC LIMIT 3")
        results = con.fetchall()
        for row in results:
            print(f"   {row}")

    except Exception as error:
        print(f"❌ Error in Gold Layer: {error}")
        raise error

if __name__ == "__main__":
    process_analysis()
