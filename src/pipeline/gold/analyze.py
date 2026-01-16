import duckdb
from pathlib import Path

# --- SETUP ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

# --- CONSTANTS ---
SILVER_FILE = SILVER_DIR / "cleaned_crypto_prices.parquet"
GOLD_FILE = GOLD_DIR / "analyzed_market_summary.parquet"

# Analysis Parameters
WINDOW_SIZE = 7

def process_data_analytics() -> Path:
    """
    Performs financial analysis on the Silver layer data (Parquet).

    Process:
    1. Reads the clean Parquet file using DuckDB.
    2. Calculates a 7-Day Moving Average (SMA).
    3. Calculates Volatility (Standard Deviation).
    4. Generates a 'Signal' (BUY/WAIT) based on price vs. SMA.
    5. Saves the result to the Gold layer.

    Returns:
        Path: The absolute path to the Gold Parquet file.
    
    Raises:
        FileNotFoundError: If the Silver input file is missing.
    """
    print("🚀 Starting Gold Layer - Data Analysis")

    # 1. Check parquet file existence before crashing
    if not SILVER_FILE.exists():
        raise FileNotFoundError(f"❌ No parquet file at: {SILVER_FILE}. Please run 'clean.py' first.")

    print(f"📖 Reading from: {SILVER_FILE}")

    # Ensure gold/data directory exists
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # 2. DuckDB Connection
    duckdb_con = duckdb.connect(database=':memory:')

    # 3. SQL Query
    query = f"""
        WITH moved_data AS (
            SELECT 
                *,
                -- Moving Average
                AVG(price_usd) OVER (
                    PARTITION BY coin_id 
                    ORDER BY extraction_timestamp 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as sma_7d,
                
                -- Volatility (Standard Deviation)
                STDDEV(price_usd) OVER (
                    PARTITION BY coin_id 
                    ORDER BY extraction_timestamp 
                    ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                ) as volatility_7d
            FROM '{SILVER_FILE}'
        )
        SELECT 
            *,
            -- Signal Logic
            CASE 
                WHEN price_usd < sma_7d AND volatility_7d > 0 THEN 'BUY'
                WHEN price_usd > sma_7d THEN 'SELL'
                ELSE 'WAIT'
            END as signal
        FROM moved_data
        ORDER BY extraction_timestamp DESC, coin_id
    """

    try:
        # Execute query
        df = duckdb_con.execute(query).df()

        # Report Preview
        print("\n📊 Market Analysis Preview:")
        print(df.head(10))

        # Save to disk
        print(f"\n💾 Saving analytics to {GOLD_FILE}.")
        df.to_parquet(GOLD_FILE, index=False)
        print("✅ Saving complete.")

        return GOLD_FILE

    except Exception as error:
        print(f"❌ Error during analysis: {error}")
        raise error
    finally:
        duckdb_con.close()

# Entry point for running the gold layer (data analytics) locally
if __name__ == "__main__":
    process_data_analytics()
