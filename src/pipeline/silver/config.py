"""
Configuration Module for the Silver Transformation Layer.

This module acts as the 'Schema Registry' for the transformation pipeline.
It defines the strict data contracts that convert raw, unstructured Bronze data
into clean, typed, and business-ready Silver tables.

Key Responsibilities:
- Centralizes directory paths for the Silver Data Lake.
- Defines the immutable schema mappings (Raw CSV Index -> Semantic Column Name).
- Sets global standards for file formats (Parquet) and compression (Snappy).
"""

from pathlib import Path
# 💡 CROSS-LAYER DEPENDENCY: 
# I import the Master Asset List from Bronze to ensure that the Silver Layer 
# automatically supports any new assets added to the Ingestion configuration.
from src.pipeline.bronze.config import CRYPTO_PAIRS, BRONZE_DIR

# --- DIRECTORY SETUP ---
# Resolves to the absolute path of: project_root/data/silver
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

# --- STORAGE OPTIMIZATION ---
# I use 'Snappy' compression for Parquet files.
# Rationale: It offers the best balance between high write speeds (crucial for 
# processing 8 years of data) and reasonable disk space savings (~90% vs CSV).
PARQUET_COMPRESSION = "snappy"

# --- SCHEMA DEFINITION ---
# The Raw Binance CSVs do not have headers. I map the integer indices 
# to semantic business names based on the Binance Vision API documentation.
COLUMN_MAPPING = {
    0: "open_time_ms",      # Timestamp: Start of the candle (Unix Milliseconds)
    1: "open",              # Price: Opening value
    2: "high",              # Price: Highest value
    3: "low",               # Price: Lowest value
    4: "close",             # Price: Closing value
    5: "volume",            # Volume: Base asset volume (e.g., BTC)
    6: "close_time_ms",     # Timestamp: End of the candle
    7: "quote_asset_vol",   # Volume: Quote asset volume (e.g., USDT)
    8: "trades",            # Count: Number of trades in this minute
    9: "taker_buy_base",    # Volume: Taker buy volume (Base)
    10: "taker_buy_quote",  # Volume: Taker buy volume (Quote)
    11: "ignore"            # Legacy/Unused field
}

# --- FINAL PROJECTION ---
# The strict list of columns to be retained in the final Parquet files.
# I drop technical artifacts (like 'ignore') to optimize storage.
FINAL_COLUMNS = [
    "coin_id",              # Partition Key (e.g., 'btc')
    "source_updated_at",    # Standardized UTC Timestamp (Derived from open_time_ms)
    "open",
    "high",
    "low",
    "close",
    "volume", 
    "quote_asset_vol",
    "trades",
    "processed_at"
]
