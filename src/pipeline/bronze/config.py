"""
Configuration Module for the Bronze Ingestion Layer.

This module acts as the central 'Control Room' for the ingestion pipeline. 
It enforces consistency across the entire platform by providing a single source of truth 
for asset lists, provider URLs, and file paths.

Key Responsibilities:
- Centralizes the master list of tracked assets (Crypto Pairs).
- Defines immutable constants for external data providers (Binance).
- Resolves dynamic file paths to ensure cross-platform compatibility.
"""

from pathlib import Path

# --- DIRECTORY SETUP ---
# Resolves to the absolute path of: project_root/data/bronze
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

# --- ASSET LISTS ---
# The immutable master roster of assets to be ingested.
# Changes here automatically propagate to all Historical, Recent, and Live ingestors.
CRYPTO_PAIRS = [
    # Tier 1: Kings (Market Movers)
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    # Tier 2: Utilities (Payments & Infrastructure)
    "XRPUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "LTCUSDT", "LINKUSDT",
    # Tier 3: Layer 2s (Scaling Solutions)
    "ARBUSDT", "MATICUSDT", "OPUSDT",
    # Tier 4: AI & Compute (High Growth Sector)
    "FETUSDT", "RENDERUSDT", "TAOUSDT", "NEARUSDT",
    # Tier 5: Gaming & Metaverse
    "IMXUSDT", "GALAUSDT", "AXSUSDT",
    # Tier 6: Storage
    "FILUSDT", "ARUSDT",
    # Tier 7: DeFi & Real World Assets
    "UNIUSDT", "AAVEUSDT", "ONDOUSDT",
    # Tier 8: New Gen L1s
    "SUIUSDT", "SEIUSDT", "TIAUSDT",
    # Tier 9: High Volatility (Memes)
    "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT"
]

# --- PROVIDER SETTINGS ---
# Configuration constants for the Binance Public Data API.
BINANCE_CONFIG = {
    "MONTHLY_URL": "https://data.binance.vision/data/spot/monthly/klines",
    "DAILY_URL": "https://data.binance.vision/data/spot/daily/klines",
    "WS_URL": "wss://stream.binance.com:9443/ws",
    "INTERVAL": "1m"  # The granular time-frame for all data (1-Minute Candles)
}
