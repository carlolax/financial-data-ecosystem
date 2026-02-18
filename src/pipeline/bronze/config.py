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
from typing import List, Dict, Union

# --- DIRECTORY SETUP ---
# Resolves to the absolute path of: project_root/data/bronze
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_DIR: Path = PROJECT_ROOT / "data" / "bronze"

# --- ASSET LISTS ---
# The immutable master roster of assets to be ingested.
# Changes here automatically propagate to all Historical, Recent, and Live ingestors.
CRYPTO_PAIRS: List[str] = [
    # Tier 1: The Kings (Market Movers)
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    # Tier 2: The Veterans (Payment & Enterprise)
    "XRPUSDT", "ADAUSDT", "TRXUSDT", "LTCUSDT", "BCHUSDT", 
    "DOTUSDT", "ATOMUSDT", "ETCUSDT", "VETUSDT",
    # Tier 3: Layer 2s (Scaling Solutions)
    "ARBUSDT", "MATICUSDT", "OPUSDT", "STRKUSDT", "IMXUSDT",
    # Tier 4: AI & Data (The 2024/25 Narrative)
    "FETUSDT", "RENDERUSDT", "TAOUSDT", "NEARUSDT", "GRTUSDT", "WLDUSDT", "ARKMUSDT",
    # Tier 5: DeFi & Real Yield
    "UNIUSDT", "AAVEUSDT", "MKRUSDT", "LDOUSDT", "RUNEUSDT", 
    "JUPUSDT", "ENAUSDT", "PENDLEUSDT", "INJUSDT",
    # Tier 6: High Performance L1s (The "Solana Killers")
    "AVAXUSDT", "SUIUSDT", "SEIUSDT", "APTUSDT", 
    "HBARUSDT", "FTMUSDT", "TIAUSDT",
    # Tier 7: Gaming & Metaverse
    "GALAUSDT", "AXSUSDT", "SANDUSDT", "MANAUSDT", "BEAMXUSDT",
    # Tier 8: Storage & Infrastructure
    "FILUSDT", "ARUSDT", "PYTHUSDT", "LINKUSDT",
    # Tier 9: The Memes (High Risk/Reward)
    "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "BONKUSDT", "FLOKIUSDT"
]

# --- PROVIDER SETTINGS ---
# Configuration constants for the Binance Public Data API.
BINANCE_CONFIG: Dict[str, str] = {
    "MONTHLY_URL": "https://data.binance.vision/data/spot/monthly/klines",
    "DAILY_URL": "https://data.binance.vision/data/spot/daily/klines",
    "WS_URL": "wss://stream.binance.com:9443/ws",
    "INTERVAL": "1m"  # The granular time-frame for all data (1-Minute Candles)
}

# --- METADATA PROVIDER SETTINGS ---
# I use CoinGecko to fetch 'Rich Context' (Logos, Descriptions, Categories).
COINGECKO_CONFIG: Dict[str, Union[str, int, Dict[str, str]]] = {
    "BASE_URL": "https://api.coingecko.com/api/v3",
    "Max_Retries": 3,
    "Delay_Seconds": 15,
    # I map Binance Symbols (BTCUSDT) to CoinGecko IDs (bitcoin)
    "ID_MAP": {
        # Kings
        "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "BNBUSDT": "binancecoin", "SOLUSDT": "solana",
        # Veterans
        "XRPUSDT": "ripple", "ADAUSDT": "cardano", "TRXUSDT": "tron", "LTCUSDT": "litecoin",
        "BCHUSDT": "bitcoin-cash", "DOTUSDT": "polkadot", "ATOMUSDT": "cosmos", 
        "ETCUSDT": "ethereum-classic", "VETUSDT": "vechain",
        # L2s
        "ARBUSDT": "arbitrum", "MATICUSDT": "matic-network", "OPUSDT": "optimism",
        "STRKUSDT": "starknet", "IMXUSDT": "immutable-x", "MNTUSDT": "mantle",
        # AI
        "FETUSDT": "fetch-ai", "RENDERUSDT": "render-token", "TAOUSDT": "bittensor",
        "NEARUSDT": "near", "GRTUSDT": "the-graph", "WLDUSDT": "worldcoin-wld", "ARKMUSDT": "arkham",
        # DeFi
        "UNIUSDT": "uniswap", "AAVEUSDT": "aave", "MKRUSDT": "maker", "LDOUSDT": "lido-dao",
        "RUNEUSDT": "thorchain", "JUPUSDT": "jupiter-exchange-solana", "ENAUSDT": "ethena-labs",
        "PENDLEUSDT": "pendle", "INJUSDT": "injective-protocol", "ONDOUSDT": "ondo-finance",
        # L1s
        "AVAXUSDT": "avalanche-2", "SUIUSDT": "sui", "SEIUSDT": "sei-network", "APTUSDT": "aptos",
        "KASUSDT": "kaspa", "HBARUSDT": "hedera-hashgraph", "FTMUSDT": "fantom", "TIAUSDT": "celestia",
        # Gaming
        "GALAUSDT": "gala", "AXSUSDT": "axie-infinity", "SANDUSDT": "the-sandbox",
        "MANAUSDT": "decentraland", "BEAMXUSDT": "beam-2",
        # Infra
        "FILUSDT": "filecoin", "ARUSDT": "arweave", "PYTHUSDT": "pyth-network", "LINKUSDT": "chainlink",
        # Memes
        "DOGEUSDT": "dogecoin", "SHIBUSDT": "shiba-inu", "PEPEUSDT": "pepe",
        "WIFUSDT": "dogwifcoin", "BONKUSDT": "bonk", "FLOKIUSDT": "floki"
    }
}
