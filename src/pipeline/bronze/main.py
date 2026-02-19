"""
Main Execution Entry Point for the Bronze Ingestion Engine.

This script acts as the CLI (Command Line Interface) for the pipeline.
It utilizes the 'Strategy Design Pattern' to select and execute the appropriate 
ingestion worker (Binance, Yahoo, etc.) based on user arguments.

Usage:
    python -m src.pipeline.bronze.main --mode historical --source binance
"""

import argparse
from typing import Optional

from .binance_ingestor import BinanceIngestor
from .coingecko_ingestor import CoinGeckoIngestor
from .base_ingestor import BaseIngestor
from src.utils.logger import get_logger

def main() -> None:
    """
    Parses command-line arguments and instantiates the selected Ingestion Strategy.
    """
    # Initialize the Orchestrator Logger
    log = get_logger("BronzeOrchestrator")

    parser = argparse.ArgumentParser(description="Institutional Data Ingestion Engine")

    # Argument 1: The Mode (Time Horizon)
    parser.add_argument(
        "--mode", 
        choices=["historical", "recent", "live", "metadata"],
        required=True, 
        help="Selects the ingestion phase: 'historical' (Archive), 'recent' (Gap-Fill), 'live' (Real-Time), or 'metadata' (Enrichment)."
    )

    # Argument 2: The Source (Data Provider)
    parser.add_argument(
        "--source", 
        choices=["binance"], 
        default="binance", 
        help="Selects the market data provider strategy."
    )

    args = parser.parse_args()

    # Log the exact parameters used to start the run
    log.info(f"=== BRONZE PIPELINE STARTED | Mode: {args.mode.upper()} | Source: {args.source.upper()} ===")

    # Factory Logic: Select the Strategy based on input
    if args.mode == "metadata":
        # Metadata is special; it doesn't need a specific 'source' arg usually, 
        # but I default to CoinGecko for now.
        log.info("Routing to Metadata Enrichment Strategy (CoinGecko).")
        crawler = CoinGeckoIngestor()
        crawler.ingest_metadata()

        log.info("=== BRONZE PIPELINE FINISHED ===")
        return

    # Initialize the ingestor variable with the Base class type hint
    ingestor: Optional[BaseIngestor] = None

    if args.source == "binance":
        log.info("Routing to Market Data Strategy (Binance).")
        ingestor = BinanceIngestor()

    # Execute the selected strategy
    if ingestor:
        if args.mode == "historical":
            ingestor.ingest_historical()
        elif args.mode == "recent":
            ingestor.ingest_recent()
        elif args.mode == "live":
            ingestor.ingest_live()

        log.info("=== BRONZE PIPELINE FINISHED ===")
    else:
        log.error(f"No ingestion strategy found for source '{args.source}'.")
        log.info("=== BRONZE PIPELINE ABORTED ===")

if __name__ == "__main__":
    main()
