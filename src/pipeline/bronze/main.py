"""
Main Execution Entry Point for the Bronze Ingestion Engine.

This script acts as the CLI (Command Line Interface) for the pipeline.
It utilizes the 'Strategy Design Pattern' to select and execute the appropriate 
ingestion worker (Binance, Yahoo, etc.) based on user arguments.

Usage:
    python -m src.pipeline.bronze.main --mode historical --source binance
"""

import argparse
from .binance_ingestor import BinanceIngestor

def main():
    """
    Parses command-line arguments and instantiates the selected Ingestion Strategy.
    """
    parser = argparse.ArgumentParser(description="Institutional Data Ingestion Engine")

    # Argument 1: The Mode (Time Horizon)
    parser.add_argument(
        "--mode", 
        choices=["historical", "recent", "live"], 
        required=True, 
        help="Selects the ingestion phase: 'historical' (Archive), 'recent' (Gap-Fill), or 'live' (Real-Time)."
    )

    # Argument 2: The Source (Data Provider)
    parser.add_argument(
        "--source", 
        choices=["binance"], 
        default="binance", 
        help="Selects the market data provider strategy."
    )

    args = parser.parse_args()

    # Factory Logic: Select the Strategy based on input
    ingestor = None
    if args.source == "binance":
        ingestor = BinanceIngestor()

    # Execute the selected strategy
    if ingestor:
        if args.mode == "historical":
            ingestor.ingest_historical()
        elif args.mode == "recent":
            ingestor.ingest_recent()
        elif args.mode == "live":
            ingestor.ingest_live()
    else:
        print(f"❌ Error: No ingestion strategy found for source '{args.source}'.")

if __name__ == "__main__":
    main()
