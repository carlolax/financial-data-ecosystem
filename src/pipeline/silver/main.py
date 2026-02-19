"""
Main Execution Entry Point for the Silver Transformation Engine.

This script serves as the Command Line Interface (CLI) for the cleaning and 
partitioning layer. It directs the raw data (Bronze) into the appropriate 
transformation pipelines based on the user's selected mode.

Usage:
    python -m src.pipeline.silver.main --mode historical
"""

import argparse
from typing import Optional

from .binance_transformer import BinanceTransformer
from .base_transformer import BaseTransformer
from src.utils.logger import get_logger

def main() -> None:
    # Parses command-line arguments and orchestrates the transformation process.
    # Initialize the Orchestrator Logger
    log = get_logger("SilverOrchestrator")

    parser = argparse.ArgumentParser(description="Institutional Data Transformation Engine")

    # Argument 1: The Mode (Time Horizon)
    parser.add_argument(
        "--mode", 
        choices=["historical", "recent"],
        required=True, 
        help="Selects the transformation phase: 'historical' (Deep Backfill) or 'recent' (Gap-Fill)."
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
    log.info(f"=== SILVER PIPELINE STARTED | Mode: {args.mode.upper()} | Source: {args.source.upper()} ===")

    # Initialize the transformer variable with the Base class type hint
    transformer: Optional[BaseTransformer] = None

    if args.source == "binance":
        log.info("Routing to Market Data Transformation Strategy (Binance).")
        transformer = BinanceTransformer()

    # Execute the selected strategy
    if transformer:
        if args.mode == "historical":
            transformer.process_historical()
        elif args.mode == "recent":
            transformer.process_recent()

        log.info("=== SILVER PIPELINE FINISHED ===")
    else:
        log.error(f"No transformation strategy found for source '{args.source}'.")
        log.info("=== SILVER PIPELINE ABORTED ===")

if __name__ == "__main__":
    main()
