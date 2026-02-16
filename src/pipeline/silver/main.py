"""
Main Execution Entry Point for the Silver Transformation Engine.

This script serves as the Command Line Interface (CLI) for the cleaning and 
partitioning layer. It directs the raw data (Bronze) into the appropriate 
transformation pipelines based on the user's selected mode.

Usage:
    python -m src.pipeline.silver.main --mode historical
"""

import argparse
from .binance_transformer import BinanceTransformer

def main() -> None:
    """
    Parses command-line arguments and orchestrates the transformation process.
    
    The function instantiates the appropriate Transformer class (currently 
    Binance-focused) and executes the requested data processing strategy 
    (Historical Backfill or Recent Gap-Fill).
    """
    parser = argparse.ArgumentParser(description="Silver Layer Transformation Engine")
    
    # Argument: The Transformation Scope
    parser.add_argument(
        "--mode", 
        choices=["historical", "recent"], 
        required=True, 
        help="Selects the transformation scope: 'historical' (Deep Archives) or 'recent' (Daily Gap-Fill)."
    )
    
    args = parser.parse_args()
    
    # Factory Logic: Instantiate the specific transformer
    transformer = BinanceTransformer()
    
    # Strategy Execution
    if args.mode == "historical":
        transformer.process_historical()
    elif args.mode == "recent":
        transformer.process_recent()

if __name__ == "__main__":
    main()
