"""
Main Execution Entry Point for the Gold Layer (Feature Engineering).

This script acts as the CLI (Command Line Interface) for the Feature Engineering pipeline.
It orchestrates the transformation of 'Silver' (Clean Data) into 'Gold' (Market Intelligence).

Usage:
    python -m src.pipeline.gold.main --assets all
    python -m src.pipeline.gold.main --assets btc eth sol
"""

import argparse
import sys
import gc
from pathlib import Path
from typing import List

from .crypto_featurizer import CryptoFeaturizer

def get_available_assets(silver_path: Path) -> List[str]:
    """
    Scans the Silver directory to find which assets have data ready for processing.

    Returns:
        List[str]: A list of coin IDs (e.g., ['btc', 'eth', 'sol']).
    """
    if not silver_path.exists():
        print(f"❌ Error: Silver Path not found at {silver_path}")
        return []

    # Look for folders starting with "coin_id="
    coin_folders = sorted(list(silver_path.glob("coin_id=*")))

    assets = []
    for folder in coin_folders:
        coin_id = folder.name.split("=")[1]
        assets.append(coin_id)

    return assets

def main() -> None:
    """
    Parses command-line arguments and triggers the Feature Engineering job.
    """
    parser = argparse.ArgumentParser(description="Gold Layer: Financial Feature Engineering Engine")

    # Argument: Which assets to process?
    parser.add_argument(
        "--assets", 
        nargs="+", 
        default=["all"], 
        help="List of assets to process (e.g., 'btc eth') or 'all' to scan the directory."
    )

    args = parser.parse_args()

    # 1. Initialize the Featurizer
    featurizer = CryptoFeaturizer()
    print(f"🥇 Gold Layer Initialized. Output: {featurizer.output_path}")

    # 2. Determine the Target List
    target_assets: List[str] = []

    if "all" in args.assets:
        print("🔍 Scanning Silver Layer for available assets.")
        target_assets = get_available_assets(featurizer.source_path)
    else:
        target_assets = [a.lower() for a in args.assets]

    if not target_assets:
        print("⚠️  No assets found to process. Have you run the Silver Pipeline?")
        sys.exit(0)

    print(f"🚀 Starting Batch Processing for {len(target_assets)} assets.\n")

    # 3. Execution Loop
    success_count = 0
    fail_count = 0

    for coin_id in target_assets:
        try:
            print(f"  ⚙️  Engineering: {coin_id.upper()}.", end="\r")

            # Step A: Load
            df = featurizer.load_data(coin_id)

            # Step B: Transform (The Math)
            rich_df = featurizer.add_features(df)

            # Step C: Save
            featurizer.save_data(rich_df, coin_id)

            success_count += 1

            # Explicitly delete the DataFrames to free up RAM immediately
            del df
            del rich_df

            # Force the Garbage Collector to release the memory back to the OS
            gc.collect()

        except FileNotFoundError:
            print(f"  ⚠️  Skipping {coin_id.upper()}: No Silver data found.")
            fail_count += 1
        except Exception as error:
            print(f"\n  ❌ Critical Failure on {coin_id.upper()}: {error}")
            fail_count += 1

    # 4. Final Report
    print("-" * 50)
    print(f"🏁 Job Complete.")
    print(f"   ✅ Success: {success_count}")
    print(f"   ❌ Failed:  {fail_count}")

if __name__ == "__main__":
    main()
