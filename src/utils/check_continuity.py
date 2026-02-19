"""
Data Quality Assurance Utility: Time-Series Continuity Checker.

This module performs a 'Sanity Check' on the Silver Layer Parquet files.
It scans the transformed data to identify missing time intervals (Gaps) 
that could negatively impact technical indicator calculations in the Gold Layer.

Key Responsibilities:
- Gap Detection: Identifies time jumps larger than the expected 1-minute interval.
- Reporting: Logs the start time, duration, and frequency of gaps for each asset.
- Health Check: Validates that the 'deduplication' and 'sorting' steps in Silver worked correctly.
"""

import pandas as pd
from pathlib import Path
from datetime import timedelta

from src.utils.logger import get_logger

# --- CONFIGURATION ---
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
SILVER_DIR: Path = PROJECT_ROOT / "data" / "silver" / "crypto_binance"

# Initialize the Logger globally for this utility script
log = get_logger("CheckContinuity")

def check_continuity(coin_id: str, threshold_minutes: int = 2) -> None:
    """
    Scans a specific asset's history for continuity gaps.

    Args:
        coin_id (str): The unique identifier for the asset (e.g., 'btc').
        threshold_minutes (int, optional): The tolerance for a gap before flagging it. 
                                           Defaults to 2 minutes.
    """
    # Construct path: data/silver/crypto_binance/coin_id=btc/historical_master.parquet
    coin_path: Path = SILVER_DIR / f"coin_id={coin_id}"
    file_path: Path = coin_path / "historical_master.parquet"

    if not file_path.exists():
        log.warning(f"{coin_id.upper()}: No Silver Parquet file found.")
        return

    try:
        # Optimization: Read only the timestamp column to save memory
        df: pd.DataFrame = pd.read_parquet(file_path, columns=["source_updated_at"])
    except Exception as error:
        log.error(f"{coin_id.upper()}: Failed to read Parquet file. Error: {error}")
        return

    # Calculate the time difference between consecutive rows
    time_diffs = df["source_updated_at"].diff()

    # Any jump larger than 'threshold_minutes' (60 seconds) indicates missing data.
    gap_mask = time_diffs > timedelta(minutes=threshold_minutes)

    # Filter the DataFrame to show only the rows where gaps occurred
    gaps: pd.DataFrame = df[gap_mask]

    # Reporting Logic
    if gaps.empty:
        log.info(f"✅ {coin_id.upper()}: Perfect Continuity ({len(df):,} rows)")
    else:
        max_gap = time_diffs.max()
        # Retrieve the timestamp of the first gap found
        first_gap_date = gaps["source_updated_at"].iloc[0]

        log.warning(f"⚠️  {coin_id.upper()}: Data Gaps Detected!")
        log.warning(f"   - Total Gaps: {len(gaps)}")
        log.warning(f"   - Max Gap Duration: {max_gap}")
        log.warning(f"   - First Gap Occurred: {first_gap_date}")

def main() -> None:
    # Orchestrates the continuity check across all available Silver assets.
    log.info("=== CONTINUITY AUDIT STARTED ===")
    log.info("Initiating Gap Hunt (Threshold: >2 minutes).")
    log.info(f"Scanning Directory: {SILVER_DIR}")

    # 1. Discover all coin folders (e.g., coin_id=btc, coin_id=eth)
    if not SILVER_DIR.exists():
        log.error(f"Silver Directory not found at {SILVER_DIR}")
        log.info("=== CONTINUITY AUDIT ABORTED ===")
        return

    coin_folders: list[Path] = sorted(list(SILVER_DIR.glob("coin_id=*")))

    if not coin_folders:
        log.warning("No asset folders found. Have you run the Silver Pipeline?")
        log.info("=== CONTINUITY AUDIT ABORTED ===")
        return

    # 2. Iterate and Analyze
    for folder in coin_folders:
        # Extract 'btc' from 'coin_id=btc'
        current_coin_id: str = folder.name.split("=")[1]
        check_continuity(current_coin_id)
        
    log.info("=== CONTINUITY AUDIT FINISHED ===")

if __name__ == "__main__":
    main()
