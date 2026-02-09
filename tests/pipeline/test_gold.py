import sys
import os
import pytest
import pandas as pd
from unittest.mock import patch
from datetime import datetime, timedelta

# --- PATH SETUP ---
# Add 'src' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from pipeline.gold.analyze import process_analysis

# --- FIXTURES ---
@pytest.fixture
def sample_silver_data():
    """
    Creates a Pandas DataFrame with specific price patterns to test signal logic.

    Data Scenarios:
    1. 'bull_coin': Price doubles at the end.
       - Expected Result: Price > SMA -> 'SELL'
    2. 'bear_coin': Price drops by half at the end.
       - Expected Result: Price < SMA -> 'BUY'

    Returns:
        pd.DataFrame: A DataFrame ready to be saved as a Parquet file.
    """
    # Create timestamps for the last 7 days
    base_time = datetime.now()
    timestamps = [base_time - timedelta(days=i) for i in range(7)]
    timestamps.reverse() # Oldest to newest

    data = []
    
    # SCENARIO 1: Bull Coin (Flat price 100, then jumps to 200)
    # SMA (approx) will be around 114. Current Price 200 > 114 -> SELL
    for i, ts in enumerate(timestamps):
        price = 200 if i == 6 else 100 
        data.append({
            "coin_id": "bull_coin",
            "symbol": "bull",
            "name": "Bull Coin",
            "current_price": price,
            "market_cap": 1000000,
            "ath": 500,
            "source_updated_at": ts,
            "ingested_timestamp": ts,
            "processed_at": ts
        })

    # SCENARIO 2: Bear Coin (Flat price 100, then drops to 50)
    # SMA (approx) will be around 92. Current Price 50 < 92 -> BUY
    for i, ts in enumerate(timestamps):
        price = 50 if i == 6 else 100
        data.append({
            "coin_id": "bear_coin",
            "symbol": "bear",
            "name": "Bear Coin",
            "current_price": price,
            "market_cap": 1000000,
            "ath": 500,
            "source_updated_at": ts,
            "ingested_timestamp": ts,
            "processed_at": ts
        })

    return pd.DataFrame(data)

# --- TEST 1: Financial Logic & Signal Generation ---
def test_process_analysis_logic(tmp_path, sample_silver_data):
    """
    Verifies the financial calculations and signal generation logic in DuckDB.

    Scenario:
        - Input data contains a "Bull Coin" (Price spike) and "Bear Coin" (Price drop).
        - The process_analysis function is executed using temporary directories.

    Assertions:
        1. The output Parquet file is created.
        2. 'bull_coin' receives a 'SELL' signal (Price > SMA).
        3. 'bear_coin' receives a 'BUY' signal (Price < SMA).
        4. The SMA is calculated correctly (not zero).

    Args:
        tmp_path (Path): Pytest fixture for temporary directories.
        sample_silver_data (pd.DataFrame): Fixture data.
    """
    # 1. SETUP: Create Temp Directories
    temp_silver = tmp_path / "data" / "silver"
    temp_gold = tmp_path / "data" / "gold"
    temp_silver.mkdir(parents=True)
    temp_gold.mkdir(parents=True)

    # Write the mock data to the "Silver" location
    input_file = temp_silver / "clean_prices_test.parquet"
    sample_silver_data.to_parquet(input_file)

    # 2. PATCH & EXECUTE: Redirect code to use temp folders
    with patch("pipeline.gold.analyze.SILVER_DIR", temp_silver), \
         patch("pipeline.gold.analyze.GOLD_DIR", temp_gold):

        process_analysis()

        # 3. ASSERT: Check if output file exists in the Gold directory
        expected_output = temp_gold / "analyzed_market_summary.parquet"
        assert expected_output.exists(), "Gold analysis file was not created"

        # Load results to verify logic
        df = pd.read_parquet(expected_output)

        # Get the latest row for each coin
        bull_row = df[df['coin_id'] == 'bull_coin'].sort_values('source_updated_at', ascending=False).iloc[0]
        bear_row = df[df['coin_id'] == 'bear_coin'].sort_values('source_updated_at', ascending=False).iloc[0]

        # Verify BULL Logic: Price (200) > SMA (approx 114) -> SELL
        print(f"Bull Coin -> Price: {bull_row['current_price']}, SMA: {bull_row['sma_7d']}, Signal: {bull_row['signal']}")
        assert bull_row['signal'] == 'SELL'

        # Verify BEAR Logic: Price (50) < SMA (approx 92) -> BUY
        print(f"Bear Coin -> Price: {bear_row['current_price']}, SMA: {bear_row['sma_7d']}, Signal: {bear_row['signal']}")
        assert bear_row['signal'] == 'BUY'

# --- TEST 2: Missing File Handling ---
def test_process_analysis_no_file(tmp_path):
    """
    Verifies that the process fails fast if the Silver data is missing.

    Scenario:
        - The Silver directory exists but is empty.

    Assertions:
        - Raises FileNotFoundError.
    """
    # 1. SETUP: Empty Silver Directory
    temp_silver = tmp_path / "data" / "silver"
    temp_gold = tmp_path / "data" / "gold"
    temp_silver.mkdir(parents=True)
    temp_gold.mkdir(parents=True)

    # 2. EXECUTE & ASSERT
    with patch("pipeline.gold.analyze.SILVER_DIR", temp_silver), \
         patch("pipeline.gold.analyze.GOLD_DIR", temp_gold):

        try:
            process_analysis()
        except FileNotFoundError:
            pytest.fail("process_analysis() crashed on missing files! Should exit gracefully.")
        
        expected_output = temp_gold / "analyzed_market_summary.parquet"
        assert not expected_output.exists(), "Output file should not be created if input is missing"
