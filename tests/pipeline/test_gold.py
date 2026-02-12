import pytest
import pandas as pd
import duckdb
from src.pipeline.gold.analyze import process_analysis
from unittest.mock import patch

@pytest.fixture
def silver_data():
    """
    Generates a mock 'Silver Layer' DataFrame with synthetic historical data.

    This fixture creates a 15-day price history for a single asset (Bitcoin)
    to allow testing of window functions that require time-series depth:
    - 7-Day SMA: Requires at least 7 data points.
    - 14-Day RSI: Requires at least 14 data points.

    Returns:
        pd.DataFrame: A Pandas DataFrame mimicking the clean Silver Parquet format.
    """
    # Create 15 days of data for Bitcoin to test 7-day SMA and 14-day RSI
    dates = pd.date_range(start="2023-01-01", periods=15, freq="D")

    # Simulating a price uptrend: 100, 101, 102.
    prices = [100 + i for i in range(15)]

    df = pd.DataFrame({
        "coin_id": ["bitcoin"] * 15,
        "symbol": ["btc"] * 15,
        "name": ["Bitcoin"] * 15,
        "current_price": prices,
        "market_cap": [10000 * p for p in prices],
        "market_cap_rank": [1] * 15,
        "fully_diluted_valuation": [11000 * p for p in prices],
        "total_volume": [5000] * 15,
        "source_updated_at": dates
    })
    return df

def test_process_analysis_logic(tmp_path, silver_data):
    """
    Verifies the Gold Layer's financial modeling and state management logic.

    Scenario:
        - Input: 15 days of historical price data (simulated Uptrend).
        - Action: The analysis process calculates indicators via DuckDB.
        - Expectation:
            1. SMA_7d is calculated correctly (mathematically verified).
            2. RSI_14d detects the uptrend (validating the window function).
            3. The final schema includes both raw metrics (FDV, Market Cap) and calculated signals.

    Args:
        tmp_path (Path): Pytest fixture for temporary file operations.
        silver_data (pd.DataFrame): The mock historical data.
    """
    # 1. SETUP: Create a mock Silver Parquet file
    silver_dir = tmp_path / "data" / "silver"
    silver_dir.mkdir(parents=True)

    silver_file = silver_dir / "clean_prices_20230115.parquet"
    silver_data.to_parquet(silver_file)

    # Add 'src.' to patch paths
    with patch("src.pipeline.gold.analyze.SILVER_DIR", silver_dir), \
         patch("src.pipeline.gold.analyze.GOLD_DIR", tmp_path / "data" / "gold"):

        # 2. EXECUTE
        result_file = process_analysis()

        # 3. ASSERT
        assert result_file is not None
        assert result_file.exists()

        # Verify Calculations via DuckDB
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM '{result_file}' ORDER BY source_updated_at").df()

        # Check Rich Schema Preservation
        assert "market_cap" in df.columns
        assert "fully_diluted_valuation" in df.columns

        # Check SMA Logic (Day 15 should have an SMA)
        # SMA of last 7 days (108..114) -> Avg should be 111
        last_row = df.iloc[-1]
        assert last_row['sma_7d'] is not None
        assert 110 < last_row['sma_7d'] < 112

        # Check RSI Logic
        # Constant uptrend means RSI should be high (Overbought) or near 100
        assert last_row['rsi_14d'] > 70

        # Check Signal Logic
        # Price (114) > SMA (111) -> Should be BUY or WAIT (depending on RSI check)
        # Since RSI is likely > 70 (Overbought), logic might suppress BUY, but I check column exists
        assert "signal" in df.columns

def test_process_analysis_no_file(tmp_path):
    """
    Verifies that the Gold Layer exits gracefully when no Silver data is available.

    Scenario:
        - Input: An empty 'data/silver' directory.
        - Action: The analysis process is triggered.
        - Expectation: The function returns None immediately without raising errors.
    """
    silver_dir = tmp_path / "data" / "silver"
    silver_dir.mkdir(parents=True)

    # Add 'src.' to patch paths
    with patch("src.pipeline.gold.analyze.SILVER_DIR", silver_dir), \
         patch("src.pipeline.gold.analyze.GOLD_DIR", tmp_path / "data" / "gold"):

        result = process_analysis()
        assert result is None
