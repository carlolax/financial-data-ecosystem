import sys
import os
import pytest
import json
import pandas as pd
from unittest.mock import patch

# --- PATH SETUP ---
# Add 'src' to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from pipeline.silver.clean import process_cleaning

# --- FIXTURES ---
@pytest.fixture
def sample_bronze_data():
    """
    Returns a list of raw dictionaries mimicking CoinGecko JSON structure.
    Includes a case for FDV calculation (Max Supply vs Total Supply).
    """
    return [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 50000,
            "market_cap": 1000000000,
            "market_cap_rank": 1,
            "total_volume": 20000,
            "high_24h": 51000,
            "low_24h": 49000,
            "price_change_percentage_24h": 2.5,
            "circulating_supply": 19000000,
            "total_supply": 21000000,
            "max_supply": 21000000,
            "ath": 69000,
            "ath_change_percentage": -25.0,
            "ath_date": "2021-11-10T00:00:00.000Z",
            "last_updated": "2023-10-01T12:00:00.000Z",
            "ingested_timestamp": "2023-10-01T12:05:00.000Z"
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 3000,
            "market_cap": 350000000,
            "market_cap_rank": 2,
            "total_volume": 15000,
            "high_24h": 3100,
            "low_24h": 2900,
            "price_change_percentage_24h": 1.2,
            "circulating_supply": 120000000,
            "total_supply": 120000000,
            "max_supply": None,
            "ath": 4800,
            "ath_change_percentage": -35.0,
            "ath_date": "2021-11-10T00:00:00.000Z",
            "last_updated": "2023-10-01T12:00:00.000Z",
            "ingested_timestamp": "2023-10-01T12:05:00.000Z"
        }
    ]

# --- TEST 1: End-to-End Transformation ---
def test_process_cleaning_success(tmp_path, sample_bronze_data):
    """
    Verifies the complete Silver Layer transformation logic using a temporary file system.

    Scenario:
        - A temporary 'bronze' folder is created with valid JSON data.
        - The process_cleaning function is run.

    Assertions:
        1. A Parquet file is created in the temporary 'silver' folder.
        2. The output file contains the correct columns (e.g., 'coin_id', 'processed_at').
        3. The 'fully_diluted_valuation' logic correctly handles NULL max_supply.
    """
    # 1. SETUP: Create Temp Directories mimicking the project structure
    # tmp_path is a built-in Pytest fixture that cleans up after itself
    temp_bronze = tmp_path / "data" / "bronze"
    temp_silver = tmp_path / "data" / "silver"
    temp_bronze.mkdir(parents=True)
    temp_silver.mkdir(parents=True)

    # Write sample data to a temp JSON file
    input_file = temp_bronze / "raw_prices_test.json"
    with open(input_file, "w") as f:
        json.dump(sample_bronze_data, f)

    # 2. PATCH: Redirect the code to use my Temp folders instead of the real ones.
    with patch("pipeline.silver.clean.BRONZE_DIR", temp_bronze), \
         patch("pipeline.silver.clean.SILVER_DIR", temp_silver):

        # 3. EXECUTE: The function no longer returns a path, it just runs.
        process_cleaning()

        # 4. ASSERT: I search for ANY parquet file created in the folder.
        generated_files = list(temp_silver.glob("*.parquet"))
        assert len(generated_files) > 0, "No Parquet file was created in Silver"

        output_path = generated_files[0]
        assert output_path.exists()

        # Verify Data Integrity using Pandas
        df = pd.read_parquet(output_path)

        # Check Columns
        assert "coin_id" in df.columns # Renamed from 'id'
        assert "processed_at" in df.columns # Added timestamp
        assert "fully_diluted_valuation" in df.columns # Calculated field

        # Check Logic: Bitcoin FDV (Price * Max Supply)
        btc_row = df[df['symbol'] == 'btc'].iloc[0]
        expected_btc_fdv = 50000 * 21000000
        assert btc_row['fully_diluted_valuation'] == expected_btc_fdv

        # Check Logic: Ethereum FDV (Price * Total Supply because Max is Null)
        eth_row = df[df['symbol'] == 'eth'].iloc[0]
        expected_eth_fdv = 3000 * 120000000
        assert eth_row['fully_diluted_valuation'] == expected_eth_fdv

# --- TEST 2: Missing Files Error ---
def test_process_cleaning_no_files(tmp_path):
    """
    Verifies that the process raises a FileNotFoundError if the Bronze directory is empty.

    Scenario:
        - The Bronze directory exists but contains no files matching the pattern.
    """
    # 1. SETUP: Empty Temp Directory
    temp_bronze = tmp_path / "data" / "bronze"
    temp_silver = tmp_path / "data" / "silver"
    temp_bronze.mkdir(parents=True)

    # 2. PATCH & EXECUTE
    with patch("pipeline.silver.clean.BRONZE_DIR", temp_bronze), \
         patch("pipeline.silver.clean.SILVER_DIR", temp_silver):

        try:
            process_cleaning()
        except FileNotFoundError:
            pytest.fail("process_cleaning() crashed on missing files! Should exit gracefully.")
