import pytest
import json
import duckdb
from src.pipeline.silver.clean import process_cleaning
from unittest.mock import patch

@pytest.fixture
def raw_data():
    """
    Generates a mock 'Bronze Layer' dataset conforming to the V2 Rich Schema.

    This fixture simulates the JSON output from the CoinGecko API, ensuring
    all critical financial fields are present for testing:
    - Standard: id, symbol, current_price
    - V2 Additions: market_cap, market_cap_rank, fully_diluted_valuation, total_volume
    - Edge Cases: Explicitly tests 'max_supply' as None (e.g., ETH) to ensure stability.

    Returns:
        list[dict]: A list of dictionary objects representing raw coin data.
    """
    return [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 50000,
            "market_cap": 900000000000,
            "market_cap_rank": 1,
            "fully_diluted_valuation": 950000000000,
            "total_volume": 30000000000,
            "high_24h": 51000,
            "low_24h": 49000,
            "price_change_percentage_24h": 2.5,
            "circulating_supply": 18000000,
            "total_supply": 21000000,
            "max_supply": 21000000,
            "ath": 69000,
            "ath_change_percentage": -20.5,
            "ath_date": "2021-11-10T00:00:00.000Z",
            "last_updated": "2023-10-27T10:00:00.000Z",
            "ingested_timestamp": "2023-10-27T10:05:00.000Z"
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 3000,
            "market_cap": 350000000000,
            "market_cap_rank": 2,
            "total_volume": 15000000000,
            "high_24h": 3100,
            "low_24h": 2900,
            "price_change_percentage_24h": -1.2,
            "circulating_supply": 120000000,
            "total_supply": 120000000,
            "max_supply": None, 
            "ath": 4800,
            "ath_change_percentage": -30.1,
            "ath_date": "2021-11-10T00:00:00.000Z",
            "last_updated": "2023-10-27T10:00:00.000Z",
            "ingested_timestamp": "2023-10-27T10:05:00.000Z"
        }
    ]

def test_process_cleaning_success(tmp_path, raw_data):
    """
    Verifies that the Silver Layer correctly transforms raw JSON into Parquet
    while preserving the V2 Rich Schema.

    Scenario:
        - Input: A raw JSON file containing V2 fields (FDV, Rank, Volume).
        - Action: The cleaning process runs via DuckDB.
        - Expectation:
            1. The output Parquet file is created.
            2. All V2 fields (market_cap_rank, fully_diluted_valuation, etc.) exist in the output schema.
            3. Data types are correct (e.g., NULL max_supply is preserved, not crashed).

    Args:
        tmp_path (Path): Pytest fixture for temporary file operations.
        raw_data (list): The mock data fixture.
    """
    # 1. SETUP: Create a raw JSON file
    bronze_dir = tmp_path / "data" / "bronze"
    silver_dir = tmp_path / "data" / "silver"
    bronze_dir.mkdir(parents=True)
    silver_dir.mkdir(parents=True)

    raw_file = bronze_dir / "raw_prices_20231027_100000.json"
    expected_output = silver_dir / "clean_prices_20231027_100000.parquet"

    with open(raw_file, "w") as f:
        json.dump(raw_data, f)

    # Patch the directory constants to use my tmp_path
    with patch("src.pipeline.silver.clean.BRONZE_DIR", bronze_dir), \
         patch("src.pipeline.silver.clean.SILVER_DIR", silver_dir):

        # 2. EXECUTE
        process_cleaning()

        # 3. ASSERT
        assert expected_output.exists()

        # Verify Schema with DuckDB
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM '{expected_output}'").df()

        # Filter for 'bitcoin' before asserting the price
        btc_price = df[df['coin_id'] == 'bitcoin']['current_price'].iloc[0]
        assert btc_price == 50000

        # Check column mapping (id -> coin_id)
        assert "coin_id" in df.columns

        # Check V2 Rich Schema columns
        assert "fully_diluted_valuation" in df.columns
        assert "market_cap_rank" in df.columns
        assert "ath_change_percentage" in df.columns

        # Check Logic: ETH (index 1) has None max_supply
        eth_row = df[df['symbol']=='eth']
        assert eth_row['max_supply'].isna().any()

def test_process_cleaning_no_files(tmp_path):
    """
    Verifies that the Silver Layer handles empty input directories gracefully.

    Scenario:
        - Input: An empty 'data/bronze' directory.
        - Action: The cleaning process is triggered.
        - Expectation: The function returns None and does not crash or create empty files.
    """
    bronze_dir = tmp_path / "data" / "bronze"
    silver_dir = tmp_path / "data" / "silver"
    bronze_dir.mkdir(parents=True)

    with patch("src.pipeline.silver.clean.BRONZE_DIR", bronze_dir), \
         patch("src.pipeline.silver.clean.SILVER_DIR", silver_dir):

        result = process_cleaning()
        assert result is None
