import sys
import os
import pytest
import pandas as pd
import duckdb

# Import the function to be tested
from cloud_functions.silver.main import process_silver

# Setup config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

def test_silver_transformation_logic(mocker):    
    # SETUP DATA:
    fake_json_content = """
    {
        "bitcoin": {"usd": 50000.0, "usd_market_cap": 1000000.0, "usd_24h_vol": 5000.0},
        "ethereum": {"usd": 3000.0, "usd_market_cap": 500000.0, "usd_24h_vol": 2000.0}
    }
    """

    # Add time component to match the Regex expectation
    test_filename = "raw_prices_20260114_120000.json"

    # Mock setup
    mock_storage = mocker.patch('cloud_functions.silver.main.storage')
    mock_bucket = mock_storage.Client.return_value.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    def create_fake_file(filename):
        with open(filename, 'w') as f:
            f.write(fake_json_content)

    mock_blob.download_to_filename.side_effect = create_fake_file

    # Tries to upload first then read the file before it gets deleted
    captured_data = {} 

    def intercept_upload(filename):
        print(f"Intercepting file at: {filename}")
        # Read the file now while it still exists
        captured_data['df'] = pd.read_parquet(filename)

    mock_blob.upload_from_filename.side_effect = intercept_upload

    # EXECUTE:
    fake_cloud_event = mocker.Mock()
    fake_cloud_event.data = {
        "bucket": "fake-bronze-bucket",
        "name": test_filename 
    }

    process_silver(fake_cloud_event)

    # ASSERT:
    # Verify the intercepted the data
    assert 'df' in captured_data, "Did not intercept any upload call!"
    df = captured_data['df']

    print("\nIntercepted Parquet Data (Before Deletion):")
    print(df)

    # Verify logic (DuckDB transformation)
    assert "coin_id" in df.columns
    assert "price_usd" in df.columns 
    assert 50000.0 in df['price_usd'].values
    assert "bitcoin" in df['coin_id'].values
