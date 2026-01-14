import sys
import os
import pytest
from requests.exceptions import HTTPError

# Import the function to be tested
from cloud_functions.bronze.main import ingest_bronze

# Setup config for testing
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Test 1
def test_ingest_bronze_success(mocker):
    # Verifies that params are passed correctly to requests.get

    # SETUP:
    # Mock the 'requests' library to not hit the real CoinGecko API
    mock_requests = mocker.patch('cloud_functions.bronze.main.requests')

    # Define what the fake API should return
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "bitcoin": {"usd": 50000},
        "ethereum": {"usd": 3000},
        "solana": {"usd": 100}
    }
    mock_requests.get.return_value = mock_response

    # Mock the 'storage' library to not hit real Google Cloud
    mock_storage = mocker.patch('cloud_functions.bronze.main.storage')

    # Drill down to the blob. This mocks the entire chain: storage.Client().bucket().blob()
    mock_blob = mock_storage.Client.return_value.bucket.return_value.blob.return_value

    # EXECUTE:
    # Run the actual function
    ingest_bronze(mocker.Mock())

    # ASSERT:
    # Check that it was called with 'params', not just a long URL string
    mock_requests.get.assert_called_once_with(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": "bitcoin,ethereum,solana",
            "vs_currencies": "usd",
            "include_24hr_vol": "true"
        }
    )

    mock_blob.upload_from_string.assert_called_once()

# Test 2
def test_ingest_bronze_api_failure(mocker):
    # Test that the function handles API failures gracefully (or raises error).
    mock_requests = mocker.patch('cloud_functions.bronze.main.requests')

    # Fake a 404 Error
    mock_response = mocker.Mock()
    mock_response.status_code = 404

    # Mock raise_for_status to actually raise an error
    mock_response.raise_for_status.side_effect = Exception("Failed to fetch data")
    mock_requests.get.return_value = mock_response

    # Run function and expect it to fail
    with pytest.raises(Exception) as excinfo:
        ingest_bronze(mocker.Mock())

    assert "Failed to fetch data" in str(excinfo.value)
