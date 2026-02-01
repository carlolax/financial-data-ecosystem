import sys
import os
import pytest
from unittest.mock import MagicMock, patch

# --- PATH SETUP ---
# Add 'src' to the system path so I can import the pipeline modules
# (I go up two levels from 'tests/pipeline' to reach the root, then into 'src')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from pipeline.bronze.ingest import process_ingestion

# --- FIXTURES ---
@pytest.fixture
def sample_coin_data():
    """
    Fixture that provides a standardized list of mock coin data.

    Returns:
        list: A list of dictionaries representing CoinGecko API response objects.
    """
    return [
        {"id": "bitcoin", "current_price": 50000, "market_cap": 1000000},
        {"id": "ethereum", "current_price": 3000, "market_cap": 500000}
    ]

# --- TEST 1: The "Happy Path" ---
# I use @patch decorator here (utilizing 'patch')
@patch('pipeline.bronze.ingest.requests.get')
@patch('builtins.open')
@patch('os.makedirs')
@patch('pipeline.bronze.ingest.TARGET_CRYPTO_COINS', "bitcoin,ethereum")
def test_ingest_bronze_success(mock_makedirs, mock_open, mock_get, sample_coin_data):
    """
    Verifies the successful execution of the Bronze Layer ingestion process.

    Scenario:
        - The API returns a valid 200 OK response with data.
        - The file system allows writing.

    Assertions:
        1. The API endpoint (requests.get) is called.
        2. The directory creation (os.makedirs) is attempted.
        3. The file opening (open) is called with 'w' mode and a .json extension.

    Args:
        mock_makedirs (MagicMock): Mock for os.makedirs.
        mock_open (MagicMock): Mock for builtins.open.
        mock_get (MagicMock): Mock for requests.get.
        sample_coin_data (list): Fixture data.
    """
    # 1. SETUP: Create a Mock Response using MagicMock (utilizing 'MagicMock')
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_coin_data

    # Assign this mock response to the mock_get return value
    mock_get.return_value = mock_response

    # 2. EXECUTE
    output_path = process_ingestion()

    # 3. ASSERT
    # Verify API was called
    assert mock_get.called

    # Verify directory creation was attempted
    assert mock_makedirs.called

    # Verify file writing
    # I check if 'open' was called with a file ending in .json and mode 'w'
    mock_open.assert_called()
    args, _ = mock_open.call_args
    assert args[0].name.startswith("raw_prices_")
    assert str(args[0]).endswith(".json")
    assert args[1] == "w"

# --- TEST 2: Rate Limit Handling ---
@patch('pipeline.bronze.ingest.requests.get')
def test_ingest_rate_limit_error(mock_get):
    """
    Verifies that the ingestion process fails fast upon hitting a rate limit.

    Scenario:
        - The CoinGecko API returns a 429 status code.

    Assertions:
        - The function raises an Exception with the specific message "Rate limit hit".

    Args:
        mock_get (MagicMock): Mock for requests.get.
    """
    # 1. SETUP: Mock a 429 response
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_get.return_value = mock_response

    # 2. EXECUTE & ASSERT
    with pytest.raises(Exception) as excinfo:
        process_ingestion()

    assert "Rate limit hit (429)" in str(excinfo.value)

# --- TEST 3: Batching Logic ---
@patch('pipeline.bronze.ingest.time.sleep')
@patch('pipeline.bronze.ingest.requests.get')
@patch('builtins.open')
@patch('os.makedirs')
@patch('pipeline.bronze.ingest.TARGET_CRYPTO_COINS', "btc,eth,sol")
@patch('pipeline.bronze.ingest.BATCH_SIZE', 1) # Force batch size to 1
def test_ingest_batching_logic(mock_makedirs, mock_open, mock_get, mock_sleep):
    """
    Verifies the smart batching and rate-limit sleeping logic.

    Scenario:
        - Input list has 3 coins.
        - BATCH_SIZE is mocked to 1 (forcing 3 separate batches).

    Assertions:
        1. requests.get is called 3 times (once per batch).
        2. time.sleep is called 2 times (between Batch 1-2 and Batch 2-3, but NOT after Batch 3).

    Args:
        mock_makedirs (MagicMock): Mock for os.makedirs.
        mock_open (MagicMock): Mock for builtins.open.
        mock_get (MagicMock): Mock for requests.get.
        mock_sleep (MagicMock): Mock for time.sleep.
    """
    # 1. SETUP: Return empty list so logic proceeds without crashing
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_get.return_value = mock_response

    # 2. EXECUTE
    process_ingestion()

    # 3. ASSERT
    # Should call API 3 times (once per coin)
    assert mock_get.call_count == 3

    # Should sleep 2 times (between batches, but not after the last one)
    assert mock_sleep.call_count == 2
