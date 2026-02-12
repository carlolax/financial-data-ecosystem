import pytest
from unittest.mock import patch, MagicMock
from src.pipeline.bronze.ingest import process_ingestion

# --- TEST DATA ---
MOCK_COINS = ["bitcoin", "ethereum", "solana", "cardano", "ripple", 
              "polkadot", "dogecoin", "shiba-inu", "litecoin", "tron", 
              "avalanche-2", "uniswap", "chainlink", "stellar"]
MOCK_COINS_STR = ",".join(MOCK_COINS)

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Sets up environment variables for testing."""
    monkeypatch.setenv("CRYPTO_COINS", ",".join(MOCK_COINS))
    monkeypatch.setenv("BRONZE_BUCKET_NAME", "test-bucket")

# --- TESTS ---
@patch('src.pipeline.bronze.ingest.requests.get')
def test_ingest_bronze_success(mock_get, tmp_path):
    """
    Verifies that the ingestion process runs successfully when API returns valid data.

    Scenario:
        - The CoinGecko API returns a 200 OK with valid JSON.
        - The function should save the file to the 'data/bronze' directory.

    Assertions:
        - The requests.get method was called.
        - A file was created in the correct directory.
        - The function returns the path to that file.

    Args:
        mock_get (MagicMock): Mock for requests.get.
        tmp_path (Path): Pytest fixture for a temporary directory.
    """
    # 1. SETUP: Mock a successful API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": "bitcoin", "current_price": 50000}]
    mock_get.return_value = mock_response

    # Point the output to the temporary directory
    test_ingest_dir = tmp_path / "data" / "bronze"

    # Patch 'INGEST_DATA_DIR' to match the ingest.py variable name
    with patch('src.pipeline.bronze.ingest.INGEST_DATA_DIR', test_ingest_dir), \
         patch('src.pipeline.bronze.ingest.TARGET_CRYPTO_COINS', MOCK_COINS_STR):

        # 2. EXECUTE
        result_file = process_ingestion()

        # 3. ASSERT
        assert result_file is not None
        assert result_file.exists()
        assert result_file.name.startswith("raw_prices_")

@patch('src.pipeline.bronze.ingest.requests.get')
def test_ingest_rate_limit_error(mock_get):
    """
    Verifies that the ingestion process fails gracefully after retries.

    Scenario:
        - The CoinGecko API returns a 429 status code consistently.
        - The code should retry 3 times (exponential backoff) and then raise an Exception.

    Assertions:
        - The function raises an Exception with the specific message "Failed to fetch batch".

    Args:
        mock_get (MagicMock): Mock for requests.get.
    """
    # 1. SETUP: Mock a 429 response (Rate Limit)
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_get.return_value = mock_response

    # 2. EXECUTE & ASSERT
    # The code tries 3 times before raising "Failed to fetch batch"
    with patch('src.pipeline.bronze.ingest.TARGET_CRYPTO_COINS', MOCK_COINS_STR):
        with pytest.raises(Exception) as excinfo:
            process_ingestion()

    assert "Failed to fetch batch" in str(excinfo.value)

@patch('src.pipeline.bronze.ingest.requests.get')
def test_ingest_batching_logic(mock_get, tmp_path):
    """
    Verifies that the list of coins is correctly split into chunks.

    Scenario:
        - I have 14 coins in my mock list.
        - The BATCH_SIZE is implicitly 250 (from the module), so it should fit in 1 batch.
        - To test batching *logic*, I'd ideally mock a smaller BATCH_SIZE, but for now
          I ensure that the request contains the comma-separated list of IDs.

    Assertions:
        - requests.get was called with the correct parameters.
    """
    # 1. SETUP
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_get.return_value = mock_response

    test_ingest_dir = tmp_path / "data" / "bronze"

    # Patch 'INGEST_DATA_DIR' here as well
    with patch('src.pipeline.bronze.ingest.INGEST_DATA_DIR', test_ingest_dir), \
         patch('src.pipeline.bronze.ingest.TARGET_CRYPTO_COINS', MOCK_COINS_STR):
        # 2. EXECUTE
        process_ingestion()

        # 3. ASSERT
        # Verify that the URL call included my coin IDs
        # Note: The code uses kwargs['params'] for the CoinGecko call
        args, kwargs = mock_get.call_args
        called_params = kwargs.get('params', {})

        # Check that 'bitcoin' (first) and 'stellar' (last) are in the IDs string
        assert "bitcoin" in called_params['ids']
        assert "stellar" in called_params['ids']
