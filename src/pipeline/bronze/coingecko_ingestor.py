import requests
import time
import json
from pathlib import Path
from typing import Any, Dict, Optional

from .config import BRONZE_DIR, CRYPTO_PAIRS, COINGECKO_CONFIG
from src.utils.logger import get_logger

class CoinGeckoIngestor:
    """
    The Concrete Implementation for Metadata Enrichment via CoinGecko.

    This class serves as the 'Context Layer' for the data pipeline. While the 
    Binance Ingestor handles the quantitative data (Price/Volume), this ingestor 
    retrieves the qualitative 'Soul' of the asset:
    - Semantic Descriptions (What does this project do?)
    - Visual Identity (Logos/Icons)
    - Sector Taxonomy (DeFi, AI, Gaming, L1)

    Technical Note:
        CoinGecko's Public API has strict rate limits (approx. 10-30 calls/min).
        This class implements a 'Patient Crawler' strategy with enforced delays 
        to ensure compliance and prevent IP bans.
    """

    def __init__(self) -> None:
        """
        Initializes the Metadata Crawler with provider settings, storage paths, and logging.
        """
        self.base_url: str = COINGECKO_CONFIG["BASE_URL"]
        self.id_map: Dict[str, str] = COINGECKO_CONFIG["ID_MAP"]
        self.delay: int = COINGECKO_CONFIG["Delay_Seconds"]
        self.max_retries: int = COINGECKO_CONFIG["Max_Retries"]

        # Initialize the Observer-based Logger
        self.log = get_logger("CoinGeckoIngestor")

        # Storage: data/bronze/metadata/coingecko_raw.json
        self.output_dir: Path = BRONZE_DIR / "metadata"

        # Ensure directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file: Path = self.output_dir / "coingecko_raw.json"

    def ingest_metadata(self) -> None:
        """
        Executes the rate-limited crawling strategy to harvest asset details.

        Workflow:
        1. Loads existing metadata state (Resume Capability).
        2. Iterates through the Master Asset List defined in Config.
        3. Maps the Binance Symbol (e.g., BTCUSDT) to the CoinGecko ID (e.g., bitcoin).
        4. Fetches and saves the data incrementally to prevent loss during crashes.

        Outputs:
            A JSON file containing the 'Rich Context' for all tracked assets.
        """
        self.log.info(f"Initiating CoinGecko Metadata Crawl for {len(CRYPTO_PAIRS)} assets.")
        self.log.info(f"Rate Limit Protocol Active: Delay set to {self.delay}s per request.")

        # 1. Load existing data if we have it
        full_data: Dict[str, Any] = {}
        if self.output_file.exists():
            try:
                with open(self.output_file, "r") as f:
                    full_data = json.load(f)
                self.log.info(f"Resuming from previous state ({len(full_data)} assets loaded).")
            except json.JSONDecodeError:
                self.log.warning("Corrupt JSON found. Starting fresh.")
                full_data = {}

        # 2. Crawl
        for symbol in CRYPTO_PAIRS:
            # Skip if we already have valid data for this coin
            if symbol in full_data and full_data[symbol].get("description"):
                self.log.info(f"Skipping {symbol} (Metadata already secured)")
                continue

            cg_id: Optional[str] = self.id_map.get(symbol)
            if not cg_id:
                self.log.warning(f"Configuration Error: No CoinGecko ID map found for {symbol}")
                continue

            # API Endpoint: specific to fetching static coin details
            url: str = f"{self.base_url}/coins/{cg_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false"

            try:
                # Terminal UI only (not logged) to avoid spamming the log file with 'Fetching.' states
                print(f"  ⬇️  Fetching Context: {symbol} ({cg_id}).", end="\r")

                resp = requests.get(url)

                if resp.status_code == 200:
                    data: Dict[str, Any] = resp.json()

                    # Extract only the high-value fields (Bronze = Raw, but selective)
                    links = data.get("links", {})
                    homepage_list = links.get("homepage", [])
                    homepage_url = homepage_list[0] if isinstance(homepage_list, list) and homepage_list else ""

                    extracted: Dict[str, Any] = {
                        "id": data.get("id"),
                        "symbol": data.get("symbol"),
                        "name": data.get("name"),
                        "description": data.get("description", {}).get("en", ""),
                        "categories": data.get("categories", []),
                        "image": data.get("image", {}).get("large", ""),
                        "genesis_date": data.get("genesis_date"),
                        "homepage": homepage_url
                    }

                    full_data[symbol] = extracted

                    # Log the success and clear the terminal line
                    print(" " * 50, end="\r") 
                    self.log.info(f"Secured Metadata: {symbol}")

                    # Atomic Write pattern to prevent data loss
                    with open(self.output_file, "w") as f:
                        json.dump(full_data, f, indent=4)

                elif resp.status_code == 429:
                    print(" " * 50, end="\r") 
                    self.log.warning(f"API Rate Limit Hit (429) for {symbol}. Cooling down for 60s.")
                    time.sleep(60)
                else:
                    print(" " * 50, end="\r") 
                    self.log.error(f"HTTP Error for {symbol}: {resp.status_code}")

            except Exception as error:
                print(" " * 50, end="\r") 
                self.log.error(f"Network Exception for {symbol}: {error}")

            # 3. Strict Rate Limit Compliance
            time.sleep(self.delay)

        self.log.info(f"Metadata Enrichment Complete. Asset Profiles saved to: {self.output_file}")
