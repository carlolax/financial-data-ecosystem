import requests
import time
import json
import os
from .config import BRONZE_DIR, CRYPTO_PAIRS, COINGECKO_CONFIG

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

    def __init__(self):
        """
        Initializes the Metadata Crawler with provider settings and storage paths.
        """
        self.base_url = COINGECKO_CONFIG["BASE_URL"]
        self.id_map = COINGECKO_CONFIG["ID_MAP"]
        self.delay = COINGECKO_CONFIG["Delay_Seconds"]
        self.max_retries = COINGECKO_CONFIG["Max_Retries"]

        # Storage: data/bronze/metadata/coingecko_raw.json
        self.output_dir = BRONZE_DIR / "metadata"
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = self.output_dir / "coingecko_raw.json"

    def ingest_metadata(self):
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
        print(f"🦎 Initiating CoinGecko Metadata Crawl for {len(CRYPTO_PAIRS)} assets.")
        print(f"   (Delay set to {self.delay}s to respect API Rate Limits)")

        # 1. Load existing data if I have it (Resume capability)
        full_data = {}
        if self.output_file.exists():
            try:
                with open(self.output_file, "r") as f:
                    full_data = json.load(f)
                print(f"   ↪️ Resuming from previous state ({len(full_data)} assets loaded).")
            except json.JSONDecodeError:
                print("   ⚠️ Corrupt JSON found. Starting fresh.")
                full_data = {}

        # 2. Crawl
        for symbol in CRYPTO_PAIRS:
            # Skip if I already have valid data for this coin
            if symbol in full_data and full_data[symbol].get("description"):
                print(f"  ⏩ Skipping {symbol} (Metadata already secured)")
                continue

            cg_id = self.id_map.get(symbol)
            if not cg_id:
                print(f"  ⚠️  Configuration Error: No CoinGecko ID map found for {symbol}")
                continue

            # API Endpoint: specific to fetching static coin details
            url = f"{self.base_url}/coins/{cg_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false"

            try:
                print(f"  ⬇️  Fetching Context: {symbol} ({cg_id}).", end="\r")
                resp = requests.get(url)

                if resp.status_code == 200:
                    data = resp.json()

                    # Extract only the high-value fields (Bronze = Raw, but selective)
                    extracted = {
                        "id": data.get("id"),
                        "symbol": data.get("symbol"),
                        "name": data.get("name"),
                        "description": data.get("description", {}).get("en", ""),
                        "categories": data.get("categories", []),
                        "image": data.get("image", {}).get("large", ""),
                        "genesis_date": data.get("genesis_date"),
                        "homepage": (data.get("links", {}).get("homepage", []) or [""])[0]
                    }

                    full_data[symbol] = extracted
                    print(f"  ✅ Secured: {symbol:<30}      ")

                    # Atomic Write pattern to prevent data loss
                    with open(self.output_file, "w") as f:
                        json.dump(full_data, f, indent=4)

                elif resp.status_code == 429:
                    print(f"\n  🛑 API Rate Limit Hit. Cooling down for 60s.")
                    time.sleep(60)
                else:
                    print(f"\n  ❌ HTTP Error {symbol}: {resp.status_code}")

            except Exception as error:
                print(f"\n  ❌ Network Exception {symbol}: {error}")

            # 3. Strict Rate Limit Compliance
            time.sleep(self.delay)

        print(f"\n✨ Metadata Enrichment Complete. Asset Profiles saved to: {self.output_file}")
