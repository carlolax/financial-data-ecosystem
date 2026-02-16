import pandas as pd
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from .base import BaseTransformer
from .config import CRYPTO_PAIRS, COLUMN_MAPPING, FINAL_COLUMNS, PARQUET_COMPRESSION

class BinanceTransformer(BaseTransformer):
    """
    The Concrete Transformation Engine for Binance Market Data.

    This class implements the 'BaseTransformer' contract specifically for the 
    Cryptocurrency sector via Binance. It is responsible for the crucial 'Cleaning' phase 
    of the ELT pipeline.

    Key Transformation Steps:
    1. Direct Zip Extraction: Reads CSVs directly from Bronze archives (memory-only) 
       to avoid disk I/O bottlenecks.
    2. Schema Enforcement: Maps raw integer columns to semantic business names.
    3. Timestamp Normalization: Detects and fixes inconsistent time units 
       (Milliseconds vs Microseconds) to ensure a standard UTC timeline.
    4. Partitioning: Writes data in Hive-style partitions (coin_id=btc) for 
       optimized query performance.
    """
    
    def __init__(self):
        """
        Initializes the Transformer using the master crypto asset list.
        """
        super().__init__(dataset_name="crypto_binance")
        self.pairs = CRYPTO_PAIRS

    def _transform_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Internal Helper: Applies the core business logic to a raw DataFrame.

        Args:
            df (pd.DataFrame): The raw, headless CSV data.
            symbol (str): The asset symbol (e.g., 'BTCUSDT').

        Returns:
            pd.DataFrame: A clean, typed, and schema-compliant DataFrame.
        """
        # 1. Apply Semantic Naming (Map index 0 -> 'open_time_ms')
        df.rename(columns=COLUMN_MAPPING, inplace=True)

        # 2. Add Partition Key (Essential for Hive-style storage)
        # I strip 'USDT' to create a clean 'coin_id' (e.g., 'btc')
        df["coin_id"] = symbol.replace("USDT", "").lower()

        # 3. Fix the "2025 Timestamp Bug"
        # Binance sometimes switches between Milliseconds (13 digits) and Microseconds (16 digits).
        # I detect the length of the first timestamp to determine the unit.
        sample_ts = str(int(df["open_time_ms"].iloc[0]))
        if len(sample_ts) > 13:
            # It's Microseconds (us) -> Convert to ms-based datetime
            df["source_updated_at"] = pd.to_datetime(df["open_time_ms"], unit="us", utc=True)
        else:
            # It's Milliseconds (ms) -> Standard conversion
            df["source_updated_at"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)

        # 4. Add Audit Trail (Lineage)
        df["processed_at"] = datetime.now(timezone.utc)

        # 5. Filter & Reorder Columns (Drop technical artifacts like 'ignore')
        return df[FINAL_COLUMNS]

    def process_historical(self):
        """
        Batch processes the deep historical archives (Monthly Zips).

        Workflow:
        - Scans 'data/bronze/crypto_binance/historical_monthly/{coin}/'
        - Aggregates ALL monthly files for a single coin into one master DataFrame.
        - Saves a single optimized Parquet file: 'data/silver/crypto_binance/coin_id={coin}/historical_master.parquet'
        """
        print(f"🔨 Initiating Silver Transformation (Historical) for {len(self.pairs)} assets.")

        source_dir: Path = self.bronze_path / "historical_monthly"

        for symbol in self.pairs:
            coin_id = symbol.replace("USDT", "").lower()
            coin_source_path: Path = source_dir / coin_id

            # Destination: Hive Partitioned Folder
            coin_dest_path: Path = self.silver_path / f"coin_id={coin_id}"
            coin_dest_path.mkdir(parents=True, exist_ok=True)

            output_file: Path = coin_dest_path / "historical_master.parquet"

            # Idempotency Check: Don't re-process if the Silver file exists
            if output_file.exists():
                print(f"  ⏩ Skipping {symbol} (Silver Parquet already exists)")
                continue

            if not coin_source_path.exists():
                print(f"  ⚠️  No Bronze data found for {symbol}")
                continue

            print(f"  🔄 Processing {symbol}.", end="\r")

            # memory buffer for all monthly data of this coin
            all_dfs = []
            zip_files = sorted(list(coin_source_path.glob("*.zip")))

            for zip_path in zip_files:
                try:
                    with zipfile.ZipFile(zip_path) as z:
                        # Assumption: Zip contains one CSV with the same name (minus .zip)
                        csv_name = zip_path.name.replace(".zip", ".csv")
                        with z.open(csv_name) as f:
                            # Read raw CSV (No header)
                            df = pd.read_csv(f, header=None)
                            clean_df = self._transform_dataframe(df, symbol)
                            all_dfs.append(clean_df)
                except Exception as error:
                    print(f"\n  ❌ Error reading archive {zip_path.name}: {error}")

            if all_dfs:
                # Merge months -> Sort by Time -> Write Parquet
                master_df = pd.concat(all_dfs)
                master_df.sort_values("source_updated_at", inplace=True)

                master_df.to_parquet(output_file, compression=PARQUET_COMPRESSION)
                print(f"  ✅ Transformed {len(master_df):,} rows for {symbol}       ")
            else:
                print(f"  ⚠️  No valid data extracted for {symbol}      ")

    def process_recent(self):
        """
        Batch processes the recent daily files (Gap-Fill).

        Current Status: Placeholder.
        Future Logic: Read daily Zips -> Clean -> Append to Master Parquet.
        """
        print("🚧 Recent data transformation module is under construction.")
