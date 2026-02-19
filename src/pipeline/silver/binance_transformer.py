import pandas as pd
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from .base_transformer import BaseTransformer
from .config import CRYPTO_PAIRS, COLUMN_MAPPING, FINAL_COLUMNS, PARQUET_COMPRESSION
from src.utils.logger import get_logger

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
        # Initializes the Transformer using the master crypto asset list and Logger.
        super().__init__(dataset_name="crypto_binance")
        self.pairs = CRYPTO_PAIRS
        self.log = get_logger("BinanceTransformer")

    def _transform_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Internal Helper: Applies the core business logic to a raw DataFrame.

        Args:
            df (pd.DataFrame): The raw, headless CSV data.
            symbol (str): The asset symbol (e.g., 'BTCUSDT').

        Returns:
            pd.DataFrame: A clean, typed, and schema-compliant DataFrame.
        """
        # Hardening: Schema Validation
        # 1. Column Count Check (Binance 1m Klines always have 12 columns)
        if df.shape[1] != 12:
            raise ValueError(f"Schema Mismatch! Expected 12 columns, got {df.shape[1]} for {symbol}.")

        # 2. Data Type Check (Column 0 must be the Open Time integer)
        # If it's not numeric, I am likely reading a header row or garbage data.
        if not pd.api.types.is_numeric_dtype(df.iloc[:, 0]):
             raise ValueError(f"Type Mismatch! Column 0 is not numeric for {symbol}. Possible corrupt CSV.")

        # 1. Apply Semantic Naming (Map index 0 -> 'open_time_ms')
        # Note: Assumes COLUMN_MAPPING maps integers (0, 1, 2) to strings
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
        - Deduplicates records to resolve overlaps between monthly and daily files.
        - Saves a single optimized Parquet file: 'data/silver/crypto_binance/coin_id={coin}/historical_master.parquet'
        """
        self.log.info(f"Initiating Silver Transformation (Historical) for {len(self.pairs)} assets.")

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
                self.log.info(f"Skipping {symbol} (Silver Parquet already exists)")
                continue

            if not coin_source_path.exists():
                self.log.warning(f"No Bronze data found for {symbol}")
                continue

            self.log.info(f"Processing Historical Data for {symbol}.")

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
                    self.log.error(f"Error reading archive {zip_path.name}: {error}")
                    # Silent fail on bad zips to keep loop moving
                    pass

            if all_dfs:
                # Merge months
                master_df = pd.concat(all_dfs)

                # Deduplication Shield
                original_len = len(master_df)
                master_df.drop_duplicates(subset=["source_updated_at"], keep="last", inplace=True)
                dropped_rows = original_len - len(master_df)

                if dropped_rows > 0:
                    self.log.info(f"[{symbol}] Deduplication: Dropped {dropped_rows:,} overlapping rows.")

                # Sort by Time -> Write Parquet
                master_df.sort_values("source_updated_at", inplace=True)

                master_df.to_parquet(output_file, compression=PARQUET_COMPRESSION)
                self.log.info(f"Transformed {len(master_df):,} rows for {symbol}")
            else:
                self.log.warning(f"No valid data extracted for {symbol}")

    def process_recent(self):
        """
        Batch processes the recent daily files (Gap-Fill).

        Workflow:
        - Locates daily zip files in 'data/bronze/crypto_binance/recent_daily/{coin}/'
        - Loads the existing Silver Master Parquet file.
        - Merges the new daily records into the master dataset.
        - Deduplicates to ensure no overlaps (Gap-Fill Strategy).
        - Overwrites the Master Parquet file with the updated timeline.
        """
        self.log.info(f"Initiating Silver Transformation (Recent Daily) for {len(self.pairs)} assets.")
        
        recent_dir: Path = self.bronze_path / "recent_daily"

        for symbol in self.pairs:
            coin_id = symbol.replace("USDT", "").lower()
            
            # Paths
            coin_recent_path: Path = recent_dir / coin_id

            # Silver Destination
            coin_silver_path: Path = self.silver_path / f"coin_id={coin_id}"
            master_file: Path = coin_silver_path / "historical_master.parquet"

            # Pre-flight checks
            if not master_file.exists():
                self.log.warning(f"Skipping {symbol}: No Historical Master found to merge with.")
                continue

            if not coin_recent_path.exists():
                # It's possible I have history but no recent data yet
                continue

            # Find daily zips
            zip_files = sorted(list(coin_recent_path.glob("*.zip")))
            if not zip_files:
                continue

            self.log.info(f"Merging {len(zip_files)} daily files for {symbol}.")

            # 1. Load New Daily Data
            new_dfs = []
            for zip_path in zip_files:
                try:
                    with zipfile.ZipFile(zip_path) as z:
                        # Assumption: Zip contains one CSV
                        csv_name = zip_path.name.replace(".zip", ".csv")
                        with z.open(csv_name) as f:
                            # Read raw CSV (No header)
                            df = pd.read_csv(f, header=None)
                            # Transform using the SAME logic as historical
                            clean_df = self._transform_dataframe(df, symbol)
                            new_dfs.append(clean_df)
                except Exception:
                    # Silent fail on bad zips to keep loop moving
                    pass

            if new_dfs:
                try:
                    # 2. Concatenate New Data
                    daily_df = pd.concat(new_dfs)

                    # 3. Load Existing Master Data
                    master_df = pd.read_parquet(master_file)

                    # 4. Combine (History + Recent)
                    combined_df = pd.concat([master_df, daily_df])

                    # 5. Deduplicate (The Critical "Gap Fill" Step)
                    # Track overlapping rows
                    original_len = len(combined_df)
                    combined_df.drop_duplicates(subset=["source_updated_at"], keep="last", inplace=True)
                    dropped_rows = original_len - len(combined_df)

                    if dropped_rows > 0:
                        self.log.info(f"[{symbol}] Deduplication: Dropped {dropped_rows:,} overlapping rows.")

                    # 6. Sort
                    combined_df.sort_values("source_updated_at", inplace=True)

                    # 7. Atomic Overwrite
                    combined_df.to_parquet(master_file, compression=PARQUET_COMPRESSION)

                    self.log.info(f"Merged & Updated {symbol}: Now {len(combined_df):,} rows.")
                except Exception as error:
                     self.log.error(f"Failed to merge {symbol}: {error}")
            else:
                self.log.warning(f"No valid rows extracted from daily files for {symbol}")
