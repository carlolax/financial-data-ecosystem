import os
from abc import ABC, abstractmethod
from pathlib import Path
from .config import SILVER_DIR, BRONZE_DIR

class BaseTransformer(ABC):
    """
    The Abstract Blueprint for Silver Layer Transformation.

    This class serves as the foundational contract for all data cleaning operations.
    It implements the 'Template Method' design pattern, standardizing how raw data 
    (Bronze) is located, processed, and stored as refined assets (Silver).

    Key Architectural Goals:
    - Path Abstraction: Automatically resolves input/output directories based on dataset name.
    - Consistency: Enforces strict implementation of Historical vs. Recent processing logic.
    - Scalability: Allows seamless addition of new asset classes (e.g., StocksTransformer) 
      without modifying the core pipeline logic.

    Attributes:
        dataset_name (str): The unique identifier for the data source (e.g., 'crypto_binance').
        bronze_path (Path): The absolute path to the Raw Input directory.
        silver_path (Path): The absolute path to the Cleaned Output directory.
    """
    
    def __init__(self, dataset_name: str):
        """
        Initializes the Transformer and provisions the Silver storage layer.

        Args:
            dataset_name (str): The directory name used in the Bronze layer 
                                (e.g., 'crypto_binance'). This maps 1:1 to the Silver layer.
        """
        self.dataset_name = dataset_name
        self.bronze_path = BRONZE_DIR / dataset_name
        self.silver_path = SILVER_DIR / dataset_name
        
        # Ensure the destination directory exists before processing begins
        os.makedirs(self.silver_path, exist_ok=True)

    @abstractmethod
    def process_historical(self):
        """
        Orchestrates the transformation of deep historical archives.

        Implementation Contract:
        1. Locate and read raw archive files (e.g., Monthly Zips).
        2. Apply schema enforcement and data type casting.
        3. Write optimized, partitioned files (e.g., Parquet) to Silver storage.
        4. Must be idempotent (skip if output already exists).
        """
        pass

    @abstractmethod
    def process_recent(self):
        """
        Orchestrates the transformation of recent incremental data.

        Implementation Contract:
        1. Locate daily gap-fill files.
        2. Clean and merge them into the existing historical dataset 
           or store them as strictly appended partitions.
        """
        pass
