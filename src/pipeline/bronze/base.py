from abc import ABC, abstractmethod
from pathlib import Path
from .config import BRONZE_DIR

class BaseIngestor(ABC):
    """
    The Abstract Blueprint for Financial Data Ingestion.

    This class defines the strict contract that all specific data ingestors must follow.
    It implements the 'Template Method' design pattern, ensuring that regardless of 
    the asset class (Crypto, Stocks, Forex), the ingestion workflow remains consistent.

    Attributes:
        asset_type (str): The category of the asset (e.g., 'crypto_binance'), 
                          used to organize the data directory structure.
        base_path (Path): The absolute path to the bronze storage directory for this asset type.
    """

    def __init__(self, asset_type: str) -> None:
        """
        Initializes the Ingestor and provisions the storage directory.

        Args:
            asset_type (str): A unique identifier for the data source (e.g., 'stocks_yahoo').
                              This is used to create a dedicated folder in the Bronze layer.
        """
        self.asset_type: str = asset_type
        self.base_path: Path = BRONZE_DIR / asset_type

        # Ensure the destination directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def ingest_historical(self) -> None:
        """
        Orchestrates the massive retrieval of deep historical archives.

        Implementation Requirement:
            - Must iterate through years/months of available data.
            - Must handle bulk downloads (e.g., Zip files).
            - Must be idempotent (skip files that already exist).
        """
        pass

    @abstractmethod
    def ingest_recent(self) -> None:
        """
        Bridges the temporal gap between the historical archives and the present day.

        Implementation Requirement:
            - Must identify missing days from the 1st of the current month up to yesterday.
            - Must download daily granular files to ensure data continuity.
        """
        pass

    @abstractmethod
    def ingest_live(self) -> None:
        """
        Establishes a persistent, real-time connection to the market.

        Implementation Requirement:
            - Must connect via WebSocket or high-frequency polling.
            - Must run indefinitely until manually stopped.
            - Must capture market events with millisecond precision.
        """
        pass
