"""
Abstract Base Class for Feature Engineering (Gold Layer).

This module defines the blueprint for all 'Featurizers'. 
The Gold Layer is where 'Data' becomes 'Information'. It transforms raw 
Silver price history (OHLCV) into predictive signals (Technical Indicators) 
ready for Machine Learning models.

Key Responsibilities:
- Contract Enforcement: Ensures all asset classes (Crypto, Stocks) follow the same feature logic.
- I/O Standardization: Manages the loading of Silver Parquet files and saving of Gold Parquet files.
- Extensibility: Allows for easy addition of new indicators without breaking the core pipeline.
"""

from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path

class BaseFeaturizer(ABC):
    """
    The Abstract Blueprint for Financial Feature Engineering.

    This class strictly enforces the workflow:
    1. Load Clean Data (Silver)
    2. Calculate Technical Indicators (RSI, MACD, Bollinger Bands)
    3. Save Enriched Data (Gold)

    Attributes:
        source_path (Path): The directory containing Silver Layer data.
        output_path (Path): The directory where Gold Layer data will be stored.
    """
    
    def __init__(self, source_path: Path, output_path: Path) -> None:
        """
        Initializes the Featurizer and provisions the output directory.

        Args:
            source_path (Path): The absolute path to the Silver data (e.g., data/silver/crypto_binance).
            output_path (Path): The absolute path for Gold data (e.g., data/gold/crypto_binance).
        """
        self.source_path: Path = source_path
        self.output_path: Path = output_path
        
        # Ensure the destination directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def load_data(self, coin_id: str) -> pd.DataFrame:
        """
        Retrieves the Cleaned Silver Data for a specific asset.

        Args:
            coin_id (str): The unique identifier for the asset (e.g., 'btc').

        Returns:
            pd.DataFrame: A pandas DataFrame containing the historical OHLCV data.
        """
        pass

    @abstractmethod
    def add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The Core Logic: Applies technical indicators and feature engineering.

        Implementation Requirement:
            - Must accept a clean OHLCV DataFrame.
            - Must append new columns for indicators (e.g., 'rsi_14', 'sma_50').
            - Must NOT remove original price columns unless explicitly required.

        Args:
            df (pd.DataFrame): The input DataFrame with 'open', 'high', 'low', 'close', 'volume'.

        Returns:
            pd.DataFrame: The enriched DataFrame with added feature columns.
        """
        pass

    @abstractmethod
    def save_data(self, df: pd.DataFrame, coin_id: str) -> None:
        """
        Persists the Enriched Gold Data to storage.

        Args:
            df (pd.DataFrame): The final DataFrame containing both price and features.
            coin_id (str): The unique identifier for the asset (e.g., 'btc').
        """
        pass
