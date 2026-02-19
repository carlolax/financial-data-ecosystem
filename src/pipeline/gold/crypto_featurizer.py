"""
Concrete Implementation of Feature Engineering for Cryptocurrency Assets.

This module contains the 'CryptoFeaturizer', which applies standard technical analysis
indicators to the Silver Layer data. It uses the 'pandas_ta' library to calculate
signals efficiently.

Key Features:
- **Momentum:** Relative Strength Index (RSI).
- **Trend:** Moving Average Convergence Divergence (MACD).
- **Volatility:** Bollinger Bands (BBANDS).
- **Averages:** Simple Moving Averages (SMA) for 50 and 200 periods.
- **Stationarity:** Logarithmic Returns (essential for Machine Learning).
"""

import pandas as pd
import pandas_ta as ta
import numpy as np
from pathlib import Path
from typing import Optional

from .base_featurizer import BaseFeaturizer

class CryptoFeaturizer(BaseFeaturizer):
    """
    Applies Technical Analysis (TA) indicators to Crypto OHLCV data.

    Inherits from:
        BaseFeaturizer: Enforces the load -> transform -> save contract.
    """

    def __init__(self) -> None:
        """
        Initializes the Featurizer with standard paths.

        Source: data/silver/crypto_binance
        Output: data/gold/crypto_binance
        """
        project_root: Path = Path(__file__).resolve().parent.parent.parent.parent

        super().__init__(
            source_path=project_root / "data" / "silver" / "crypto_binance",
            output_path=project_root / "data" / "gold" / "crypto_binance"
        )

    def load_data(self, coin_id: str) -> pd.DataFrame:
        """
        Retrieves the Cleaned Silver Data for a specific asset.

        Args:
            coin_id (str): The unique identifier (e.g., 'btc').

        Returns:
            pd.DataFrame: Sorted OHLCV data.

        Raises:
            FileNotFoundError: If the Silver Parquet file does not exist.
        """
        file_path: Path = self.source_path / f"coin_id={coin_id}" / "historical_master.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"❌ Silver data not found for {coin_id} at {file_path}")

        # Load Parquet file
        df: pd.DataFrame = pd.read_parquet(file_path)

        # Ensure rows are sorted by time (Crucial for TA calculations)
        df.sort_values("source_updated_at", inplace=True)

        df.reset_index(drop=True, inplace=True)

        return df

    def add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        The Core Logic: Applies technical indicators and feature engineering.

        Indicators Added:
        1. RSI (14)
        2. MACD (12, 26, 9)
        3. Bollinger Bands (20, 2)
        4. SMA (50, 200)
        5. Log Returns (1-period)
        """
        # Work on a copy to avoid SettingWithCopy warnings
        df = df.copy()

        # 1. RSI (Relative Strength Index) - Momentum
        # detecting Overbought (>70) or Oversold (<30) conditions.
        df["rsi_14"] = ta.rsi(df["close"], length=14)

        # 2. MACD (Moving Average Convergence Divergence) - Trend
        # Returns three columns: MACD line, Histogram, and Signal line.
        macd: Optional[pd.DataFrame] = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None:
            df = pd.concat([df, macd], axis=1)

        # 3. Bollinger Bands - Volatility
        # Measures if price is high or low relative to recent volatility.
        bbands: Optional[pd.DataFrame] = ta.bbands(df["close"], length=20, std=2)
        if bbands is not None:
            df = pd.concat([df, bbands], axis=1)

        # 4. SMA (Simple Moving Average) - Long Term Trend
        # The 'Golden Cross' happens when SMA_50 crosses above SMA_200.
        df["sma_50"] = ta.sma(df["close"], length=50)
        df["sma_200"] = ta.sma(df["close"], length=200)

        # 5. Log Returns - Target Variable for ML
        # I use Log Returns because they are additive and symmetric (better for math).
        # Shift(1) means "Previous Close".
        # I use numpy for vectorization speed.
        df["log_ret"] = np.log(df["close"] / df["close"].shift(1))

        # Replace infinite values (division by zero) with NaNs so they can be dropped
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # 6. Clean Up
        # Indicators like SMA_200 require the first 200 rows to calculate.
        # These initial rows will be NaN (Not a Number). I drop them to keep data clean.
        original_len = len(df)
        df.dropna(inplace=True)
        dropped_rows = original_len - len(df)

        if dropped_rows > 0:
            # I don't print here to avoid spamming the console, but good to know logic works
            pass

        return df

    def save_data(self, df: pd.DataFrame, coin_id: str) -> None:
        """
        Persists the Enriched Gold Data to storage.

        Args:
            df (pd.DataFrame): The feature-engineered dataset.
            coin_id (str): The asset identifier.
        """
        # Destination: data/gold/crypto_binance/coin_id={coin}/
        coin_dir: Path = self.output_path / f"coin_id={coin_id}"
        coin_dir.mkdir(parents=True, exist_ok=True)

        output_file: Path = coin_dir / "features.parquet"

        # Save with Snappy compression (Fast & Efficient)
        df.to_parquet(output_file, compression="snappy")
        print(f"  ✅ Saved Gold Data: {coin_id.upper()} ({len(df):,} rows, {len(df.columns)} features)")
