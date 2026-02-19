import requests
import websocket
import json
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .base_ingestor import BaseIngestor
from .config import CRYPTO_PAIRS, BINANCE_CONFIG
from src.utils.logger import get_logger

class BinanceIngestor(BaseIngestor):
    """
    The Concrete Implementation for Cryptocurrency Ingestion via Binance.

    This class fulfills the 'BaseIngestor' contract specifically for the Binance Exchange.
    It handles the nuances of the Binance Vision API, including URL construction, 
    zip file handling, and WebSocket subscription management.
    """

    def __init__(self) -> None:
        # Initializes the Binance Ingestor with the master crypto pair list and Logger.
        super().__init__(asset_type="crypto_binance")
        self.pairs: list[str] = CRYPTO_PAIRS
        self.config: dict = BINANCE_CONFIG

        # Initialize the Observer-based Logger
        self.log = get_logger("BinanceIngestor")

    def _is_valid_zip(self, file_path: Path) -> bool:
        # Checks if a file is a valid, non-empty Zip archive.
        if not file_path.exists():
            return False
        if file_path.stat().st_size == 0:
            return False
        if not zipfile.is_zipfile(file_path):
            return False
        return True

    def ingest_historical(self) -> None:
        """
        Downloads monthly 1-minute kline archives (Zip format) from Binance Vision.

        Range: July 2017 to Present.
        """
        self.log.info(f"Initiating Deep Historical Backfill for {len(self.pairs)} assets.")
        dest_dir: Path = self.base_path / "historical_monthly"

        years: list[str] = ["2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]
        months: list[str] = [f"{i:02d}" for i in range(1, 13)]

        for symbol in self.pairs:
            coin_dir: Path = dest_dir / symbol.replace("USDT", "").lower()
            coin_dir.mkdir(parents=True, exist_ok=True)

            self.log.info(f"Scanning archives for {symbol}.")
            for year in years:
                for month in months:
                    if year == "2017" and int(month) < 8:
                        continue

                    filename: str = f"{symbol}-{self.config['INTERVAL']}-{year}-{month}.zip"
                    url: str = f"{self.config['MONTHLY_URL']}/{symbol}/{self.config['INTERVAL']}/{filename}"
                    save_path: Path = coin_dir / filename

                    # Hardening: Check existing file integrity
                    if save_path.exists():
                        if self._is_valid_zip(save_path):
                            continue
                        else:
                            self.log.warning(f"Found corrupt/empty file: {filename}. Deleting.")
                            save_path.unlink()

                    # Download Logic with Smart HTTP Handling
                    try:
                        resp = requests.get(url)

                        if resp.status_code == 200:
                            with open(save_path, "wb") as f:
                                f.write(resp.content)

                            if not self._is_valid_zip(save_path):
                                save_path.unlink()
                                self.log.error(f"Integrity Check Failed (Deleted): {filename}")
                            else:
                                self.log.info(f"Secured: {filename}")

                        elif resp.status_code == 404:
                            self.log.warning(f"404 Not Found: {filename} (Asset likely unlisted at this time)")

                        elif resp.status_code in [429, 418]:
                            self.log.error("Rate Limit Exceeded / IP Banned by Binance! Sleeping for 5 minutes.")
                            time.sleep(300) # Sleep for 5 minutes to let the ban lift

                        else:
                            self.log.error(f"Unexpected HTTP {resp.status_code} for {filename}")

                    except Exception as error:
                        self.log.error(f"Network Error during download: {error}")

    def ingest_recent(self) -> None:
        """
        Downloads daily 1-minute kline archives for the current incomplete month.

        Range: 1st of current month -> Yesterday.
        """
        self.log.info("Synchronizing Recent Daily Data.")
        dest_dir: Path = self.base_path / "recent_daily"

        today: datetime = datetime.now(timezone.utc)
        start_date: datetime = today.replace(day=1)
        end_date: datetime = today - timedelta(days=1)

        dates: list[datetime] = []
        curr: datetime = start_date
        while curr <= end_date:
            dates.append(curr)
            curr += timedelta(days=1)

        for symbol in self.pairs:
            coin_dir: Path = dest_dir / symbol.replace("USDT", "").lower()
            coin_dir.mkdir(parents=True, exist_ok=True)

            for d in dates:
                date_str: str = d.strftime("%Y-%m-%d")
                filename: str = f"{symbol}-{self.config['INTERVAL']}-{date_str}.zip"
                url: str = f"{self.config['DAILY_URL']}/{symbol}/{self.config['INTERVAL']}/{filename}"
                save_path: Path = coin_dir / filename

                if save_path.exists():
                    if self._is_valid_zip(save_path):
                        continue
                    else:
                        self.log.warning(f"Found corrupt/empty daily file: {filename}. Deleting.")
                        save_path.unlink()

                try:
                    resp = requests.get(url)
                    if resp.status_code == 200:
                        with open(save_path, "wb") as f:
                            f.write(resp.content)

                        if not self._is_valid_zip(save_path):
                            save_path.unlink()
                            self.log.error(f"Integrity Check Failed for daily file: {filename}")
                        else:
                            self.log.info(f"Secured Daily: {filename}")

                    elif resp.status_code == 404:
                        self.log.warning(f"404 Not Found: {filename} (Data pending from Binance)")

                    elif resp.status_code in [429, 418]:
                        self.log.error("Rate Limit Exceeded! Sleeping for 5 minutes.")
                        time.sleep(300)

                except Exception as error:
                    self.log.error(f"Network Error: {error}")

    def ingest_live(self) -> None:
        """
        Connects to the Binance WebSocket Stream to capture real-time market data.

        Output: Appends row-based CSV data to a local buffer file.
        """
        self.log.info("Establishing Real-Time WebSocket Connection.")
        buffer_file: Path = self.base_path / "live_buffer" / "stream_buffer.csv"
        buffer_file.parent.mkdir(parents=True, exist_ok=True)

        def on_open(_ws: websocket.WebSocketApp) -> None:
            self.log.info("WebSocket Connected successfully.")
            params = [f"{c.lower()}@kline_1m" for c in self.pairs]
            _ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))

        def on_message(_ws: websocket.WebSocketApp, message: str) -> None:
            data = json.loads(message)
            if 'k' in data and data['k']['x']: 
                k = data['k']
                row = f"{k['s']},{k['t']},{k['o']},{k['h']},{k['l']},{k['c']},{k['v']}\n"
                with open(buffer_file, "a") as f:
                    f.write(row)
                # I use print here instead of logger to prevent the log file from growing to 10GB
                print(f"  💾 Captured: {k['s']} @ {k['c']}     ", end="\r")

        while True:
            try:
                ws = websocket.WebSocketApp(self.config['WS_URL'], on_open=on_open, on_message=on_message)
                ws.run_forever()
            except KeyboardInterrupt:
                self.log.warning("Stream Terminated by User.")
                print("\n") # Clear the carriage return
                break
            except Exception as error:
                self.log.error(f"WebSocket Error: {error}. Reconnecting in 5 seconds.")
                time.sleep(5)
