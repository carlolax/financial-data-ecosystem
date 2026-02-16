import requests
import websocket
import json
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .base import BaseIngestor
from .config import CRYPTO_PAIRS, BINANCE_CONFIG

class BinanceIngestor(BaseIngestor):
    """
    The Concrete Implementation for Cryptocurrency Ingestion via Binance.

    This class fulfills the 'BaseIngestor' contract specifically for the Binance Exchange.
    It handles the nuances of the Binance Vision API, including URL construction, 
    zip file handling, and WebSocket subscription management.
    """

    def __init__(self):
        """
        Initializes the Binance Ingestor with the master crypto pair list.
        """
        super().__init__(asset_type="crypto_binance")
        self.pairs = CRYPTO_PAIRS
        self.config = BINANCE_CONFIG

    def _is_valid_zip(self, file_path: Path) -> bool:
        """
        Helper: Checks if a file is a valid, non-empty Zip archive.
        """
        if not file_path.exists():
            return False

        # Check if the file is empty
        if file_path.stat().st_size == 0:
            return False

        # Check if the zip structure is intact
        if not zipfile.is_zipfile(file_path):
            return False

        return True

    def ingest_historical(self):
        """
        Downloads monthly 1-minute kline archives (Zip format) from Binance Vision.

        Range: July 2017 to Present.
        Storage: data/bronze/crypto_binance/historical_monthly/{symbol}/
        """
        print(f"🏛️  Initiating Deep Historical Backfill for {len(self.pairs)} assets.")
        dest_dir: Path = self.base_path / "historical_monthly"

        years = ["2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]
        months = [f"{i:02d}" for i in range(1, 13)]

        for symbol in self.pairs:
            coin_dir = dest_dir / symbol.replace("USDT", "").lower()
            coin_dir.mkdir(parents=True, exist_ok=True)

            print(f"\nScanning archives for {symbol}.")
            for year in years:
                for month in months:
                    if year == "2017" and int(month) < 8:
                        continue

                    filename = f"{symbol}-{self.config['INTERVAL']}-{year}-{month}.zip"
                    url = f"{self.config['MONTHLY_URL']}/{symbol}/{self.config['INTERVAL']}/{filename}"
                    save_path = coin_dir / filename

                    # Hardening: Check existing file integrity
                    if save_path.exists():
                        if self._is_valid_zip(save_path):
                            continue
                        else:
                            print(f"  🗑️  Found corrupt/empty file: {filename}. Deleting.")
                            save_path.unlink()

                    # Download Logic
                    try:
                        print(f"  ⬇️  Downloading: {year}-{month}.", end="\r")
                        resp = requests.get(url)
                        if resp.status_code == 200:
                            with open(save_path, "wb") as f:
                                f.write(resp.content)
                            
                            # Post-Download Verification
                            if not self._is_valid_zip(save_path):
                                save_path.unlink()
                                print(f"\n  ❌ Integrity Check Failed (Deleted): {filename}")
                            else:
                                print(f"  ✅ Secured: {filename}       ", end="\r")
                    except Exception as error:
                        print(f"\n  ❌ Network Error: {error}")

    def ingest_recent(self):
        """
        Downloads daily 1-minute kline archives for the current incomplete month.

        Range: 1st of current month -> Yesterday.
        Storage: data/bronze/crypto_binance/recent_daily/{symbol}/
        """
        print("\n📅  Synchronizing Recent Daily Data.")
        dest_dir: Path = self.base_path / "recent_daily"

        today = datetime.now(timezone.utc)
        start_date = today.replace(day=1)
        end_date = today - timedelta(days=1)

        dates = []
        curr = start_date
        while curr <= end_date:
            dates.append(curr)
            curr += timedelta(days=1)

        for symbol in self.pairs:
            coin_dir = dest_dir / symbol.replace("USDT", "").lower()
            coin_dir.mkdir(parents=True, exist_ok=True)

            for d in dates:
                date_str = d.strftime("%Y-%m-%d")
                filename = f"{symbol}-{self.config['INTERVAL']}-{date_str}.zip"
                url = f"{self.config['DAILY_URL']}/{symbol}/{self.config['INTERVAL']}/{filename}"
                save_path = coin_dir / filename

                # Hardening using integrity check
                if save_path.exists():
                    if self._is_valid_zip(save_path):
                        continue
                    else:
                        print(f"  🗑️  Found corrupt/empty file: {filename}. Deleting.")
                        save_path.unlink()

                try:
                    print(f"  ⬇️  Fetching: {date_str}.", end="\r")
                    resp = requests.get(url)
                    if resp.status_code == 200:
                        with open(save_path, "wb") as f:
                            f.write(resp.content)

                        if not self._is_valid_zip(save_path):
                            save_path.unlink()
                    elif resp.status_code == 404:
                        print(f"  ⚠️  Pending: {date_str}        ", end="\r")
                except Exception as error:
                    print(f"\n  ❌ Error: {error}")

    def ingest_live(self):
        """
        Connects to the Binance WebSocket Stream to capture real-time market data.

        Output: Appends row-based CSV data to a local buffer file.
        Storage: data/bronze/crypto_binance/live_buffer/stream_buffer.csv
        """
        print("\n📡  Establishing Real-Time WebSocket Connection.")
        buffer_file: Path = self.base_path / "live_buffer" / "stream_buffer.csv"
        buffer_file.parent.mkdir(parents=True, exist_ok=True)

        def on_open(_ws):
            print("  🔌 Connected.")
            params = [f"{c.lower()}@kline_1m" for c in self.pairs]
            _ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))

        def on_message(_ws, message):
            data = json.loads(message)
            if 'k' in data and data['k']['x']: 
                k = data['k']
                row = f"{k['s']},{k['t']},{k['o']},{k['h']},{k['l']},{k['c']},{k['v']}\n"
                with open(buffer_file, "a") as f:
                    f.write(row)
                print(f"  💾 Captured: {k['s']} @ {k['c']}     ", end="\r")

        while True:
            try:
                ws = websocket.WebSocketApp(self.config['WS_URL'], on_open=on_open, on_message=on_message)
                ws.run_forever()
            except KeyboardInterrupt:
                print("\n🛑 Stream Terminated.")
                break
            except Exception:
                time.sleep(5)
