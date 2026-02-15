import argparse
import requests
import os
import websocket
import json
import time
import zipfile  # Only needed to verify zip integrity, not to extract
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"

# URLS
BINANCE_MONTHLY_URL = "https://data.binance.vision/data/spot/monthly/klines"
BINANCE_DAILY_URL = "https://data.binance.vision/data/spot/daily/klines"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"

# MASTER COIN LIST
PAIRS = [
    # --- TIER 1: The Kings ---
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    # --- TIER 2: Utilities ---
    "XRPUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "LTCUSDT", "LINKUSDT",
    # --- TIER 3: Layer 2s ---
    "ARBUSDT", "MATICUSDT", "OPUSDT",
    # --- TIER 4: AI & Compute ---
    "FETUSDT", "RENDERUSDT", "TAOUSDT", "NEARUSDT",
    # --- TIER 5: Gaming ---
    "IMXUSDT", "GALAUSDT", "AXSUSDT",
    # --- TIER 6: Storage ---
    "FILUSDT", "ARUSDT",
    # --- TIER 7: DeFi & RWA ---
    "UNIUSDT", "AAVEUSDT", "ONDOUSDT",
    # --- TIER 8: New Gen ---
    "SUIUSDT", "SEIUSDT", "TIAUSDT",
    # --- TIER 9: Memes ---
    "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT"
]

INTERVAL = "1m"

def ingest_historical():
    """
    MODE 1: Downloads Monthly Zips (2017-2026)
    Saves to: data/bronze/historical_monthly/{coin}/
    """
    print("🏛️ STARTING HISTORICAL INGESTION (Monthly Zips).")
    dest_dir = BRONZE_DIR / "historical_monthly"

    # Define range
    years = ["2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]
    months = [f"{i:02d}" for i in range(1, 13)]

    for symbol in PAIRS:
        coin_dir = dest_dir / symbol.replace("USDT", "").lower()
        os.makedirs(coin_dir, exist_ok=True)

        print(f"\nChecking {symbol}.")
        for year in years:
            for month in months:
                # Binance started July 2017
                if year == "2017" and int(month) < 8:
                    continue

                filename = f"{symbol}-{INTERVAL}-{year}-{month}.zip"
                url = f"{BINANCE_MONTHLY_URL}/{symbol}/{INTERVAL}/{filename}"
                save_path = coin_dir / filename

                if save_path.exists():
                    print(f"  ⏩ Skipping {year}-{month} (Exists)", end="\r")
                    continue

                try:
                    print(f"  ⬇️ Downloading {year}-{month}.", end="\r")
                    resp = requests.get(url)
                    if resp.status_code == 200:
                        with open(save_path, "wb") as f:
                            f.write(resp.content)
                    elif resp.status_code == 404:
                        pass # Data not available yet
                    else:
                        print(f"\n  ❌ Failed {year}-{month}: {resp.status_code}")
                except Exception as error:
                    print(f"\n  ❌ Error {year}-{month}: {error}")

def ingest_recent():
    """
    MODE 2: Downloads Daily Zips (Current Month Gap-Fill)
    Saves to: data/bronze/recent_daily/{coin}/
    """
    print("\n📅 STARTING RECENT INGESTION (Daily Zips).")
    dest_dir = BRONZE_DIR / "recent_daily"

    # Calculate days: 1st of this month -> Yesterday
    today = datetime.now(timezone.utc)
    start_date = today.replace(day=1)
    end_date = today - timedelta(days=1)

    dates = []
    curr = start_date
    while curr <= end_date:
        dates.append(curr)
        curr += timedelta(days=1)

    print(f"Targeting range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    for symbol in PAIRS:
        coin_dir = dest_dir / symbol.replace("USDT", "").lower()
        os.makedirs(coin_dir, exist_ok=True)

        print(f"\nChecking {symbol}.")
        for d in dates:
            date_str = d.strftime("%Y-%m-%d")
            filename = f"{symbol}-{INTERVAL}-{date_str}.zip"
            url = f"{BINANCE_DAILY_URL}/{symbol}/{INTERVAL}/{filename}"
            save_path = coin_dir / filename

            if save_path.exists():
                continue

            try:
                print(f"  ⬇️ Fetching {date_str}...", end="\r")
                resp = requests.get(url)
                if resp.status_code == 200:
                    with open(save_path, "wb") as f:
                        f.write(resp.content)

                    # --- VERIFICATION STEP ---
                    if not zipfile.is_zipfile(save_path):
                        print(f"\n  ❌ CORRUPT ZIP: {date_str} (Deleting to retry later)")
                        os.remove(save_path)
                    else:
                        # Print a clean success message overwriting the "Fetching" line
                        print(f"  ✅ Verified {date_str}   ", end="\r")

                elif resp.status_code == 404:
                    # Often happens if Binance hasn't uploaded yesterday's file yet
                    print(f"  ⚠️ {date_str} not ready on server.", end="\r")
            except Exception as error:
                print(f"\n  ❌ Error {date_str}: {error}")

def ingest_live():
    """
    MODE 3: Live WebSocket Stream
    Saves to: data/bronze/live_buffer/stream_buffer.csv
    """
    print("\n📡 STARTING LIVE INGESTION (WebSocket).")
    buffer_file = BRONZE_DIR / "live_buffer" / "stream_buffer.csv"
    os.makedirs(buffer_file.parent, exist_ok=True)
    
    # Define WebSocket Logic locally since it's specific to this mode
    def on_open(ws):
        print("  🔌 Connected. Subscribing.")
        params = [f"{coin.lower()}@kline_1m" for coin in PAIRS]
        ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))

    def on_message(ws, message):
        data = json.loads(message)
        if 'k' in data and data['k']['x']: # 'x' means closed candle
            k = data['k']
            row = f"{k['s']},{k['t']},{k['o']},{k['h']},{k['l']},{k['c']},{k['v']}\n"

            # Append raw CSV data immediately to catch it all
            with open(buffer_file, "a") as f:
                f.write(row)

            print(f"  💾 Saved {k['s']} {k['c']}", end="\r")

    def run_stream():
        ws = websocket.WebSocketApp(BINANCE_WS_URL, on_open=on_open, on_message=on_message)
        ws.run_forever()

    while True:
        try:
            run_stream()
        except KeyboardInterrupt:
            print("\n🛑 Stopped.")
            break
        except Exception as error:
            print(f"\n⚠️ Error: {error}. Reconnecting.")
            time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Center Bronze Ingestion Engine")
    parser.add_argument("--mode", choices=["historical", "recent", "live"], required=True, help="Ingestion Mode")

    args = parser.parse_args()

    # Ensure Base Bronze Exists
    os.makedirs(BRONZE_DIR, exist_ok=True)

    if args.mode == "historical":
        ingest_historical()
    elif args.mode == "recent":
        ingest_recent()
    elif args.mode == "live":
        ingest_live()
