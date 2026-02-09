import duckdb
import os
import requests
from google.cloud import storage
from datetime import datetime, timezone

# --- CONFIGURATION ---
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME", "cdp-silver-clean-bucket")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET_NAME", "cdp-gold-analyze-bucket")
DISCORD_URL = os.environ.get("DISCORD_WEBHOOK_URL") 
WINDOW_SIZE = 7
RSI_PERIOD = 14
STATE_FILENAME = "analyzed_market_summary.parquet"

def send_discord_alert(coin, price, rsi, signal):
    """
    Sends a formatted alert payload to a configured Discord Webhook.

    This function constructs a JSON payload containing a rich embed with 
    market data (Price, RSI, Time) and sends it via a POST request to 
    the Discord API. It uses color coding (Green for BUY, Red for SELL) 
    to visually distinguish signals.

    Args:
        coin (str): The symbol or name of the cryptocurrency (e.g., "BTC", "bitcoin").
        price (float): The current market price of the asset.
        rsi (float): The calculated Relative Strength Index (14-day).
        signal (str): The trading signal triggering the alert (e.g., "BUY", "SELL").

    Returns:
        None: This function attempts to send a request but does not return a value. 
              Success or failure is logged to stdout.
    """
    if not DISCORD_URL:
        print("⚠️ No Discord URL set. Skipping alert.")
        return

    # Pick a color (🟩 Green for BUY, 🟥 Red for SELL)
    color = 5763719 if signal == "BUY" else 15548997

    payload = {
        "username": "Crypto Alert Bot 🤖",
        "embeds": [{
            "title": f"🚨 {signal} SIGNAL DETECTED: {coin}",
            "color": color,
            "fields": [
                {"name": "Price", "value": f"${price:,.4f}", "inline": True},
                {"name": "RSI (14d)", "value": f"{rsi:.1f}", "inline": True},
                {"name": "Time", "value": datetime.now().strftime("%Y-%m-%d %H:%M UTC"), "inline": False}
            ]
        }]
    }

    try:
        requests.post(DISCORD_URL, json=payload, timeout=5)
        print(f"🔔 Alert sent to Discord for {coin}")
    except Exception as error:
        print(f"❌ Failed to send alert: {error}")

def process_analysis(event, context):
    """
    Orchestrates the Gold Layer: Financial Analysis (Stable / Local Mode).

    Trigger:
        Event-Driven. Fires automatically when a file is finalized in the Silver Bucket.

    Workflow:
    1. Ingestion: Downloads the 'cleaned_market_data.parquet' from GCS to local disk (/tmp).
       - Note: We use the native Google Cloud Storage client instead of DuckDB's 'httpfs' 
         extension to avoid C++ threading/memory conflicts in the Cloud Function environment.
    2. Configuration: 
       - Limits DuckDB memory to 800MB (leaving room for Python overhead).
       - Restricts execution to 1 thread to prevent concurrency crashes.
    3. Transformation: 
       - Calculates 7-Day SMA and Volatility using Window Functions.
       - Generates BUY/SELL/WAIT signals based on Mean Reversion strategy.
    4. Storage: Writes results to a local file, then uploads to the Gold Bucket.
    """
    input_filename = event['name']
    print(f"🚀 Event {context.event_id} triggered. Processing update: {input_filename}")

    # Setup temporary paths
    local_new_data = "/tmp/new_data.parquet"
    local_history = "/tmp/history.parquet"
    local_output = "/tmp/output.parquet"

    try:
        storage_client = storage.Client()

        # 1. Download data from Silver layer
        silver_bucket = storage_client.bucket(SILVER_BUCKET)
        new_blob = silver_bucket.blob(input_filename)
        new_blob.download_to_filename(local_new_data)
        print(f"📥 Downloaded New Data: {input_filename}")

        # 2. Download history data
        gold_bucket = storage_client.bucket(GOLD_BUCKET)
        history_blob = gold_bucket.blob(STATE_FILENAME)

        has_history = False
        if history_blob.exists():
            print(f"📥 Downloading History: {STATE_FILENAME}")
            history_blob.download_to_filename(local_history)
            has_history = True
        else:
            print("⚠️ No history found. Starting fresh state.")

        # 3. Configure DuckDB
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA memory_limit='800MB';")

        # 4. Define Table Loading Logic
        # If I have history, I will UNION them. If not, just use new data.
        if has_history:
            con.execute(f"""
                CREATE TABLE raw_combined AS 
                SELECT * FROM '{local_history}'
                UNION ALL 
                SELECT * FROM '{local_new_data}'
            """)
        else:
            con.execute(f"CREATE TABLE raw_combined AS SELECT * FROM '{local_new_data}'")

        # 5. The Financial Query
        analysis_time = datetime.now(timezone.utc).isoformat()

        query = f"""
            WITH deduplicated_data AS (
                -- Safety: Ensure no duplicate rows if we re-process a file
                SELECT DISTINCT * FROM raw_combined
            ),

            price_changes AS (
                SELECT 
                    *,
                    current_price - LAG(current_price) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at
                    ) as price_diff
                FROM deduplicated_data
            ),

            rolling_stats AS (
                SELECT 
                    *,
                    AVG(current_price) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as sma_7d,

                    AVG(CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at
                        ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW
                    ) as avg_gain,

                    AVG(CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at
                        ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW
                    ) as avg_loss
                FROM price_changes
            ),

            final_calculations AS (
                SELECT
                    *,
                    CASE 
                        WHEN avg_loss = 0 THEN 100
                        ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
                    END as rsi_14d
                FROM rolling_stats
            )

            SELECT 
                coin_id, symbol, name, current_price, market_cap, ath, 
                sma_7d, rsi_14d,

                -- Generate Signal
                CASE 
                    WHEN current_price < sma_7d AND rsi_14d < 30 THEN 'BUY'
                    WHEN current_price > sma_7d AND rsi_14d > 70 THEN 'SELL'
                    ELSE 'WAIT'
                END as signal,

                source_updated_at, ingested_file, processed_at,
                '{analysis_time}' as analyzed_at

            FROM final_calculations
            -- Optimization: Keep only the last 30 days of history to prevent unlimited growth
            -- (Assuming roughly 144 records per day per coin, 30 days ~ 4500 rows/coin)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY source_updated_at DESC) <= 500
            ORDER BY source_updated_at DESC, coin_id
        """

        print("⚙️ Executing DuckDB Financial Models.")

        con.execute(f"""
            COPY ({query})
            TO '{local_output}'
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        # 6. Check alerts
        print("🔎 Checking for new BUY signals.")

        # Get the timestamp of the new file I just ingested to verify freshness.
        alert_query = """
            SELECT symbol, current_price, rsi_14d, signal 
            FROM raw_combined 
            ORDER BY source_updated_at DESC 
            LIMIT 1
        """
        latest_row = con.execute(alert_query).fetchone()

        if latest_row:
            symbol, price, rsi, signal = latest_row
            print(f"Info: Latest Signal for {symbol} is {signal} (RSI: {rsi})")

            # Logic: Alert if BUY
            if signal == "BUY":
                send_discord_alert(symbol, price, rsi, signal)

        # 7. Save State
        print(f"📤 Updating Gold State: {STATE_FILENAME}")
        out_blob = gold_bucket.blob(STATE_FILENAME)
        out_blob.upload_from_filename(local_output)

        print("✅ Gold Layer Success. State Updated.")

        # Cleanup
        if os.path.exists(local_new_data): os.remove(local_new_data)
        if os.path.exists(local_history): os.remove(local_history)
        if os.path.exists(local_output): os.remove(local_output)

        return "Success"

    except Exception as error:
        print(f"❌ Critical Error in Gold Cloud Function: {error}")
        # Cleanup
        if os.path.exists(local_new_data): os.remove(local_new_data)
        raise error
