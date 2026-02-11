import functions_framework
import duckdb
import os
import requests
from google.cloud import storage
from datetime import datetime, timezone

# --- CONFIGURATION ---
SILVER_BUCKET_NAME = os.environ.get("SILVER_BUCKET_NAME")
GOLD_BUCKET_NAME = os.environ.get("GOLD_BUCKET_NAME")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# --- ANALYTICS CONSTANTS ---
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
    if not DISCORD_WEBHOOK_URL:
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
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
        print(f"🔔 Alert sent to Discord for {coin}")
    except Exception as error:
        print(f"❌ Failed to send alert: {error}")

@functions_framework.cloud_event
def process_analysis(cloud_event):
    """
    Orchestrates the Gold Layer: Financial Analysis & Signal Generation (Cloud Function).

    TRIGGER:
        Event-Driven. Fires automatically when a file is finalized in the Silver Bucket.

    WORKFLOW:
    1. Ingestion: 
       - Downloads the new 'cleaned_market_data.parquet' (Rich Schema) from Silver.
       - Downloads the existing 'analyzed_market_summary.parquet' (History) from Gold.
    2. State Management: 
       - Merges New Data + Historical Data into a single DuckDB table.
       - Preserves critical metrics (FDV, Volume, Supply, Rank) for deep analytics.
    3. Financial Modeling:
       - Calculates 7-Day Simple Moving Average (SMA).
       - Calculates 14-Day Relative Strength Index (RSI).
       - Generates Signals: BUY (Oversold), SELL (Overbought), WAIT.
    4. Alerting: 
       - Sends real-time Discord notifications for active BUY/SELL signals.
    5. Storage: 
       - Updates the 'analyzed_market_summary.parquet' state file in the Gold Bucket.
       - Prunes history to keep the dataset lightweight (last 500 records per coin).

    Args:
        cloud_event: The CloudEvent object containing the GCS file metadata.
    """
    data = cloud_event.data
    input_filename = data['name']
    print(f"🚀 Event {cloud_event['id']} triggered. Processing update: {input_filename}")

    # Setup temporary paths
    local_new_data = f"/tmp/{input_filename}"
    local_history = "/tmp/history.parquet"
    local_output = "/tmp/output.parquet"

    try:
        storage_client = storage.Client()

        # 1. Download data from Silver layer
        silver_bucket = storage_client.bucket(SILVER_BUCKET_NAME)
        silver_bucket.blob(input_filename).download_to_filename(local_new_data)

        # 2. Download history data
        gold_bucket = storage_client.bucket(GOLD_BUCKET_NAME)
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
        # I added FDV, Volume, Supply, Rank, Changes to match Silver Schema
        common_columns = """
            coin_id, symbol, name, current_price, market_cap, market_cap_rank,
            fully_diluted_valuation, total_volume, 
            high_24h, low_24h, price_change_percentage_24h,
            circulating_supply, total_supply, max_supply,
            ath, ath_change_percentage, ath_date,
            source_updated_at, ingested_file, processed_at
        """

        # Union Logic (State + New Data)
        if has_history:
            # I use distinct to prevent duplicates if the function runs twice
            con.execute(f"""
                CREATE TABLE raw_combined AS 
                SELECT {common_columns} FROM '{local_history}'
                UNION ALL 
                SELECT {common_columns} FROM '{local_new_data}'
            """)
        else:
            con.execute(f"CREATE TABLE raw_combined AS SELECT * FROM '{local_new_data}'")

        # 5. The Financial Query
        analysis_time = datetime.now(timezone.utc).isoformat()

        query = f"""
            WITH deduplicated_data AS (
                SELECT DISTINCT * FROM raw_combined
            ),
            price_changes AS (
                SELECT *,
                    current_price - LAG(current_price) OVER (PARTITION BY coin_id ORDER BY source_updated_at) as price_diff
                FROM deduplicated_data
            ),
            rolling_stats AS (
                SELECT *,
                    -- 7-Day SMA
                    AVG(current_price) OVER (PARTITION BY coin_id ORDER BY source_updated_at ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW) as sma_7d,

                    -- RSI Components
                    AVG(CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END) OVER (PARTITION BY coin_id ORDER BY source_updated_at ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW) as avg_gain,
                    AVG(CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END) OVER (PARTITION BY coin_id ORDER BY source_updated_at ROWS BETWEEN {RSI_PERIOD - 1} PRECEDING AND CURRENT ROW) as avg_loss
                FROM price_changes
            ),
            final_calculations AS (
                SELECT *,
                    CASE WHEN avg_loss = 0 THEN 100 ELSE 100 - (100 / (1 + (avg_gain / avg_loss))) END as rsi_14d
                FROM rolling_stats
            )

            SELECT 
                -- Passing through all rich metrics
                coin_id, symbol, name, current_price, market_cap, market_cap_rank,
                fully_diluted_valuation, total_volume,
                high_24h, low_24h, price_change_percentage_24h,
                circulating_supply, total_supply, max_supply,
                ath, ath_change_percentage, ath_date,

                -- Calculated Signals
                sma_7d, rsi_14d,
                CASE 
                    WHEN current_price < sma_7d AND rsi_14d < 30 THEN 'BUY'
                    WHEN current_price > sma_7d AND rsi_14d > 70 THEN 'SELL'
                    ELSE 'WAIT'
                END as signal,

                source_updated_at, ingested_file, processed_at,
                '{analysis_time}' as analyzed_at

            FROM final_calculations
            -- Keep only the last 500 records per coin to prevent file explosion
            QUALIFY ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY source_updated_at DESC) <= 500
            ORDER BY source_updated_at DESC, coin_id
        """

        con.execute(f"COPY ({query}) TO '{local_output}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')")

        # 6. Check alerts
        latest_row = con.execute(f"SELECT symbol, current_price, rsi_14d, signal FROM '{local_output}' ORDER BY source_updated_at DESC LIMIT 1").fetchone()

        if latest_row and latest_row[3] != "WAIT":
            # Only alert on BUY or SELL, not WAIT
            send_discord_alert(latest_row[0], latest_row[1], latest_row[2], latest_row[3])

        # 7. Save State
        gold_bucket.blob(STATE_FILENAME).upload_from_filename(local_output)
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
        if os.path.exists(local_history): os.remove(local_history)
        raise error
