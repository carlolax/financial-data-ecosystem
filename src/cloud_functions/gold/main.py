import duckdb
import os
from google.cloud import storage
from datetime import datetime, timezone

# --- CONFIGURATION ---
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME", "cdp-silver-clean-bucket")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET_NAME", "cdp-gold-analyze-bucket")
WINDOW_SIZE = 7
RSI_PERIOD = 14

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
    print(f"🚀 Event {context.event_id} triggered. Processing file: {input_filename}")

    # setup temporary paths
    local_input = "/tmp/input.parquet"
    local_output = "/tmp/output.parquet"

    try:
        # 1. Initialize GCS Client
        storage_client = storage.Client()

        # 2. Download Data
        bucket = storage_client.bucket(SILVER_BUCKET)
        blob = bucket.blob(input_filename)

        if not blob.exists():
            print(f"⚠️ File not found in {SILVER_BUCKET}. Skipping.")
            return

        print(f"📥 Downloading gs://{SILVER_BUCKET}/{input_filename} to {local_input}.")
        blob.download_to_filename(local_input)

        # 3. Configure DuckDB
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA memory_limit='800MB';")
        con.execute("PRAGMA threads=1;")

        # 4. Define Logic
        analysis_time = datetime.now(timezone.utc).isoformat()

        query = f"""
            WITH price_changes AS (
                -- Step 1: Calculate Price Difference
                SELECT 
                    *,
                    current_price - LAG(current_price) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at
                    ) as price_diff
                FROM '{local_input}'
            ),

            rolling_stats AS (
                -- Step 2: Calculate SMA, Volatility, and RSI Components
                SELECT 
                    *,
                    -- 7-Day SMA
                    AVG(current_price) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as sma_7d,

                    -- Volatility
                    STDDEV(current_price) OVER (
                        PARTITION BY coin_id ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as volatility_7d,
                    
                    -- RSI Components (Avg Gain vs Avg Loss)
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
                -- Step 3: Compute final RSI value
                SELECT
                    coin_id, symbol, name, current_price, market_cap, ath, 
                    sma_7d, volatility_7d, source_updated_at, ingested_file, processed_at,

                    -- RSI Formula
                    CASE 
                        WHEN avg_loss = 0 THEN 100
                        ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
                    END as rsi_14d
                FROM rolling_stats
            )

            SELECT 
                coin_id, symbol, name, current_price, market_cap, ath, 
                sma_7d, volatility_7d, rsi_14d,

                -- Step 4: Generate Signal
                CASE 
                    WHEN current_price < sma_7d AND rsi_14d < 30 THEN 'BUY'
                    WHEN current_price > sma_7d AND rsi_14d > 70 THEN 'SELL'
                    ELSE 'WAIT'
                END as signal,

                source_updated_at,
                ingested_file,
                processed_at,
                '{analysis_time}' as analyzed_at

            FROM final_calculations
            ORDER BY source_updated_at DESC, coin_id
        """

        print("⚙️ Executing DuckDB Financial Models.")

        # 5. Execute to Local Temp File
        con.execute(f"""
            COPY ({query})
            TO '{local_output}'
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        # 6. Upload Result
        print(f"📤 Uploading results to {GOLD_BUCKET}.")
        out_bucket = storage_client.bucket(GOLD_BUCKET)
        out_blob = out_bucket.blob("analyzed_market_summary.parquet")
        out_blob.upload_from_filename(local_output)

        print("✅ Gold Layer Success. Pipeline Complete.")

        # Cleanup
        if os.path.exists(local_input): os.remove(local_input)
        if os.path.exists(local_output): os.remove(local_output)

        return "Success"

    except Exception as error:
        print(f"❌ Critical Error in Gold Cloud Function: {error}")
        raise error
