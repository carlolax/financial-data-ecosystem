import duckdb
import os
from google.cloud import storage
from datetime import datetime, timezone

# --- CONFIGURATION ---
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET_NAME")
WINDOW_SIZE = 7

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
        blob = bucket.blob("cleaned_market_data.parquet")

        if not blob.exists():
            print(f"⚠️ File not found in {SILVER_BUCKET}. Skipping.")
            return

        print("📥 Downloading cleaned data to local disk.")
        blob.download_to_filename(local_input)

        # 3. Configure DuckDB
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA memory_limit='800MB';")
        con.execute("PRAGMA threads=1;")

        # 4. Define Logic
        analysis_time = datetime.now(timezone.utc).isoformat()
        
        query = f"""
            WITH metrics AS (
                SELECT 
                    coin_id,
                    symbol,
                    name,
                    current_price,
                    market_cap,
                    ath,
                    source_updated_at,
                    -- 🟢 FIX: Changed from 'ingested_timestamp' to 'ingested_file'
                    ingested_file, 
                    processed_at,

                    -- 7-Day Moving Average
                    AVG(current_price) OVER (
                        PARTITION BY coin_id 
                        ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as sma_7d,

                    -- Volatility
                    STDDEV(current_price) OVER (
                        PARTITION BY coin_id 
                        ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as volatility_7d
                FROM '{local_input}'
            )

            SELECT 
                coin_id,
                symbol,
                name,
                current_price,
                market_cap,
                ath,
                sma_7d,
                volatility_7d,

                -- Signal Strategy
                CASE 
                    WHEN current_price < sma_7d AND volatility_7d > 0 THEN 'BUY'
                    WHEN current_price > sma_7d THEN 'SELL'
                    ELSE 'WAIT'
                END as signal,

                source_updated_at,
                -- 🟢 FIX: Select the correct column here too
                ingested_file,
                processed_at,
                '{analysis_time}' as analyzed_at

            FROM metrics
            ORDER BY source_updated_at DESC, coin_id
        """

        print("⚙️ Executing DuckDB Financial Models (Local Mode).")

        # 5. Execute to Local File
        con.execute(f"""
            COPY ({query}) 
            TO '{local_output}' 
            (FORMAT 'PARQUET', CODEC 'SNAPPY')
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
