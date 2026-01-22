import functions_framework
from google.cloud import storage
import duckdb
import os
from pathlib import Path

# --- CONFIGURATION ---
SILVER_BUCKET_NAME = os.environ.get("SILVER_BUCKET_NAME")
GOLD_BUCKET_NAME = os.environ.get("GOLD_BUCKET_NAME")
WINDOW_SIZE = 7

@functions_framework.cloud_event
def process_data_analyzing(cloud_event):
    # Event-Driven Cloud Function that recalculates market analytics.
    print("🚀 Event triggered. Starting Gold Layer - Cloud Data Analysis.")

    # Safety Checks
    if not SILVER_BUCKET_NAME or not GOLD_BUCKET_NAME:
        print("❌ Error: Bucket environment variables missing.")
        return

    # Setup Temporary Directories
    TMP_SILVER_DIR = Path("/tmp/silver")
    TMP_GOLD_DIR = Path("/tmp/gold")

    # Wipe and recreate to ensure clean state.
    for folder in [TMP_SILVER_DIR, TMP_GOLD_DIR]:
        if folder.exists():
            for f in folder.glob("*"):
                os.remove(f)
        folder.mkdir(parents=True, exist_ok=True)

    try:
        # Download Input File
        storage_client = storage.Client()
        silver_bucket = storage_client.bucket(SILVER_BUCKET_NAME)

        input_filename = "cleaned_market_data.parquet"
        input_blob = silver_bucket.blob(input_filename)

        local_input_path = TMP_SILVER_DIR / input_filename

        if not input_blob.exists():
            print(f"⚠️ {input_filename} not found in Silver bucket. Waiting for next run.")
            return

        print(f"📥 Downloading {input_filename} to {local_input_path}.")
        input_blob.download_to_filename(str(local_input_path))

        # Analyze
        query_to_analyze = f"""
            WITH moved_data AS (
                SELECT 
                    coin_id,
                    symbol,
                    name,
                    source_updated_at,
                    current_price,
                    market_cap,
                    ath,

                    -- Moving Average (7 Records ~ 7 Days)
                    AVG(current_price) OVER (
                        PARTITION BY coin_id 
                        ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as sma_7d,

                    -- Volatility (Standard Deviation)
                    STDDEV(current_price) OVER (
                        PARTITION BY coin_id 
                        ORDER BY source_updated_at 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as volatility_7d
                FROM '{local_input_path}'
            )
            SELECT 
                *,
                -- Signal Logic (Mean Reversion Strategy)
                CASE 
                    WHEN current_price < sma_7d AND volatility_7d > 0 THEN 'BUY'
                    WHEN current_price > sma_7d THEN 'SELL'
                    ELSE 'WAIT'
                END as signal,

                current_timestamp as analyzed_at
            FROM moved_data
            ORDER BY source_updated_at DESC, coin_id
        """

        # Define Output
        output_filename = "analyzed_market_data.parquet"
        local_output_path = TMP_GOLD_DIR / output_filename

        print("⚙️ Running financial models in DuckDB.")

        duckdb.execute(f"""
            COPY ({query_to_analyze}) 
            TO '{local_output_path}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        print(f"✅ Analysis Complete. Local file created at: {local_output_path}")

        # Upload to Gold Bucket.
        gold_bucket = storage_client.bucket(GOLD_BUCKET_NAME)
        output_blob = gold_bucket.blob(output_filename)

        print(f"📤 Uploading to gs://{GOLD_BUCKET_NAME}/{output_filename}.")
        output_blob.upload_from_filename(str(local_output_path))

        print(f"🚀 Success! Gold Layer updated.")

    except Exception as error:
        print(f"❌ Critical Error in Gold Layer: {error}.")
        # Log error and exit gracefully to prevent infinite retry loops in GCP.
