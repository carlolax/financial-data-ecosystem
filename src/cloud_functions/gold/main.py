import functions_framework
from google.cloud import storage
import duckdb
import os
import shutil
from pathlib import Path

# --- CONFIGURATION ---
GOLD_BUCKET_NAME = os.environ.get("GOLD_BUCKET_NAME", "crypto-gold-data")
WINDOW_SIZE = 7

@functions_framework.cloud_event
def process_data_analyzing(cloud_event):
    """
    Event-Driven Cloud Function that recalculates market analytics.

    Trigger:
        Google Cloud Storage (Object Finalize) on the Silver Bucket.

    Process:
        1. Downloads ALL historical Parquet files from Silver.
        2. Aggregates them using DuckDB.
        3. Calculates Moving Averages (SMA) and Volatility.
        4. Generates BUY/SELL signals.
        5. Publishes a single 'market_summary.parquet' to the Gold Bucket.
    """
    data = cloud_event.data
    source_bucket_name = data["bucket"]

    print("🚀 Event triggered! Starting Gold Layer - Data Analysis")
    print(f"Source: gs://{source_bucket_name}/{data['name']}")

    # 1. Setup Paths
    temp_root = Path("/tmp")
    history_dir = temp_root / "silver_history"
    output_file = temp_root / "market_summary.parquet"
    
    # Clean up old run data as a fresh start
    if history_dir.exists():
        shutil.rmtree(history_dir)
    history_dir.mkdir(parents=True)

    try:
        # 2. Download History
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(source_bucket_name)

        # List all processed files from Silver Layer
        blobs = list(source_bucket.list_blobs(prefix="processed/"))

        download_count = 0
        for blob in blobs:
            if blob.name.endswith(".parquet"):
                # Using Path to get the filename
                safe_name = Path(blob.name).name
                destination = history_dir / safe_name
                blob.download_to_filename(str(destination))
                download_count += 1

        print(f"✅ Downloaded {download_count} files for historical analysis.")

        if download_count == 0:
            print("⚠️ No history found. Aborting analysis.")
            return

        # 3. Analyze (DuckDB)
        duckdb_con = duckdb.connect(database=':memory:')

        # SQL Query
        query = f"""
        COPY (
            WITH base_metrics AS (
                SELECT
                    extraction_timestamp,
                    coin_id,
                    price_usd,

                    -- Calculate {WINDOW_SIZE}-Day Moving Average
                    AVG(price_usd) OVER (
                        PARTITION BY coin_id 
                        ORDER BY extraction_timestamp 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as sma_7d,

                    -- Calculate Volatility
                    STDDEV(price_usd) OVER (
                        PARTITION BY coin_id 
                        ORDER BY extraction_timestamp 
                        ROWS BETWEEN {WINDOW_SIZE - 1} PRECEDING AND CURRENT ROW
                    ) as volatility_7d

                FROM read_parquet('{history_dir}/*.parquet')
            )

            SELECT
                extraction_timestamp,
                coin_id,
                price_usd,
                CAST(sma_7d AS DECIMAL(18, 2)) as sma_7d,
                CAST(volatility_7d AS DECIMAL(18, 2)) as volatility_7d,

                -- Logic aligned with Local Pipeline
                CASE 
                    WHEN price_usd < sma_7d AND volatility_7d > 0 THEN 'BUY'
                    WHEN price_usd > sma_7d THEN 'SELL'
                    ELSE 'WAIT'
                END as signal

            FROM base_metrics
            ORDER BY extraction_timestamp DESC, coin_id
        ) TO '{output_file}' (FORMAT PARQUET);
        """

        duckdb_con.execute(query)
        print(f"📊 Analysis Complete. Saved to {output_file}")

        # 4. Publish to Gold
        dest_bucket = storage_client.bucket(GOLD_BUCKET_NAME)
        # Overwrite the single file to allow dashboard view the latest state
        dest_blob = dest_bucket.blob("analytics/market_summary.parquet")

        dest_blob.upload_from_filename(str(output_file))
        print(f"🚀 Published dashboard data: gs://{GOLD_BUCKET_NAME}/analytics/market_summary.parquet")

    except Exception as error:
        print(f"❌ Critical Error in Gold Layer: {error}")
        # Re-raise the error to stop the pipeline
        raise error

    finally:
        # 5. Cleanup
        if history_dir.exists():
            shutil.rmtree(history_dir)
        if output_file.exists():
            output_file.unlink()
        print("🧹 Local cleanup complete.")
        duckdb_con.close()
