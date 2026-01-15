import functions_framework
from google.cloud import storage
import duckdb
import os
import glob
import shutil

# Setup config 
GOLD_BUCKET_NAME = os.environ.get("GOLD_BUCKET_NAME", "crypto-gold-REPLACE-ME")

@functions_framework.cloud_event
def process_gold(cloud_event):
    data = cloud_event.data
    source_bucket_name = data["bucket"]
    new_file_name = data["name"]

    print(f"Gold Triggered by: gs://{source_bucket_name}/{new_file_name}")

    # Define paths
    local_dir = "/tmp/silver_history"
    output_path = "/tmp/market_summary.parquet"
    
    # Remove directory if it exists to prevent mixing old data
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir)

    try:
        # Download history
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(source_bucket_name)

        blobs = list(source_bucket.list_blobs(prefix="processed/"))
        
        for blob in blobs:
            if blob.name.endswith(".parquet"):
                safe_name = blob.name.split("/")[-1]
                blob.download_to_filename(f"{local_dir}/{safe_name}")
        
        # Disk verification by using glob to physically check what landed on the disk
        downloaded_files = glob.glob(f"{local_dir}/*.parquet")
        file_count = len(downloaded_files)
        print(f"Disk Check: Successfully verified {file_count} parquet files on disk.")

        if file_count == 0:
            print("Warning: No Parquet files found. Aborting analysis.")
            return

        # Analyze
        con = duckdb.connect()

        query = f"""
        COPY (
            WITH base_metrics AS (
                SELECT
                    extraction_timestamp,
                    coin_id,
                    price_usd,

                    -- Calculate 7-Day Moving Average
                    AVG(price_usd) OVER (
                        PARTITION BY coin_id 
                        ORDER BY extraction_timestamp 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) as sma_7d,

                    -- Calculate Volatility
                    STDDEV(price_usd) OVER (
                        PARTITION BY coin_id 
                        ORDER BY extraction_timestamp 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) as volatility_7d

                FROM read_parquet('{local_dir}/*.parquet')
            )

            SELECT
                extraction_timestamp,
                coin_id,
                price_usd,
                CAST(sma_7d AS DECIMAL(18, 2)) as sma_7d,
                CAST(volatility_7d AS DECIMAL(18, 2)) as volatility_7d,

                CASE 
                    WHEN price_usd < sma_7d THEN 'BUY'
                    WHEN price_usd > sma_7d THEN 'WAIT'
                    ELSE 'HOLD'
                END as signal

            FROM base_metrics
            ORDER BY extraction_timestamp DESC, coin_id
        ) TO '{output_path}' (FORMAT PARQUET);
        """

        con.execute(query)
        print(f"Gold Analysis Complete. Saved to {output_path}")

        # Upload
        dest_bucket = storage_client.bucket(GOLD_BUCKET_NAME)
        dest_blob = dest_bucket.blob("analytics/market_summary.parquet")

        dest_blob.upload_from_filename(output_path)
        print(f"Published Dashboard Data: gs://{GOLD_BUCKET_NAME}/analytics/market_summary.parquet")

    except Exception as error:
        print(f"Critical Error in Gold Layer: {error}")
        raise error
        
    finally:
        # Cleanup to prevent "Out of Storage" errors on repeated runs
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
        if os.path.exists(output_path):
            os.remove(output_path)
        print("Local cleanup complete.")
        