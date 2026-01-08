import duckdb
from google.cloud import storage
import os
import shutil

# Setup config
PROJECT_ID = "crypto-platform-carlo-2026"
SILVER_BUCKET = f"crypto-silver-{PROJECT_ID}"
GOLD_BUCKET = f"crypto-gold-{PROJECT_ID}"
TEMP_DIR = "data/temp_silver"

def process_gold_data():
    print("Starting transformation.")

    # 1. Setup Storage Client
    storage_client = storage.Client()
    silver_bucket = storage_client.bucket(SILVER_BUCKET)
    
    # 2. Download Silver Files Locally
    print(f"   Downloading files from gs://{SILVER_BUCKET}.")
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)

    blobs = list(silver_bucket.list_blobs(prefix="processed_data/"))
    downloaded_files = []

    if not blobs:
        print("No files found in Silver bucket!")
        return

    for blob in blobs:
        if blob.name.endswith(".csv"):
            filename = os.path.basename(blob.name)
            local_path = os.path.join(TEMP_DIR, filename)
            blob.download_to_filename(local_path)
            downloaded_files.append(local_path)
            print(f"   - Downloaded: {filename}")

    if not downloaded_files:
        print("No CSV files found to process.")
        return

    # 3. Initialize DuckDB
    conn = duckdb.connect(database=':memory:')
    
    # 4. Register Local Data
    print("   Loading data into DuckDB.")
    conn.execute(f"CREATE OR REPLACE VIEW silver_view AS SELECT * FROM read_csv_auto('{TEMP_DIR}/*.csv')")

    query = """
        SELECT 
            coin,
            ingested_at as timestamp,
            price_usd as current_price,
            AVG(price_usd) OVER (
                PARTITION BY coin 
                ORDER BY ingested_at
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS moving_avg_7d
        FROM silver_view
        ORDER BY coin, ingested_at
    """
    
    df_gold = conn.execute(query).df()

    print("Data Preview:")
    print(df_gold.head(10))

    # 5. Save and Upload Gold Data
    gold_bucket = storage_client.bucket(GOLD_BUCKET)
    local_output = "data/gold/gold_metrics.parquet"
    
    # Save locally
    df_gold.to_parquet(local_output)
    
    # Upload
    blob = gold_bucket.blob("metrics/gold_metrics.parquet")
    blob.upload_from_filename(local_output)
    
    print(f"Upload complete: gs://{GOLD_BUCKET}/metrics/gold_metrics.parquet")
    
    # 6. Cleanup
    shutil.rmtree(TEMP_DIR)
    print("Temp files cleaned up.")

if __name__ == "__main__":
    process_gold_data()