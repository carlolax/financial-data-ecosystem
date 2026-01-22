import functions_framework
from google.cloud import storage
import duckdb
import os
from pathlib import Path

# --- CONFIGURATION ---
BRONZE_BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME")
SILVER_BUCKET_NAME = os.environ.get("SILVER_BUCKET_NAME")

# --- HELPER FUNCTIONS ---
def validate_ingested_files(ingest_file_directory: Path, ingest_file_pattern: str = "raw_prices_*.json") -> str:
    # Verifies that source files actually exist in the local directory (or /tmp).

    # Use pathlib's glob
    ingest_files_found = list(ingest_file_directory.glob(ingest_file_pattern))
    ingest_file_count = len(ingest_files_found)

    if ingest_file_count == 0:
        raise FileNotFoundError(f"❌ No files found in {ingest_file_directory} matching '{ingest_file_pattern}'.")

    print(f"🔎 Found {ingest_file_count} files to process. Proceeding.")
    return str(ingest_file_directory / ingest_file_pattern)

def download_bronze_files(bucket_name: str, dest_dir: Path):
    # Downloads all JSON history from the Bronze Bucket to the local /tmp folder.
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    print(f"📥 Downloading JSON history from {bucket_name} to {dest_dir}.")
    
    # List all files starting with "raw_prices_".
    blobs = list(bucket.list_blobs(prefix="raw_prices_"))
    
    if not blobs:
        print("⚠️ No files found in Bronze bucket.")
        return

    for blob in blobs:
        destination = dest_dir / blob.name
        blob.download_to_filename(str(destination))

@functions_framework.cloud_event
def process_data_cleaning(cloud_event):
    # Triggered by a file upload to Bronze Bucket.
    print("🚀 Event triggered. Starting Silver Layer - Cloud Data Cleaning.")

    # Safety Checks
    if not BRONZE_BUCKET_NAME or not SILVER_BUCKET_NAME:
        print("❌ Error: Bucket environment variables missing.")
        return

    # Setup Temporary Directories
    TMP_BRONZE_DIR = Path("/tmp/bronze")
    TMP_SILVER_DIR = Path("/tmp/silver")
    
    # Clear previous run data (if container is reused) and create dirs.
    for folder in [TMP_BRONZE_DIR, TMP_SILVER_DIR]:
        if folder.exists():
            for f in folder.glob("*"):
                os.remove(f)
        folder.mkdir(parents=True, exist_ok=True)

    try:
        # Download History
        download_bronze_files(BRONZE_BUCKET_NAME, TMP_BRONZE_DIR)

        # Validation
        ingest_file_pattern = validate_ingested_files(TMP_BRONZE_DIR, "raw_prices_*.json")
        
        print(f"📂 Processing pattern: {ingest_file_pattern}")

        # DuckDB Logic
        query_to_clean = f"""
            SELECT DISTINCT
                id as coin_id,
                symbol,
                name,
                current_price,
                market_cap,
                market_cap_rank,

                -- SAFE FDV CALCULATION
                CASE 
                    WHEN max_supply IS NULL THEN (current_price * total_supply)
                    ELSE (current_price * max_supply)
                END as fully_diluted_valuation,

                total_volume,
                high_24h,
                low_24h,
                price_change_percentage_24h,
                circulating_supply,
                total_supply,
                max_supply,
                ath,
                ath_change_percentage,
                ath_date,
                last_updated as source_updated_at,
                current_timestamp as ingested_at

            FROM read_json_auto('{ingest_file_pattern}')
            ORDER BY source_updated_at DESC
        """

        # Define Output Path in /tmp.
        output_cleaned_file = TMP_SILVER_DIR / "cleaned_market_data.parquet"

        print("⚙️ Cleaning historical data with DuckDB.")

        duckdb.execute(f"""
            COPY ({query_to_clean}) 
            TO '{output_cleaned_file}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)
        
        print(f"✅ DuckDB Complete. Local file created at: {output_cleaned_file}")

        # Upload to Silver Bucket.
        storage_client = storage.Client()
        silver_bucket = storage_client.bucket(SILVER_BUCKET_NAME)
        
        output_blob_name = "cleaned_market_data.parquet"
        blob = silver_bucket.blob(output_blob_name)
        
        blob.upload_from_filename(str(output_cleaned_file))

        print(f"☁️ Uploaded to gs://{SILVER_BUCKET_NAME}/{output_blob_name}")

    except Exception as error:
        print(f"❌ Critical Error in Silver Cloud Function: {error}")
        # Log error and exit gracefully to prevent infinite retry loops in GCP.
