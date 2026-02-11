import functions_framework
import duckdb
import os
from google.cloud import storage

# --- CONFIGURATION ---
BRONZE_BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME")
SILVER_BUCKET_NAME = os.environ.get("SILVER_BUCKET_NAME")

@functions_framework.cloud_event
def process_cleaning(cloud_event):
    """
    Orchestrates the Silver Layer: Cleaning and Deduplication (Cloud Function).

    TRIGGER:
        Event-Driven. Fires automatically whenever a new file is uploaded to the Bronze Bucket.

    WORKFLOW:
    1. Input Parsing: Extracts filename from the Google Cloud Storage event.
    2. DuckDB Setup: Configures memory limits (512MB) for Cloud Run environment.
    3. Transformation (Schema Parity):
       - Loads raw JSON.
       - Applies the EXACT same SQL logic as the Local Pipeline (clean.py).
       - Calculates 'Safe FDV' (Fully Diluted Valuation) handling NULL Max Supply.
       - Preserves critical metrics (Volume, Rank, ATH) previously missing in Cloud V1.
    4. Storage: Saves as optimized Parquet (Snappy compression) and uploads to Silver Bucket.

    Args:
        cloud_event: The CloudEvent object containing the GCS file metadata.
    """
    data = cloud_event.data
    input_filename = data['name']

    print(f"🚀 Event {cloud_event['id']} triggered. Processing file: {input_filename}")

    # Define temporary local paths
    local_input = f"/tmp/{input_filename}"

    # Generate Output Name: raw_prices_X.json -> clean_prices_X.parquet
    output_filename = input_filename.replace("raw_", "clean_").replace(".json", ".parquet")
    local_output = f"/tmp/{output_filename}"

    try:
        # 1. Initialize GCS Client
        storage_client = storage.Client()

        # 2. Download JSON from Bronze
        source_bucket = storage_client.bucket(BRONZE_BUCKET_NAME)
        source_blob = source_bucket.blob(input_filename)

        print(f"📥 Downloading {input_filename} to {local_input}.")
        source_blob.download_to_filename(local_input)

        # 3. Configure DuckDB
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA memory_limit='512MB';")
        con.execute("PRAGMA threads=1;")

        # 4. Clean Data
        print("⚙️ Cleaning data with DuckDB.")

        query = f"""
            SELECT DISTINCT
                id as coin_id,
                symbol,
                name,
                current_price,
                market_cap,
                market_cap_rank,

                -- Safe FDV Calculation (Critical for Gold Layer)
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

                -- Timestamp & Lineage
                last_updated as source_updated_at,
                ingested_timestamp, 
                current_timestamp as processed_at

            FROM read_json_auto('{local_input}')
        """

        # 5. Save to Local Parquet
        con.execute(f"""
            COPY ({query}) 
            TO '{local_output}' 
            (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')
        """)

        print(f"✅ Data cleaned and saved locally to {local_output}")

        # 6. Upload to Silver
        print(f"📤 Uploading to {SILVER_BUCKET_NAME}.")
        dest_bucket = storage_client.bucket(SILVER_BUCKET_NAME)
        dest_blob = dest_bucket.blob(output_filename)

        dest_blob.upload_from_filename(local_output)

        print(f"✅ Silver Layer Success: Saved as {output_filename}")

        # Cleanup
        if os.path.exists(local_input): os.remove(local_input)
        if os.path.exists(local_output): os.remove(local_output)

    except Exception as error:
        print(f"❌ Critical Error in Silver Cloud Function: {error}")
        # Cleanup even on error
        if os.path.exists(local_input): os.remove(local_input)
        if os.path.exists(local_output): os.remove(local_output)
        raise error
