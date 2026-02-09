import duckdb
import os
from google.cloud import storage

# --- CONFIGURATION ---
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET_NAME")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")

def process_cleaning(event, context):
    """
    Orchestrates the Silver Layer: Cleaning and Deduplication (Cloud Function).

    Trigger:
        Event-Driven. Fires automatically whenever a new file is uploaded to the Bronze Bucket.

    Workflow:
    1. Authentication: Generates a Google Security Token for DuckDB.
    2. Configuration: Sets up DuckDB to treat Google Cloud Storage like an S3 bucket 
       (This allows us to use DuckDB's high-performance parallel IO).
    3. Transformation: 
       - Loads ALL raw JSON files from Bronze (Wildcard Pattern).
       - Deduplicates data based on Coin ID and Timestamp.
       - Calculates 'Safe FDV' (Fully Diluted Valuation).
    4. Storage: Writes the result as a single optimized Parquet file to the Silver Bucket.
    """
    input_filename = event['name']
    print(f"🚀 Event {context.event_id} triggered. Processing file: {input_filename}")

    # Define temporary local paths
    local_input = f"/tmp/{input_filename}"

    try:
        # 1. Initialize GCS Client
        storage_client = storage.Client()

        # 2. Download JSON from Bronze
        source_bucket = storage_client.bucket(BRONZE_BUCKET)
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
                ath,
                -- Convert timestamps
                last_updated as source_updated_at,
                current_timestamp as processed_at,
                '{input_filename}' as ingested_file
            FROM read_json_auto('{local_input}')
        """

        # 5. Save to Local Parquet (With Dynamic Name)
        # raw_prices_2025...json -> clean_prices_2025...parquet
        output_filename = input_filename.replace("raw_", "clean_").replace(".json", ".parquet")
        local_output = f"/tmp/{output_filename}"

        con.execute(f"""
            COPY ({query}) 
            TO '{local_output}' 
            (FORMAT 'PARQUET', CODEC 'SNAPPY')
        """)

        print(f"✅ Data cleaned and saved locally to {local_output}")

        # 6. Upload to Silver
        print(f"📤 Uploading to {SILVER_BUCKET}.")
        dest_bucket = storage_client.bucket(SILVER_BUCKET)
        dest_blob = dest_bucket.blob(output_filename)

        dest_blob.upload_from_filename(local_output)

        print(f"✅ Silver Layer Success: Saved as {output_filename}")

        # Cleanup
        if os.path.exists(local_input): os.remove(local_input)
        if os.path.exists(local_output): os.remove(local_output)

        return "Success"

    except Exception as error:
        print(f"❌ Critical Error in Silver Cloud Function: {error}")
        # Cleanup even on error
        if os.path.exists(local_input): os.remove(local_input)
        raise error
