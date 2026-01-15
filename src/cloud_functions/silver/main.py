import functions_framework
from google.cloud import storage
import duckdb
import os

# Setup config
SILVER_BUCKET_NAME = os.environ.get("SILVER_BUCKET_NAME", "crypto-silver-REPLACE-ME")

@functions_framework.cloud_event
def process_silver(cloud_event):
    data = cloud_event.data

    # Get event details
    file_name = data["name"]
    source_bucket_name = data["bucket"]
    
    print(f"Event triggered! Source: gs://{source_bucket_name}/{file_name}")

    # Safety Check: Ignore folders or non-json files
    if not file_name.endswith(".json"):
        print("Not a JSON file. Skipping.")
        return
    
    # Download (Input)
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    
    local_input_path = f"/tmp/{file_name}"
    # Change extension from .json to .parquet
    local_output_path = f"/tmp/{file_name.replace('.json', '.parquet')}"
    
    source_blob.download_to_filename(local_input_path)
    print(f"Downloaded to {local_input_path}")

    # Transform (DuckDB Logic)
    con = duckdb.connect()

    query = f"""
            COPY (
                WITH raw_data AS (
                    SELECT * FROM read_json('{local_input_path}',
                        columns={{
                            'bitcoin': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)',
                            'ethereum': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)',
                            'solana': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)'
                        }},
                        filename=True
                    )
                ),
                unpivoted_data AS (
                    UNPIVOT raw_data
                    ON bitcoin, ethereum, solana
                    INTO NAME coin_id VALUE metrics
                )
                SELECT
                    strptime(
                        regexp_extract(filename, 'raw_prices_(\\d{{8}}_\\d{{6}})', 1),
                        '%Y%m%d_%H%M%S'
                    ) as extraction_timestamp,  -- <--- from 'recorded_at', change it to 'extraction_timestamp' to match Gold Layer
                    coin_id,
                    CAST(metrics.usd AS DECIMAL(18, 2)) as price_usd,
                    CAST(metrics.usd_market_cap AS DECIMAL(24, 2)) as market_cap,
                    CAST(metrics.usd_24h_vol AS DECIMAL(24, 2)) as volume_24h
                FROM unpivoted_data
            ) TO '{local_output_path}' (FORMAT PARQUET);
        """

    try:
        con.execute(query)
        print(f"Transformation Complete. Saved to {local_output_path}")
    except Exception as error:
        print(f"DuckDB Error: {error}")
        if os.path.exists(local_input_path): os.remove(local_input_path)
        raise error

    # Upload (Output)
    dest_bucket = storage_client.bucket(SILVER_BUCKET_NAME)

    # Strip any existing folders from the input filename
    safe_filename = os.path.basename(file_name) 
    
    # Build the clean destination path
    dest_blob_name = f"processed/{safe_filename.replace('.json', '.parquet')}"
    
    dest_blob = dest_bucket.blob(dest_blob_name)
    
    dest_blob.upload_from_filename(local_output_path)
    print(f"Uploaded to gs://{SILVER_BUCKET_NAME}/{dest_blob_name}")

    # Cleanup /tmp to free up memory
    if os.path.exists(local_input_path): os.remove(local_input_path)
    if os.path.exists(local_output_path): os.remove(local_output_path)
    print("Cleanup complete.")
    