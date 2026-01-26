import duckdb
import os
import google.auth
from google.auth.transport.requests import Request
from datetime import datetime, timezone

# --- CONFIGURATION ---
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET_NAME")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")

def get_gcp_credentials() -> str:
    """
    Fetches the automatic Service Account credentials from the Cloud Function environment.

    Why:
    DuckDB needs an 'access_token' to read/write files in Google Cloud Storage.
    I generate this token dynamically using the function's identity (Service Account).
    """
    credentials, project = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

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
    print(f"🚀 Event triggered by file: {event['name']} in bucket: {event['bucket']}")

    try:
        # 1. Authenticate with Google Cloud
        token = get_gcp_credentials()

        # 2. Configure DuckDB for Cloud Storage
        con = duckdb.connect(database=":memory:")

        # Install the HTTPFS extension to allow reading remote files.
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Map GCS to the S3 API (This is a standard DuckDB pattern for GCP access)
        con.execute(f"SET s3_region='us-central1';")
        con.execute(f"SET s3_endpoint='storage.googleapis.com';")
        con.execute(f"SET s3_access_token='{token}';") 
        con.execute("SET s3_use_https=1;")

        # 3. Define Data Paths
        source_pattern = f"s3://{BRONZE_BUCKET}/raw_prices_*.json"
        output_path = f"s3://{SILVER_BUCKET}/cleaned_market_data.parquet"

        processing_time = datetime.now(timezone.utc).isoformat()

        # 4. Define the SQL Transformation
        # Identical to the local script: Deduplication + FDV Calculation
        query = f"""
            SELECT DISTINCT
                id as coin_id,
                symbol,
                name,
                current_price,
                market_cap,
                market_cap_rank,

                -- Safe FDV Logic (Handle infinite supply coins like ETH)
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
                ingested_timestamp,
                '{processing_time}' as processed_at

            FROM read_json_auto('{source_pattern}')
            ORDER BY source_updated_at DESC
        """

        print("⚙️ Executing DuckDB Transformation in Cloud.")

        # 5. Execute and Save
        con.execute(f"""
            COPY ({query}) 
            TO '{output_path}' 
            (FORMAT 'PARQUET', CODEC 'SNAPPY')
        """)

        print(f"✅ Silver Layer Complete. Saved to: {output_path}")
        return "Success"

    except Exception as error:
        print(f"❌ Critical Error in Silver Cloud Function: {error}")
        # Re-raising the error ensures Google Cloud marks this execution as 'Failed'
        # and logs it in the Error Reporting console.
        raise error
