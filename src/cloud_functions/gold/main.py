import duckdb
import os
import google.auth
from google.auth.transport.requests import Request
from datetime import datetime, timezone

# --- CONFIGURATION ---
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET_NAME")
WINDOW_SIZE = 7

def get_gcp_credentials() -> str:
    """
    Fetches the automatic Service Account credentials from the Cloud Function environment.

    Why:
    DuckDB needs an 'access_token' to read/write files in Google Cloud Storage.
    I generate this token dynamically using the function's identity (Service Account).
    """
    # I use '_' to ignore the project ID since I only need the credentials.
    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

def process_analysis(event, context):
    """
    Orchestrates the Gold Layer: Financial Analysis (Cloud Function).

    Trigger:
        Event-Driven. Fires automatically when 'cleaned_market_data.parquet' 
        is created in the Silver Bucket.

    Workflow:
    1. Authentication: Generates a Google Security Token for DuckDB.
    2. Configuration: Sets up DuckDB to treat Google Cloud Storage like an S3 bucket.
    3. Transformation: Calculates SMA (7-day) and Volatility; generates BUY/SELL signals.
    4. Storage: Writes the result directly to the Gold Bucket.
    """
    # This helps trace specific execution runs in Google Cloud Logs.
    print(f"🚀 Event {context.event_id} triggered by file: {event['name']} in bucket: {event['bucket']}")

    try:
        # 1. Authenticate with Google Cloud
        token = get_gcp_credentials()

        # 2. Configure DuckDB (In-Memory)
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Map GCS to the S3 API (Standard DuckDB pattern for GCP access)
        con.execute(f"SET s3_region='us-central1';")
        con.execute(f"SET s3_endpoint='storage.googleapis.com';")
        con.execute(f"SET s3_access_token='{token}';") 
        con.execute("SET s3_use_https=1;")

        # 3. Define Paths (Direct Cloud Access)
        source_path = f"s3://{SILVER_BUCKET}/cleaned_market_data.parquet"
        output_path = f"s3://{GOLD_BUCKET}/analyzed_market_summary.parquet"
        
        analysis_time = datetime.now(timezone.utc).isoformat()

        # 4. Define the Analytical Query
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
                    ingested_timestamp, 
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
                FROM '{source_path}'
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
                ingested_timestamp,
                processed_at,
                '{analysis_time}' as analyzed_at

            FROM metrics
            ORDER BY source_updated_at DESC, coin_id
        """

        print("⚙️ Executing DuckDB Financial Models in Cloud.")

        # 5. Execute & Save (Direct Write)
        con.execute(f"""
            COPY ({query}) 
            TO '{output_path}' 
            (FORMAT 'PARQUET', CODEC 'SNAPPY')
        """)

        print(f"✅ Gold Layer Complete. Saved to: {output_path}")
        return "Success"

    except Exception as error:
        print(f"❌ Critical Error in Gold Cloud Function: {error}")
        raise error
