from google.cloud import storage
import pandas as pd
import json
from datetime import datetime

# Setup config
PROJECT_ID = "crypto-platform-carlo-2026"
BRONZE_BUCKET = "crypto-lake-carlo-2026-v1" 
SILVER_BUCKET = f"crypto-silver-{PROJECT_ID}"

def get_latest_file(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix="raw_data/"))

    if not blobs:
        print("No files found in Bronze bucket.")
        return None

    latest_blob = sorted(blobs, key=lambda x: x.time_created)[-1]
    print(f"Found latest file: {latest_blob.name}")
    return latest_blob

def transform_data(json_data):
    rows = []
    for coin_name, stats in json_data.items():
        row = {
            "coin": coin_name,
            "price_usd": stats.get("usd"),
            "market_cap": stats.get("usd_market_cap"),
            "volume_24h": stats.get("usd_24h_vol"),
            "change_24h": stats.get("usd_24h_change"),
            "ingested_at": datetime.now()
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df

def save_to_silver(df, bucket_name):
    # Saves the DataFrame as a CSV directly to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Generate filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"processed_data/crypto_clean_{timestamp}.csv"

    blob = bucket.blob(filename)

    # Convert DataFrame to CSV string (in-memory)
    csv_data = df.to_csv(index=False)

    blob.upload_from_string(csv_data, content_type="text/csv")
    print(f"Clean data saved to: gs://{bucket_name}/{filename}")

if __name__ == "__main__":
    print("🚀 Starting Silver Layer Transformation.")
    
    # 1. Get the latest raw file
    blob = get_latest_file(BRONZE_BUCKET)
    
    if blob:
        # 2. Download and Parse JSON
        json_content = blob.download_as_string()
        data = json.loads(json_content)
        
        # 3. Transform
        clean_df = transform_data(data)
        print("📊 Data Preview:")
        print(clean_df.head())
        
        # 4. Load to Silver
        save_to_silver(clean_df, SILVER_BUCKET)
