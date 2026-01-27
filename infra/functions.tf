# ==========================================
# CLOUD FUNCTIONS INFRASTRUCTURE
# ==========================================

# -------------------------------------------------------------------------
# 🥉 BRONZE LAYER: Ingestion (Gen 2 Function)
# -------------------------------------------------------------------------

# Step 1: Zip the Code
# Terraform packages the Python source code from 'src/cloud_functions/bronze'
# into a deployable zip file.
data "archive_file" "bronze_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/bronze"
  output_path = "${path.module}/bronze_layer_function.zip"
}

# Step 2: Upload Zip to Cloud Storage
# The zip file is moved to the "Function Source" bucket.
# The MD5 hash in the filename ensures Terraform detects code changes and redeploys.
resource "google_storage_bucket_object" "bronze_zip_upload" {
  name   = "bronze_layer-${data.archive_file.bronze_layer_zip.output_md5}.zip"
  
  # LINKS TO: resource "google_storage_bucket" "function_source" in storage.tf
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.bronze_layer_zip.output_path
}

# Step 3: Deploy the Cloud Function (Gen 2)
# I use Gen 2 here because it's triggered via HTTP (Cloud Scheduler).
resource "google_cloudfunctions2_function" "bronze_ingest_function" {
  name        = "function-bronze-ingest"
  location    = var.region
  description = "Ingests raw crypto data from CoinGecko to Bronze Bucket"

  # Build Settings: Where the code comes from
  build_config {
    runtime     = "python310"
    entry_point = "process_ingestion" # MUST match the function name in main.py
    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.bronze_zip_upload.name
      }
    }
  }

  # Runtime Settings: Memory, Timeout, Identity
  service_config {
    max_instance_count = 1      # Limit concurrency to prevent API rate limits
    available_memory   = "256M" # Lightweight task, low memory needed
    timeout_seconds    = 60

    # Identity: LINKS TO: resource "google_service_account" "function_runner" in iam.tf
    service_account_email = google_service_account.function_runner.email

    # Environment Variables injected into the Python script
    environment_variables = {
      # LINKS TO: resource "google_storage_bucket" "data_lake" in storage.tf
      BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
      CRYPTO_COINS       = "bitcoin,ethereum,solana,cardano,ripple"
    }
  }
}

# Output the URL so Cloud Scheduler knows where to send the request
output "bronze_function_uri" {
  value = google_cloudfunctions2_function.bronze_ingest_function.service_config[0].uri
}

# -------------------------------------------------------------------------
# 🥈 SILVER LAYER: Cleaning (Gen 1 Function)
# -------------------------------------------------------------------------

# Step 1: Zip the Code
# Terraform takes your Python code from 'src/cloud_functions/silver' 
# and packages it into a standard zip file.
data "archive_file" "silver_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/silver"
  output_path = "${path.module}/silver_layer_function.zip"
}

# Step 2: Upload to Cloud Storage
# The zip file is uploaded to your "Function Source" bucket.
# The 'md5' hash ensures Terraform only uploads a new version if your code changes.
resource "google_storage_bucket_object" "silver_layer_zip_upload" {
  name   = "silver-cleaning-${data.archive_file.silver_layer_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.silver_layer_zip.output_path
}

# Step 3: Deploy the Cloud Function
# This function is "Event-Driven" (triggered by storage), so I use Gen 1.
resource "google_cloudfunctions_function" "silver_clean" {
  name        = "silver-cleaning-func"
  description = "Event-driven: Clean Bronze JSON to Silver Parquet"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  # Compute Power: 512MB is plenty for DuckDB processing
  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.silver_layer_zip_upload.name

  # CRITICAL UPDATE: Matches your Python function name
  entry_point = "process_cleaning"

  # Identity: Uses the same "Runner" service account as Bronze
  service_account_email = google_service_account.function_runner.email

  # TRIGGER: The "Domino Effect"
  # This tells Google: "Watch the Bronze bucket. If a NEW file is finalized (uploaded),
  # fire this function immediately."
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.data_lake.name
  }

  # Config: Passes bucket names to the Python script via OS Environment Variables
  environment_variables = {
    BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
  }
}

# -------------------------------------------------------------------------
# 🥇 GOLD LAYER: Analytics (Gen 1 Function)
# -------------------------------------------------------------------------

# Step 1: Zip the Code
# Packages the Python logic from 'src/cloud_functions/gold'
data "archive_file" "gold_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/gold"
  output_path = "${path.module}/gold_layer_function.zip"
}

# Step 2: Upload to Cloud Storage
# Moves the zip file to the system 'function_source' bucket
resource "google_storage_bucket_object" "gold_layer_zip_upload" {
  name   = "gold-analytics-${data.archive_file.gold_layer_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.gold_layer_zip.output_path
}

# Step 3: Deploy the Cloud Function
# Uses Gen 1 because I need the Storage Trigger ("watch a bucket") capability.
resource "google_cloudfunctions_function" "gold_analysis" {
  name        = "gold-analytics-func"
  description = "Event-driven: Calculate SMA & Signals from Silver Data"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.gold_layer_zip_upload.name

  # MUST match the function name in src/cloud_functions/gold/main.py
  entry_point = "process_analysis"

  # Identity: Uses the same "Runner" service account
  service_account_email = google_service_account.function_runner.email

  # TRIGGER: The Final Domino
  # Fires when a file is finalized (saved) in the SILVER bucket.
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.silver_layer.name
  }

  # Config: Injects bucket names so Python knows where to read/write
  environment_variables = {
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
    GOLD_BUCKET_NAME   = google_storage_bucket.gold_layer.name
  }
}
