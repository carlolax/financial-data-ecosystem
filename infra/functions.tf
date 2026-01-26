# ==========================================
# CLOUD FUNCTIONS INFRASTRUCTURE
# ==========================================

# -------------------------------------------------------------------------
# 🥉 BRONZE LAYER: Ingestion (Gen 2 Function)
# -------------------------------------------------------------------------

# 1. Zip the Python Code
data "archive_file" "bronze_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/bronze"
  output_path = "${path.module}/bronze_layer_function.zip"
}

# 2. Upload Zip to the System Bucket
resource "google_storage_bucket_object" "bronze_zip_upload" {
  # MD5 hash ensures we only redeploy if code changes
  name = "bronze_layer-${data.archive_file.bronze_layer_zip.output_md5}.zip"

  # LINKS TO: resource "google_storage_bucket" "function_source" in storage.tf
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.bronze_layer_zip.output_path
}

# 3. Create the Cloud Function
resource "google_cloudfunctions2_function" "bronze_ingest_function" {
  name        = "function-bronze-ingest"
  location    = var.region
  description = "Ingests raw crypto data from CoinGecko to Bronze Bucket"

  build_config {
    runtime     = "python310"
    entry_point = "process_ingestion" # Matches main.py definition
    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.bronze_zip_upload.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "256M"
    timeout_seconds    = 60

    # LINKS TO: resource "google_service_account" "function_runner" in iam.tf
    service_account_email = google_service_account.function_runner.email

    environment_variables = {
      # LINKS TO: resource "google_storage_bucket" "data_lake" in storage.tf
      BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
      CRYPTO_COINS       = "bitcoin,ethereum,solana,cardano,ripple"
    }
  }
}

output "bronze_function_uri" {
  value = google_cloudfunctions2_function.bronze_ingest_function.service_config[0].uri
}

# -------------------------------------------------------------------------
# 🥈 SILVER LAYER: Cleaning (Gen 1 Function)
# -------------------------------------------------------------------------

data "archive_file" "silver_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/silver"
  output_path = "${path.module}/silver_layer_function.zip"
}

resource "google_storage_bucket_object" "silver_layer_zip_upload" {
  name   = "silver-cleaning-${data.archive_file.silver_layer_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.silver_layer_zip.output_path
}

resource "google_cloudfunctions_function" "silver_clean" {
  name        = "silver-cleaning-func"
  description = "Event-driven: Clean Bronze JSON to Silver Parquet"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.silver_layer_zip_upload.name

  entry_point = "process_data_cleaning"

  # LINKS TO: iam.tf
  service_account_email = google_service_account.function_runner.email

  # TRIGGER: Runs when a file is created in Bronze (data_lake)
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.data_lake.name
  }

  environment_variables = {
    BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
  }
}

# -------------------------------------------------------------------------
# 🥇 GOLD LAYER: Analysis (Gen 1 Function)
# -------------------------------------------------------------------------

data "archive_file" "gold_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/gold"
  output_path = "${path.module}/gold_layer_function.zip"
}

resource "google_storage_bucket_object" "gold_layer_zip_upload" {
  name   = "gold-analyzing-${data.archive_file.gold_layer_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.gold_layer_zip.output_path
}

resource "google_cloudfunctions_function" "gold_analyze" {
  name        = "gold-analyzing-func"
  description = "Event-driven: Analyze Market Signals"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.gold_layer_zip_upload.name

  entry_point = "process_data_analyzing"

  # LINKS TO: iam.tf
  service_account_email = google_service_account.function_runner.email

  # TRIGGER: Runs when a file is created in Silver (silver_layer)
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.silver_layer.name
  }

  environment_variables = {
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
    GOLD_BUCKET_NAME   = google_storage_bucket.gold_layer.name
  }
}
