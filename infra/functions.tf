# ==========================================
# CLOUD FUNCTIONS INFRASTRUCTURE
# ==========================================

# --- BRONZE LAYER (Ingestion) ---

data "archive_file" "bronze_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/bronze"
  output_path = "${path.module}/bronze_layer_function.zip"
}

resource "google_storage_bucket_object" "bronze_layer_zip_upload" {
  name   = "bronze-ingesting-${data.archive_file.bronze_layer_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.bronze_layer_zip.output_path
}

resource "google_cloudfunctions_function" "bronze_ingest" {
  name        = "bronze-ingesting-func"
  description = "Ingests Crypto Data to Bronze Bucket"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.bronze_layer_zip_upload.name
  trigger_http          = true
  entry_point           = "process_ingest_data"

  service_account_email = google_service_account.function_runner.email

  environment_variables = {
    BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
  }
}

# --- SILVER LAYER (Cleaning) ---

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

  entry_point           = "process_data_cleaning"
  service_account_email = google_service_account.function_runner.email

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.data_lake.name
  }

  environment_variables = {
    BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
  }
}

# --- GOLD LAYER (Analysis) ---

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

  entry_point           = "process_data_analyzing"
  service_account_email = google_service_account.function_runner.email

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.silver_layer.name
  }

  environment_variables = {
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
    GOLD_BUCKET_NAME   = google_storage_bucket.gold_layer.name
  }
}
