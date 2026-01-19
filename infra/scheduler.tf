resource "google_cloud_scheduler_job" "daily_bronze_trigger" {
  name        = "daily-crypto-ingest"
  description = "Triggers the Bronze Ingestion Function daily"
  schedule    = "0 6 * * *"
  time_zone   = "Australia/Brisbane"
  region      = var.region
  project     = var.project_id

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions_function.bronze_ingest.https_trigger_url

    oidc_token {
      service_account_email = google_service_account.function_runner.email
      audience              = google_cloudfunctions_function.bronze_ingest.https_trigger_url
    }
  }
}