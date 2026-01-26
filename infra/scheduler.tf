# ==========================================
# CLOUD SCHEDULER (The Automation)
# ==========================================

# 1. Create a dedicated Service Account for the Scheduler
# This identity represents the "Clock" that triggers the function.
resource "google_service_account" "scheduler_sa" {
  account_id   = "scheduler-sa"
  display_name = "Cloud Scheduler Service Account"
}

# 2. Grant Permission: Allow Scheduler SA to invoke the Bronze Function
resource "google_cloud_run_service_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  service  = google_cloudfunctions2_function.bronze_ingest_function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# 3. Define the Cron Job (Runs every hour)
resource "google_cloud_scheduler_job" "bronze_ingest_job" {
  name             = "job-bronze-ingest"
  description      = "Triggers Bronze Layer ingestion every hour"
  schedule         = "0 * * * *" # Every hour at minute 0
  time_zone        = "Etc/UTC"
  attempt_deadline = "320s"

  http_target {
    http_method = "GET"
    uri         = google_cloudfunctions2_function.bronze_ingest_function.service_config[0].uri

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}
