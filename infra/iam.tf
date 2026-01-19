# ==========================================
# SECURITY & IDENTITY
# ==========================================

resource "google_service_account" "function_runner" {
  account_id   = "crypto-runner-sa"
  display_name = "Crypto Ingestion Runner"
}

resource "google_project_iam_member" "runner_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.function_runner.email}"
}