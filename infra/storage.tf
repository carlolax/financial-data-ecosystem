# ==========================================
# STORAGE BUCKETS (The Data Hierarchy)
# ==========================================

# 1. SYSTEM BUCKET: Function Source Code
# ------------------------------------------------
# This bucket acts as a staging area. Terraform zips your Python code 
# and uploads it here before deploying it to Google Cloud Functions.
resource "google_storage_bucket" "function_source" {
  name     = "cdp-function-source"
  location = var.region

  # DEV SETTING: Allows Terraform to delete the bucket even if it contains files.
  # (Dangerous in Production, useful in Dev for easy teardowns).
  force_destroy = true

  # SECURITY: Enforces modern IAM permissions (no legacy ACLs).
  uniform_bucket_level_access = true
}

# 2. BRONZE LAYER: Raw Data Lake
# ------------------------------------------------
# Stores the raw JSON responses exactly as they come from CoinGecko.
# This is our "Source of Truth" if we ever need to re-process data.
resource "google_storage_bucket" "data_lake" {
  name          = var.bronze_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  # SECURITY: Blocks all public internet access to this bucket.
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true

  # SAFETY: Keeps previous versions of files if they are overwritten.
  versioning { enabled = true }

  # COST SAVING: The "Janitor".
  # Automatically deletes raw JSON files after 30 days since we 
  # likely only need the processed version in Silver/Gold.
  lifecycle_rule {
    condition {
      age = 30 # Days
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = "dev"
    layer       = "bronze"
  }
}

# 3. SILVER LAYER: Cleaned Data
# ------------------------------------------------
# Stores the data after it has been cleaned, deduplicated, and converted
# to Parquet format. This is the "Refined" data.
resource "google_storage_bucket" "silver_layer" {
  name          = var.silver_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  # SECURITY
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning { enabled = true }

  labels = {
    environment = "dev"
    layer       = "silver"
  }
}

# 4. GOLD LAYER: Analytics Data
# ------------------------------------------------
# Stores aggregated metrics (e.g., "Monthly Average Price").
# This data is ready for dashboards and visualization tools.
resource "google_storage_bucket" "gold_layer" {
  name          = var.gold_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  # SECURITY
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning { enabled = true }

  labels = {
    environment = "dev"
    layer       = "gold"
  }
}