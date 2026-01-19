# ==========================================
# STORAGE BUCKETS (The Layers)
# ==========================================

# --- FUNCTION SOURCE CODE BUCKET ---
resource "google_storage_bucket" "function_source" {
  name                        = "cdp-function-source"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# --- BRONZE LAYER (Raw Data) ---
resource "google_storage_bucket" "data_lake" {
  name                        = var.bronze_bucket_name
  location                    = var.region
  force_destroy               = true
  storage_class               = "STANDARD"
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true

  versioning { enabled = true }
}

# --- SILVER LAYER (Clean Data) ---
resource "google_storage_bucket" "silver_layer" {
  name                        = var.silver_bucket_name
  location                    = var.region
  force_destroy               = true
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning { enabled = true }

  labels = {
    environment = "dev"
    layer       = "silver"
  }
}

# --- GOLD LAYER (Analyze Data) ---
resource "google_storage_bucket" "gold_layer" {
  name                        = var.gold_bucket_name
  location                    = var.region
  force_destroy               = true
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning { enabled = true }

  labels = {
    environment = "dev"
    layer       = "gold"
  }
}