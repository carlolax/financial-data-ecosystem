# ---------------------------------------------------------
# BRONZE LAYER: The Data Lake
# ---------------------------------------------------------

resource "google_storage_bucket" "data_lake" {
    name            = var.bucket_name
    location        = var.gcp_region
    force_destroy   = true # Allows us to delete the bucket even if it has files (Practice mode only)

    # FREE TIER OPTIMIZATION:
    # Standard storage class is cheap.
    storage_class   = "STANDARD"

    # Security: Block public access (No accidental leaks)
    public_access_prevention = "enforced"

    # Versioning: If we overwrite a file, keep the old one (Backup)
    versioning {
        enabled     = true
    }
}

# ---------------------------------------------------------
# SILVER LAYER: Clean Data
# ---------------------------------------------------------

resource "google_storage_bucket" "silver_layer" {
    name = "crypto-silver-${var.project_id}" # Will look like: crypto-silver-crypto-platform-carlo-2026
    location        = var.gcp_region
    storage_class   = "STANDARD"

    uniform_bucket_level_access = true
    public_access_prevention = "enforced"

    versioning {
        enabled     = true
    }

    labels = {
        environment = "dev"
        layer       = "silver"
    }
}
