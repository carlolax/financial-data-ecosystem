variable "bronze_bucket_name" {
  type        = string
  description = "Name of the Bronze bucket"
  default     = "cdp-bronze-ingest-bucket"
}

variable "silver_bucket_name" {
  type        = string
  description = "Name of the Silver bucket"
  default     = "cdp-silver-clean-bucket"
}

variable "gold_bucket_name" {
  type        = string
  description = "Name of the Gold bucket"
  default     = "cdp-gold-analyze-bucket"
}

variable "project_id" {
  description = "The GCP Project ID"
  default     = "crypto-data-platform"
}

variable "region" {
  description = "GCP Region"
  default     = "us-central1"
}

variable "billing_account_id" {
  description = "The ID of the billing account to associate this project with"
  type        = string
}
