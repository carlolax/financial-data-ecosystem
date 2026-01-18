variable "project_id" {
  description = "The Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}

variable "bronze_bucket_name" {
  description = "Name of the Bronze bucket"
  type        = string
}

variable "silver_bucket_name" {
  description = "Name of the Silver bucket"
  type        = string
}

variable "gold_bucket_name" {
  description = "Name of the Gold bucket"
  type        = string
}