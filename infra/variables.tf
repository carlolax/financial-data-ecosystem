variable "project_id" {
  description = "The ID of the Google Cloud Project"
  type        = string
}

variable "region" {
  description = "The Google Cloud region (e.g., australia-southeast1)"
  type        = string
  default     = "australia-southeast1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}