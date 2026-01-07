variable "gcp_project" {
    description = "The ID of the Google Cloud Project"
    type        = string
}

variable "gcp_region" {
    description = "The region to deploy resources (e.g., us-central1)"
    type        = string
    default     = "us-central1" # Free tier friendly region
}

variable "bucket_name" {
    description = "The unique name of our Data Lake bucket"
    type        = string
}

variable "project_id" {
    description = "The GCP Project ID"
    type        = string
}