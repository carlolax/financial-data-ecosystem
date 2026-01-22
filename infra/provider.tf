provider "google" {
  project = var.project_id
  region  = var.region

  user_project_override = true
  billing_project       = "crypto-data-platform"
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.51.0"
    }
  }

  backend "gcs" {
    bucket = "cdp-terraform-status"
    prefix = "terraform/status"
  }
}