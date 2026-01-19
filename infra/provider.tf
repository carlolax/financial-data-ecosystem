provider "google" {
  project = var.project_id
  region  = var.region
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