terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("/secrets/credentials.json")
  project     = var.gcp_project
  region      = "us-east1"
}