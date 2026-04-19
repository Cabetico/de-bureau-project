variable "gcp_project" {
  description = "GCP Project ID"
  type        = string
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  type        = string
}

variable "bucket_location" {
  description = "GCS Bucket Location"
  type        = string
  default     = "US-EAST1"
}

variable "environment" {
  description = "Environment label"
  type        = string
  default     = "dev"
}