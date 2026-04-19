resource "google_storage_bucket" "xmls_bucket" {
  name          = var.gcs_bucket_name
  location      = var.bucket_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365  # Auto-delete objects older than 1 year (adjust as needed)
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}
