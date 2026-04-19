output "bucket_name" {
  value       = google_storage_bucket.xmls_bucket.name
  description = "The name of the GCS bucket"
}

output "bucket_url" {
  value       = google_storage_bucket.xmls_bucket.url
  description = "The URL of the GCS bucket"
}

output "bucket_self_link" {
  value       = google_storage_bucket.xmls_bucket.self_link
  description = "The URI of the GCS bucket"
}
