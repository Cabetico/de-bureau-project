#!/bin/bash
# dbt/scripts/seed_from_gcs.sh
set -e
 
echo "🔑 Activating service account..."
gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}" --project="${GCP_PROJECT}"
 
echo "📥 Downloading offices.csv from GCS..."
# Use gcloud storage instead of gsutil (Google's recommendation)
gcloud storage cp "${GCS_BUCKET_URI}/offices.csv" /dbt/seeds/offices.csv
 
echo "✅ File downloaded successfully"
ls -lh /dbt/seeds/offices.csv
 
echo "🌱 Running dbt seed..."
dbt seed --select offices
 
echo "🎉 Done!"
 