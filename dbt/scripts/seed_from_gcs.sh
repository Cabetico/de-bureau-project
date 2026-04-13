# dbt/scripts/seed_from_gcs.sh
#!/bin/bash
set -e

echo "🔑 Activating service account..."
gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}"

echo "📥 Downloading offices.csv from GCS..."
gsutil cp "${GCS_BUCKET_URI}/offices.csv" /dbt/seeds/offices.csv  ## need to download the offices.csv from GCS to the dbt/seeds directory

echo "🌱 Running dbt seed..."
dbt seed --select offices