#!/bin/bash
# destroy-bq-datasets.sh
# ☢️ NUCLEAR OPTION - Destroys ALL BigQuery datasets
# USE WITH CAUTION!

set -e

PROJECT_ID="${GCP_PROJECT:-dtc-de-340821}"

# Activate service account if credentials file exists
if [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "🔐 Activating service account..."
    gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" --project="$PROJECT_ID" 2>/dev/null || true
fi

# Datasets to destroy
DATASETS=("raw" "staging" "intermediate" "marts" "seeds")

echo "☢️  NUCLEAR OPTION - DESTROY ALL DATASETS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Project: $PROJECT_ID"
echo ""
echo "This will DELETE the following datasets and ALL their tables:"
for DATASET in "${DATASETS[@]}"; do
    echo "  ❌ $DATASET"
done
echo ""
echo "⚠️  WARNING: THIS CANNOT BE UNDONE!"
echo "⚠️  ALL DATA WILL BE PERMANENTLY DELETED!"
echo ""
read -p "Type 'DESTROY' to confirm: " CONFIRM

if [ "$CONFIRM" != "DESTROY" ]; then
    echo "❌ Aborted. No datasets were deleted."
    exit 1
fi

echo ""
echo "🔥 Starting destruction sequence..."
echo ""

for DATASET in "${DATASETS[@]}"; do
    echo "Destroying dataset: $DATASET"
    
    # Check if dataset exists using bq show
    if bq show --project_id="$PROJECT_ID" "$PROJECT_ID:$DATASET" &>/dev/null; then
        # Delete the dataset and all tables
        bq rm -r -f -d "$PROJECT_ID:$DATASET"
        echo "  ✅ Dataset $DATASET destroyed"
    else
        echo "  ℹ️  Dataset $DATASET doesn't exist, skipping..."
    fi
    echo ""
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 All datasets destroyed!"
echo ""
echo "💡 Tip: Run 'dbt run' to recreate them"