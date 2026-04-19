#!/bin/bash
# run-terraform.sh
# Helper script to run Terraform with environment variables from .env
 
set -e
 
# Map environment variables to Terraform variables
export TF_VAR_gcp_project="${GCP_PROJECT}"
export TF_VAR_gcs_bucket_name="${GCP_PROJECT}-xmls-bucket"
export TF_VAR_bucket_location="${BUCKET_LOCATION:-US-EAST1}"
export TF_VAR_environment="${ENVIRONMENT:-dev}"
 
# Print what we're using
echo "🔧 Terraform Configuration:"
echo "  GCP Project:    $TF_VAR_gcp_project"
echo "  Bucket Name:    $TF_VAR_gcs_bucket_name"
echo "  Location:       $TF_VAR_bucket_location"
echo "  Environment:    $TF_VAR_environment"
echo ""
 
# Change to terraform directory (assuming script is in /app)
cd /app/terraform/gcs-bucket
 
# Run terraform with all arguments passed to this script
terraform "$@"