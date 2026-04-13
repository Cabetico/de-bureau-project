# de-bureau-project
project attempt for data-engineer-zoomcamp


## 1. Terraform binary instalation

```bash
    # Download Terraform
    wget https://releases.hashicorp.com/terraform/1.11.3/terraform_1.11.3_linux_amd64.zip

    # Install unzip if needed
    sudo apt-get install -y unzip

    # Unzip it
    unzip terraform_1.11.3_linux_amd64.zip

    # Move to system path
    sudo mv terraform /usr/local/bin/

    # Verify
    terraform -v
```

#### Verify instalation

```bash
    terraform -v
```


## 2. Store your GCP Credentials
Save your services account JSON to a safe location ins WSL: 

```bash
    mkdir -p ~/.gcp
    nano ~/.gcp/credentials.json
# Paste your JSON credentials and save (Ctrl+X, Y, Enter)

   # Lock down permissions
   chmod 600 ~/.gcp/credentials.json
```

## 3. Set Up Your Terraform Project

```bash
    mkdir -p ~/terraform/gcs-bucket
    cd ~/terraform/gcs-bucket
```

Inside of gcs-bucket folder create the following files:
`provider.tf` — tells Terraform to use GCP with your credentials:

```bash
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("~/.gcp/credentials.json")
  project     = "dtc-de-340821"
  region      = "us-east1"
}
```

`main.tf` — defines the GCS bucket:
```bash
  resource "google_storage_bucket" "xmls_bucket" {
  name          = "dtc-de-340821-xmls-bucket"   # Must be globally unique
  location      = "US-EAST1"
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
    environment = "dev"
    managed_by  = "terraform"
  }
} 
```

`outputs.tf` — prints useful info after apply:

```bash
output "bucket_name" {
  value = google_storage_bucket.xmls_bucket.name
 }

output "bucket_url" {
  value = google_storage_bucket.xmls_bucket.url
 }
```
## 4. Test Your Credentials & Configuration

```bash
    # Export credentials as env var (alternative to file reference, good for testing)
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/credentials.json

# Initialize Terraform (downloads the GCP provider)
terraform init


terraform validate
# Expected: Success! The configuration is valid.


terraform plan
#This will show you exactly what resources Terraform intends to create — #no changes are made yet.
```

* Destroy the bucket

```bash
    terraform destroy
```

A couple of important warnings before you destroy:

🪣 If the bucket has files inside it, GCP will block the deletion by default. You'd need to add this to your main.tf first:

```bash
   force_destroy = true 
```

* 💾 `terraform destroy` removes the real GCP resource — the bucket and all its contents are gone permanently
* 📄 Your local `.tf` files are not affected — so you can always recreate the bucket later with `terraform apply`


## 5. Data Producer Script 

For this project we will generate the data with a script because this type of data is impossible to acquire for confidenciality reasons


* from docker container
```bash
python producer_cc.py \
  --archivos 10000 \
  --applications applications.json \
  --offices offices.csv \
  --zip bureau.zip \
  --gcs-bucket dtc-de-340821-xmls-bucket \
  --gcs-credentials /secrets/credentials.json
```


```bash
python producer_cc.py \
  --archivos 10000 \
  --applications applications.json \
  --offices offices.csv \
  --zip bureau.zip \
  --gcs-bucket $GCS_BUCKET_NAME \
  --gcs-credentials $SECRETS_PATH
```
## 6. DLT to create BIG Query tables


Three things worth understanding about the design:

*ZIP streaming without local download*  — `gcsfs.open()` returns a file-like object that reads from GCS on demand. Python's `zipfile.ZipFile` accepts any file-like, so it reads the ZIP's central directory first (at the end of the file), then seeks to each XML member and reads only its bytes. The whole ZIP never lands on disk.

Why `@dlt.resource` instead of `@dlt.transformer` — the previous version piped filesystem() | parse_circulo_xml(). Here the ZIP is a single GCS object, not a folder of files, so there's nothing to glob. The resource handles both listing (iterating ZIP members) and reading in one step.

*BigQuery dataset name* — dlt creates the buro dataset automatically if it doesn't exist. The four tables will appear as `buro.personas`, `buro.domicilios`, `buro.cuentas`, `buro.consultas` in the BigQuery console under your project `dtc-de-340821`.


```bash
uv run python ingestar_circulo_credito.py \
  --zip gs://dtc-de-340821-xmls-bucket/xmls/bureau.zip \
  --project dtc-de-340821 \
  --credentials ~/.gcp/dtc-de-340821-b12a67c335f2.json \
  --disposition replace
```

```bash
python ingestar_circulo_credito.py \
  --zip gs://dtc-de-340821-xmls-bucket/xmls/bureau.zip \
  --applications gs://dtc-de-340821-xmls-bucket/applications.json \
  --credentials /secrets/credentials.json \
  --disposition replace
```

 "--zip"
 "--applications"
 "--project"
 "--credentials"
 "--disposition"
 "--location" 

```bash
python ingestar_circulo_credito.py \
  --zip $GCS_ZIP_URL \
  --applications "gs://${GCS_BUCKET_NAME}/applications.json" \
  --project $GCP_PROJECT \
  --credentials $SECRETS_PATH \
  --disposition replace

```

## 7. DBT

* install dbt
```bash
  uv add dbt-bigquery
  ## creates dbt subfolder in project folder
  uv run dbt init dbt
```

### 1. produce and ingest
```bash
  uv run python ingestion/producer_cc.py --archivos 100 --zip bundle.zip --gcs-bucket ...
  uv run python ingestion/ingestar_circulo_credito.py --disposition append
```

### 2. transform
```bash
cd dbt && uv run dbt run && uv run dbt test
```

### DBT CONTAINER

When dbt container up and running

```bash
  docker exect -it bureau-dbt bash
  chmod +x /scripts/seed_from_gcs.sh
  bash /scripts/seed_from_gcs.sh
```


```bash 
  docker exec -it bureau-dbt bash
  dbt debug
  dbt run
  dbt test
```



#### ISSUE WHEN UPDATING A PIPELINE

That's a dlt schema caching issue — the pipeline remembers the old schema from the previous run and doesn't pick up the new curp and rfc columns. You need to drop the pipeline state so it rebuilds the schema from scratch:

```bash
    dlt pipeline circulo_credito drop
```

Then re-run with replace:


```bash
    uv run python ingestar_circulo_credito.py --origen ./xmls --disposition replace
```

The `drop` command wipes dlt's local state (stored in `~/.dlt/pipelines/circulo_credito/`), forcing it to re-infer the schema from your current code on the next run. Without it, dlt sees the existing schema and skips columns it already thinks it knows about.


### features

* general 
  * average of consultas per application → needs to join with the application id
  * average of domicilios per application → needs to join with the application id
  * average of cuentas per application → needs to join with the application id
  * distribution application with cuentas vencidas | sin vencer

* per state
    * applications number
    * disbursment amount avg
    * monthly barplot N of distribution 