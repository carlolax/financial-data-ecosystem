# ☁️ Crypto Data Platform (GCP + Python + Terraform)

A serverless data engineering platform that ingests, processes, and analyzes cryptocurrency market data. This project uses **Infrastructure as Code (IaC)** to deploy a scalable architecture on Google Cloud Platform.

## 🏗 Architecture

**Region:** `australia-southeast1` (Sydney)

1.  **Ingestion (Bronze):**
    * **Source:** CoinGecko API.
    * **Compute:** Google Cloud Function (Python 3.10).
    * **Trigger:** Cloud Scheduler (Daily cron job).
    * **Storage:** Google Cloud Storage (JSON).
2.  **Processing (Silver):** *[Coming Soon]*
    * DuckDB for cleaning and deduplication.
    * Parquet storage.
3.  **Analytics (Gold):** *[Coming Soon]*
    * Business logic and aggregation.

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Infrastructure:** Terraform
* **Database:** DuckDB (In-process SQL OLAP)
* **Cloud:** Google Cloud Platform (Functions, Storage, Scheduler, IAM)

## 📂 Project Structure

```text
.
├── infra/                  # Terraform Infrastructure code
│   ├── main.tf             # Resource definitions (Buckets, Functions)
│   ├── variables.tf        # Input variable declarations
│   └── terraform.tfvars    # Configuration values (Region, IDs)
├── src/
│   ├── cloud_functions/    # Production-ready Cloud Functions
│   │   └── bronze/         # Ingestion Logic (main.py)
│   └── bronze/             # Local testing scripts
├── data/                   # Local data storage (for testing)
└── README.md
```

## 🚀 Deployment Guide

### Prerequisites
- Google Cloud SDK (gcloud) installed and authenticated.
- Terraform installed.
- Python 3.10+ installed.

### 1. Infrastructure Setup
Navigate to the infrastructure folder and apply the Terraform configuration.

```bash
cd infra
terraform init
terraform plan
terraform apply
```

### 2. Manual Trigger (Testing)
You can manually trigger the ingestion function from the CLI to verify it works:

```bash
gcloud functions call bronze-ingest-func \
  --region=australia-southeast1 \
  --data='{}'
```

## 🧪 Local Development
To run the logic locally without deploying to the cloud:

```bash
# Activate environment
source crypto-env/bin/activate

# Run local ingestion script
python src/bronze/ingest.py
```

## 🛡 Security
- Service Account: Uses a dedicated `crypto-runner-sa` with restricted permissions (`storage.admin`).
- Region: All resources confined to `australia-southeast1` for data sovereignty.
- Secrets: No API keys or secrets are committed to the repository.
