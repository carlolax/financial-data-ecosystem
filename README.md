# ☁️ Crypto Data Platform (GCP + Python + Terraform)

![Build Status](https://github.com/carlolax/crypto-data-platform/actions/workflows/deploy.yml/badge.svg)

A serverless, event-driven data engineering platform that ingests, processes, and analyzes cryptocurrency market data. This project uses **Infrastructure as Code (IaC)** to deploy a scalable, self-healing architecture on Google Cloud Platform and includes a **Hybrid Strategy Command Center** for visualization.

## 🏗 Architecture

**Region:** `us-central1` (Iowa) - *Optimized for GCP Free Tier*

The pipeline follows a "Medallion Architecture" (Bronze → Silver → Gold), where each stage automatically triggers the next.

1.  **Ingestion (Bronze Layer):**
    * **Source:** CoinGecko API.
    * **Compute:** Google Cloud Function (Python 3.10).
    * **Trigger:** Cloud Scheduler (Daily cron job).
    * **Storage:** Google Cloud Storage (Raw JSON).
    * **Function:** `bronze-ingesting-func`

2.  **Processing (Silver Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Bronze).
    * **Logic:** DuckDB (SQL-on-Serverless).
    * **Transformation:** Performs Schema Enforcement (filters unknown coins) and unpivots data from Wide to Long format.
    * **Storage:** Google Cloud Storage (Parquet).
    * **Function:** `silver-cleaning-func`

3.  **Analytics (Gold Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Silver).
    * **Logic:** Aggregation & Window Functions (SQL).
        * Calculates **7-Day Moving Averages** and **Volatility**.
        * Generates **Buy/Wait/Hold Signals**.
    * **Storage:** Google Cloud Storage (Parquet).
    * **Function:** `gold-analyzing-func`

4.  **Visualization (The Command Center):**
    * **Tool:** Streamlit (Python-based UI).
    * **Mode:** Hybrid (Toggle between `LOCAL` disk data and `CLOUD` live bucket data).
    * **Features:** Interactive Plotly charts and financial metrics.

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Infrastructure:** Terraform
* **Data Processing:** Pandas (Local Ingest), DuckDB (Cloud Transformation)
* **Cloud:** Google Cloud Platform (Functions, Storage, Scheduler, IAM, Pub/Sub)
* **Visualization:** Streamlit, Plotly
* **Testing:** Pytest, Mocks (unittest.mock)
* **Data Format:** JSON (Raw) → Parquet (Compressed)

## 📂 Project Structure

```text
.
├── infra/                  # Terraform Infrastructure code
│   ├── main.tf             # Resource definitions (Buckets, Functions, IAM)
│   ├── variables.tf        # Input variable declarations
│   └── terraform.tfvars    # Configuration values (Region, IDs)
├── src/
│   ├── cloud_functions/    # Production-ready Cloud Functions
│   │   ├── bronze/         # Ingestion Logic (main.py + requirements.txt)
│   │   ├── silver/         # Transformation Logic (main.py + requirements.txt)
│   │   └── gold/           # Analytics & Signals Logic (main.py + requirements.txt)
│   ├── pipeline/           # Local Data Pipeline Logic
│   │   ├── bronze/         # Local ingestion script (ingest.py)
│   │   ├── silver/         # Local cleaning script (clean.py)
│   │   ├── gold/           # Local analytics script (analyze.py)
│   │   └── run_pipeline.py # Pipeline Orchestrator (Runs all layers)
│   └── dashboard.py        # Hybrid Streamlit Dashboard
├── tests/                  # Unit Test Suite
│   ├── test_bronze.py      # Bronze Layer Tests (Mocked API)
│   └── test_silver.py      # Silver Layer Tests (Mocked GCS + Real DuckDB)
├── data/                   # Local data storage (for testing)
│   ├── bronze/             # Raw JSON files
│   ├── silver/             # Cleaned Parquet files
│   └── gold/               # Final Aggregated Parquet files
└── README.md
```

## ⚙️ CI/CD Automation
This project uses GitHub Actions to automate the infrastructure deployment, ensuring a "GitOps" workflow where code changes automatically reflect in the cloud.

- **Workflow**: `.github/workflows/deploy.yml`
- **Trigger**: Pushes to the `main` branch (specifically for `infra/` or `src/cloud_functions/`).
- **Operations**:
    1. Setup: Authenticates via Workload Identity (Service Account Key).
    2. Lint: Runs `terraform fmt` to ensure code quality.
    3. Deploy: Runs `terraform apply` to update Google Cloud resources.
    4. State Management: Terraform State is stored remotely in a GCS Bucket (`gs://cdp-tf-state...`) to allow team collaboration and persistence.

## 🚀 Deployment Guide
**Prerequisites**
- Google Cloud SDK (gcloud) installed and authenticated.
- Terraform installed.
- Python 3.10+ installed.

### 1. Infrastructure Setup
**Option A: Automated (Recommended)** Simply commit your changes to the `main` branch. GitHub Actions will automatically provision and update the infrastructure.

**Option B: Manual (Dev/Debug)** Navigate to the infrastructure folder and apply the Terraform configuration manually.
```bash
cd infra
terraform init
terraform plan
terraform apply
```

### 2. Manual Trigger (The "Domino Effect")
You only need to trigger the Bronze function. The rest of the pipeline is fully automated.
```bash
gcloud functions call bronze-ingesting-func \
  --region=us-central1 \
  --data='{}'
```

### 3. Verification & Visualization
To see the results in the Strategy Command Center:
```bash
# Authenticate locally to read from GCS
gcloud auth application-default login

# Launch the Dashboard
streamlit run src/dashboard.py
```
*Note: Use the **"Data Source"** toggle in the sidebar to switch between `CLOUD` (Live) and `LOCAL` (Dev) modes instantly.*

## 🧪 Local Development
To run the logic locally without deploying to the cloud:
```bash
# Activate environment
source crypto-env/bin/activate

# Run the Orchestrator
python src/pipeline/run_pipeline.py
```
*Alternatively, you can run individual layers manually:*
```bash
python src/pipeline/bronze/ingest.py
python src/pipeline/silver/clean.py
python src/pipeline/gold/analyze.py
```

## 🛡 Security
- **Service Account**: Uses a dedicated `crypto-runner-sa` with restricted permissions (`storage.admin`).
- **Idempotency**: All functions are designed to run multiple times without corrupting data (Overwrite logic).
- **Schema Enforcement**: Strict typing in DuckDB prevents pipeline crashes from bad API data.
- **Secret Management**: Sensitive keys are stored in GitHub Secrets and never committed to the repository.