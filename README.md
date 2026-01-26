# ☁️ Crypto Data Platform (GCP + Python + Terraform)

![Build Status](https://github.com/carlolax/crypto-data-platform/actions/workflows/deploy.yaml/badge.svg)

A serverless, event-driven data engineering platform that ingests, processes, and analyzes cryptocurrency market data. This project uses **Infrastructure as Code (IaC)** to deploy a scalable, self-healing architecture on Google Cloud Platform and includes a **Hybrid Strategy Command Center** for visualization.

## 🏗 Architecture

**Region:** `us-central1` (Iowa) - *Optimized for GCP Free Tier*

The pipeline follows a "Medallion Architecture" (Bronze → Silver → Gold), where each stage automatically triggers the next.

1.  **Ingestion (Bronze Layer):**
    * **Source:** CoinGecko API (`/coins/markets` endpoint).
    * **Features:**
        * **Rich Data:** Captures ATH, Circulating Supply, High/Low 24h, and Market Cap Rank.
        * **Smart Batching:** Automatically splits large coin lists into chunks to ensure scalability.
        * **Rate Limiting:** Built-in throttling to respect API limits (prevents 429 errors).
        * **Fail-Safe:** Implements "Graceful Degradation" to prevent Cloud Retry Storms.
    * **Compute:** Google Cloud Function Gen 2 (Python 3.10).
    * **Trigger:** Cloud Scheduler (Hourly cron job).
    * **Storage:** Google Cloud Storage (Raw JSON).
    * **Function:** `function-bronze-ingest`

2.  **Processing (Silver Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Bronze).
    * **Logic:** DuckDB (SQL-on-Serverless).
    * **Features:**
        * **Historical Reconstruction:** Uses a "Wildcard Pattern" (`*.json`) to rebuild the entire dataset from history every run.
        * **Schema Evolution:** Automatically handles complex fields like `ath`, `circulating_supply`, and `max_supply`.
        * **Smart Valuation:** Calculates "Safe FDV" (Fully Diluted Valuation) for infinite-supply coins like ETH and DOGE.
    * **Storage:** Google Cloud Storage (Parquet - Master History File).
    * **Function:** `silver-cleaning-func`

3.  **Analytics (Gold Layer):**
    * **Trigger:** Event-Driven (Fires after Silver Layer completion).
    * **Logic:** DuckDB (SQL-on-Serverless).
    * **Features:**
        * **Financial Modeling:** Calculates 7-Day Simple Moving Averages (SMA) and Volatility (Standard Deviation).
        * **Algorithmic Signals:** Generates "BUY" (Dip), "SELL" (Rally), or "WAIT" signals based on Mean Reversion strategy.
        * **Dynamic Input:** Automatically detects and processes the latest historical Master File.
    * **Storage:** Google Cloud Storage (Parquet - Analytics Ready).
    * **Function:** `gold-analyzing-func`

4.  **Visualization (The Command Center):**
    * **Tool:** Streamlit (Python-based UI).
    * **Mode:** Hybrid (Toggle between `LOCAL` disk data and `CLOUD` live bucket data).
    * **Features:** Interactive Plotly charts and financial metrics.

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Infrastructure:** Terraform
* **Data Processing:** Pandas (Local Ingest), DuckDB (Cloud Transformation)
* **Cloud:** Google Cloud Platform (Functions, Storage, Scheduler, IAM)
* **Visualization:** Streamlit, Plotly
* **Testing:** Pytest, Mocks (unittest.mock)
* **Data Format:** JSON (Raw) → Parquet (Compressed)

## 📂 Project Structure

```text
.
├── CONTRIBUTING.md
├── LICENSE
├── README.md
├── SECURITY.md
├── data/                   # Local data storage (for testing)
│   ├── bronze/             # Raw JSON files
│   ├── gold/               # Final Aggregated Parquet files
│   └── silver/             # Cleaned Parquet files
├── infra/                  # Terraform Infrastructure Code
│   ├── bronze_layer_function.zip
│   ├── budget.tf           # Billing alerts
│   ├── functions.tf        # Cloud Function definitions (Source Zipping + Deployment)
│   ├── gcp-key.json        # (Ignored) Service Account Key
│   ├── gold_layer_function.zip
│   ├── iam.tf              # Service Accounts & Permissions
│   ├── provider.tf         # GCP Provider & Backend Config
│   ├── scheduler.tf        # Cloud Scheduler (Cron Jobs)
│   ├── silver_layer_function.zip
│   ├── storage.tf          # GCS Bucket Definitions (Bronze/Silver/Gold)
│   ├── terraform.tfstate   # (Ignored) State file
│   ├── terraform.tfvars    # Configuration values (Region, IDs)
│   └── variables.tf        # Input variable declarations
├── src/
│   ├── cloud_functions/    # Production-ready Cloud Functions
│   │   ├── bronze/         # Ingestion Logic (main.py + requirements.txt)
│   │   ├── gold/           # Analytics Logic (main.py + requirements.txt)
│   │   └── silver/         # Transformation Logic (main.py + requirements.txt)
│   ├── dashboard.py        # Hybrid Streamlit Dashboard
│   ├── environment.yaml    # Conda Environment
│   ├── pipeline/           # Local Data Pipeline Logic
│   │   ├── bronze/         # Local ingestion script (ingest.py)
│   │   ├── gold/           # Local analytics script (analyze.py)
│   │   ├── run_pipeline.py # Orchestrator
│   │   └── silver/         # Local cleaning script (clean.py)
│   └── requirements.txt
└── tests/                  # Unit Test Suite
    ├── test_bronze.py
    └── test_silver.py
```

## ⚙️ CI/CD Automation
This project uses GitHub Actions to automate the infrastructure deployment, ensuring a "GitOps" workflow where code changes automatically reflect in the cloud.

- **Workflow**: `.github/workflows/deploy.yaml`
- **Trigger**: Pushes to the `main` branch (specifically for `infra/` or `src/cloud_functions/`).
- **Operations**:
    1. Setup: Authenticates via Workload Identity (Service Account Key).
    2. Lint: Runs `terraform fmt` to ensure code quality.
    3. Deploy: Runs `terraform apply` to update Google Cloud resources.
    4. State Management: Terraform State is stored remotely in a GCS Bucket to allow team collaboration and persistence.

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
gcloud functions call function-bronze-ingest \
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