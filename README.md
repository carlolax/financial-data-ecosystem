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
    * **Compute:** Google Cloud Function Gen 2 (Python 3.10).
    * **Trigger:** Cloud Scheduler (Hourly cron job) OR Client-Side Remote Control.
    * **Storage:** Google Cloud Storage (Raw JSON).
    * **Function:** `cdp-bronze-ingest-v2`

2.  **Processing (Silver Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Bronze).
    * **Logic:** DuckDB (SQL-on-Serverless).
    * **Stability:** Uses a **"Download → Process → Upload"** pattern to handle memory constraints and prevent C++ threading crashes in the serverless environment.
    * **Features:**
        * **Historical Reconstruction:** Uses a "Wildcard Pattern" (`*.json`) to rebuild the entire dataset from history every run.
        * **Schema Evolution:** Automatically handles complex fields like `ath`, `circulating_supply`, and `max_supply`.
    * **Storage:** Google Cloud Storage (Parquet - Master History File).
    * **Function:** `cdp-silver-clean-v2`

3.  **Analytics (Gold Layer):**
    * **Trigger:** Event-Driven (Fires after Silver Layer completion).
    * **Logic:** DuckDB (SQL-on-Serverless).
    * **Features:**
        * **Financial Modeling:** Calculates 7-Day Simple Moving Averages (SMA) and Volatility (Standard Deviation).
        * **Algorithmic Signals:** Generates "BUY" (Dip), "SELL" (Rally), or "WAIT" signals based on Mean Reversion strategy.
    * **Storage:** Google Cloud Storage (Parquet - Analytics Ready).
    * **Function:** `cdp-gold-analytics-v2`

4.  **Visualization (The Command Center):**
    * **Tool:** Streamlit (Python-based UI).
    * **Mode:** Hybrid (Toggle between `LOCAL` disk data and `CLOUD` live bucket data).
    * **Features:** Interactive Plotly charts and financial metrics.

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Infrastructure:** Terraform (IaC)
* **Data Processing:** Pandas (Ingest), DuckDB (OLAP Transformation)
* **Cloud:** Google Cloud Platform (Cloud Functions V2, Storage, Scheduler, IAM)
* **Visualization:** Streamlit, Plotly
* **Orchestration:** Eventarc (Triggers) & Custom Hybrid CLI (`run_pipeline.py`)

## 📂 Project Structure

```text
.
├── CONTRIBUTING.md
├── LICENSE
├── README.md
├── SECURITY.md
├── run_pipeline.py         # 🚀 Hybrid CLI Controller (Entry Point)
├── .env                    # (Excluded from Git) Local Environment Variables
├── data/                   # Local data storage (for testing)
│   ├── bronze/             # Raw JSON files
│   ├── gold/               # Final Aggregated Parquet files
│   └── silver/             # Cleaned Parquet files
├── infra/                  # Terraform Infrastructure Code
│   ├── bronze_layer_function.zip
│   ├── budget.tf           # Billing alerts
│   ├── functions.tf        # Cloud Function definitions
│   ├── gcp-key.json        # (Ignored) Service Account Key
│   ├── gold_layer_function.zip
│   ├── iam.tf              # Service Accounts & Permissions
│   ├── provider.tf         # GCP Provider & Backend Config
│   ├── scheduler.tf        # Cloud Scheduler (Cron Jobs)
│   ├── silver_layer_function.zip
│   ├── storage.tf          # GCS Bucket Definitions
│   ├── terraform.tfstate   # (Ignored) State file
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
│   │   └── silver/         # Local cleaning script (clean.py)
│   └── requirements.txt
└── tests/                  # Unit Test Suite
    ├── test_bronze.py
    └── test_silver.py
```

## 🚀 Deployment & Usage Guide
1. **Setup**
**Prerequisites**:
- Google Cloud SDK (gcloud) installed and authenticated.
- Terraform installed.
- Python 3.10+ installed.

**Environment Config**: Create a `.env` file in the root directory to store your Cloud Function URL:
```bash
BRONZE_FUNCTION_URL="[https://your-cloud-function-url-here.a.run.app](https://your-cloud-function-url-here.a.run.app)"
GOLD_BUCKET_NAME="your-gold-bucket-name"
```

2. **Infrastructure (IaC)**

**Option A**: **Automated (Recommended)** Simply commit your changes to the `main` branch. GitHub Actions will automatically provision and update the infrastructure.

**Option B**: **Manual (Dev/Debug)**
```bash
cd infra
terraform init
terraform apply
```

3. **Pipeline Control Center (Hybrid CLI)**

This project includes a custom CLI tool to orchestrate the pipeline in different modes.

| Mode | Command | Description |
|---|---|---|
| **Cloud (Default)** | `python run_pipeline.py --mode cloud` | Authenticates and triggers the live GCP pipeline. |
| **Local** | `python run_pipeline.py --mode local` | Runs the logic locally on your laptop (saves to `/data`). |
| **All** | `python run_pipeline.py --mode all` | Runs Local first, then Cloud (for comparison). |

4. **Visualization**

To see the results in the Strategy Command Center:
```bash
# Launch the Dashboard
streamlit run src/dashboard.py
```

*Note: Use the "**Data Source**" toggle in the sidebar to switch between `CLOUD` (Live) and `LOCAL` (Dev) modes instantly.*

## 🛡 Security
- Service Account: Uses a dedicated `crypto-runner-sa` with restricted permissions (`storage.admin`).
- Idempotency: All functions are designed to run multiple times without corrupting data (Overwrite logic).
- Schema Enforcement: Strict typing in DuckDB prevents pipeline crashes from bad API data.
- Authentication: Cloud Functions are private and require OIDC tokens; the `run_pipeline.py` script handles this securely via Google Auth libraries.
