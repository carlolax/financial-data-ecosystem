# ☁️ Crypto Data Platform (GCP + Python + Terraform)

![Build Status](https://github.com/carlolax/crypto-data-platform/actions/workflows/deploy.yaml/badge.svg)

A serverless, event-driven data engineering platform that ingests, processes, and analyzes cryptocurrency market data. This project uses **Infrastructure as Code (IaC)** to deploy a scalable, self-healing architecture on Google Cloud Platform and includes a **Hybrid Strategy Command Center** for visualization and real-time alerting.

## 🏗 Architecture & Design Decisions

**Region:** `us-central1` (Iowa) - *Optimized for GCP Free Tier*

> 🧠 **Deep Dive:** Want to know why I chose Serverless over Kubernetes? Read our **[Infrastructure Architecture Decisions](docs/infrastructure_decisions.md)**.

The pipeline follows a "Medallion Architecture" (Bronze → Silver → Gold), where each stage automatically triggers the next.

1.  **Ingestion (Bronze Layer):**
    * **Source:** CoinGecko API (`/coins/markets` endpoint).
    * **Features:**
        * **Smart Batching:** Automatically splits large coin lists into chunks.
        * **Rate Limiting:** Built-in throttling to respect API limits.
    * **Storage:** Google Cloud Storage (Raw JSON).
    * **Trigger:** Cloud Scheduler (Hourly).

2.  **Processing (Silver Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Bronze).
    * **Logic:** DuckDB (SQL-on-Serverless).
    * **Features:**
        * **Historical Reconstruction:** Rebuilds dataset from history every run using Wildcard Patterns.
        * **Schema Evolution:** Handles complex fields like `ath` and `circulating_supply`.
    * **Storage:** Google Cloud Storage (Parquet - Master History File).

3.  **Analytics & Alerting (Gold Layer):**
    * **Trigger:** Event-Driven (Fires after Silver Layer completion).
    * **Logic:** DuckDB + Python Requests.
    * **Features:**
        * **Financial Modeling:** Calculates **RSI (14-Day)**, **7-Day SMA**, and **Volatility**.
        * **Real-Time Alerts:** Sends rich notifications to **Discord** when "BUY" signals are detected.
    * **Storage:** Google Cloud Storage (Parquet - Analytics Ready).

4.  **Visualization (The Command Center):**
    * **Tool:** Streamlit (Python-based UI).
    * **Mode:** Hybrid (Toggle between `LOCAL` disk data and `CLOUD` live bucket data).
    * **Visuals:** Interactive Plotly charts, Momentum Oscillators, and Risk Heatmaps.

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Infrastructure:** Terraform (IaC) - *Manages IAM, Storage, and Compute.*
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
├── docs/                   # 📚 Architecture & Design Documents
│   └── infrastructure_decisions.md
├── data/                   # Local data storage (for testing/hybrid mode)
│   ├── bronze/             # Raw JSON files
│   ├── gold/               # Final Aggregated Parquet files
│   └── silver/             # Cleaned Parquet files
├── infra/                  # Terraform Infrastructure Code
│   ├── functions.tf        # Cloud Function definitions
│   ├── iam.tf              # Service Accounts & Permissions
│   ├── storage.tf          # GCS Bucket Definitions (w/ Random IDs)
│   ├── scheduler.tf        # Cloud Scheduler (Cron Jobs)
│   ├── provider.tf         # GCP Provider & Backend Config
│   └── variables.tf        # Input variable declarations
├── src/
│   ├── cloud_functions/    # Production-ready Cloud Functions
│   │   ├── bronze/         # Ingestion Logic (main.py)
│   │   ├── gold/           # Analytics & Alerting Logic (main.py)
│   │   └── silver/         # Transformation Logic (main.py)
│   ├── dashboard.py        # Hybrid Streamlit Dashboard
│   ├── pipeline/           # Local Data Pipeline Logic (Mirrors Cloud)
│   │   ├── bronze/         # Local ingestion script
│   │   ├── gold/           # Local analytics script
│   │   └── silver/         # Local cleaning script
│   ├── run_pipeline.py     # 🚀 Hybrid CLI Controller (Entry Point)
│   └── scripts/
│       └── backfill.py     # Utility to reload historical data
└── tests/                  # Unit Test Suite
    ├── __init__.py
    └── pipeline/
        ├── __init__.py
        ├── test_bronze.py  # API Mocking & Rate Limit Tests
        ├── test_gold.py    # Financial Math & Signal Logic Verification
        └── test_silver.py  # DuckDB SQL Integration Tests
```

## 🧪 Testing & Quality Assurance
This project uses **Pytest** to ensure reliability across all layers of the pipeline. The test suite covers API handling, SQL logic, and financial modeling without needing to hit live cloud resources.

**Run the Suite**
```bash
pytest
```

**Strategy**
1. **Bronze (Ingestion)**:
    - **Mocking**: Uses `unittest.mock` to simulate CoinGecko API responses.
    - **Safety**: Ensures no actual HTTP requests are made during testing to prevent rate-limiting or IP bans.
    - **Logic**: Verifies "Fail Fast" behavior on 429 errors and correct batch processing.
2. **Silver (Processing)**:
    - **Integration**: Uses Pytest's `tmp_path` to create temporary, real JSON files.
    - **Engine**: Runs actual DuckDB SQL queries against these temp files to verify column cleaning, deduplication, and schema evolution.
3. **Gold (Analytics)**:
    - **Verification**: Uses controlled Pandas DataFrames to simulate market patterns (e.g., "Bull Run" vs "Crash").
    - **Math**: mathematically verifies that the **RSI**, **7-Day SMA**, and **Volatility** calculations trigger the correct `BUY` or `SELL` signals.

## 🚀 Deployment & Usage Guide
1. **Setup**
**Prerequisites**:
- Google Cloud SDK (gcloud) installed and authenticated.
- Terraform installed.
- Python 3.10+ installed.

**Environment Config**: Create a `.env` file in the root directory:
```bash
BRONZE_FUNCTION_URL="[https://your-cloud-function-url-here.a.run.app](https://your-cloud-function-url-here.a.run.app)"
GOLD_BUCKET_NAME="your-gold-bucket-name-[random-id]"
DISCORD_WEBHOOK_URL="[https://discord.com/api/webhooks/](https://discord.com/api/webhooks/)..."
```

2. **Infrastructure (IaC)**

**Automated (Recommended)** Simply commit your changes to the `main` branch. GitHub Actions will automatically provision and update the infrastructure.

**Manual (Dev/Debug)**

1. Create a `terraform.tfvars` file in the `infra/` folder with your secrets (do not commit this file).
2. Run:
```bash
cd infra
terraform init
terraform apply
```

*Note: Buckets now use random suffixes (e.g., `cdp-bronze-lake-a1b2c3`) to ensure global uniqueness.*

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
- **Secret Management**: Discord Webhooks and sensitive keys are injected via Environment Variables and GitHub Secrets; they are never hardcoded.
- **Service Account**: Uses a dedicated `crypto-runner-sa` with restricted permissions (`storage.admin`).
- **Idempotency**: All functions are designed to run multiple times without corrupting data (Overwrite/Update logic).
- **Schema Enforcement**: Strict typing in DuckDB prevents pipeline crashes from bad API data.
- **Authentication**: Cloud Functions are private and require OIDC tokens; the `run_pipeline.py` script handles this securely via Google Auth libraries.
