# ☁️ Crypto Data Platform

![Build Status](https://github.com/carlolax/crypto-data-platform/actions/workflows/deploy.yaml/badge.svg)

A serverless, event-driven financial data ecosystem that ingests, cleans, and analyzes high-frequency cryptocurrency market data. This project implements a strict **Medallion Architecture** with **Environment Parity**, ensuring that historical backfills (Local) and live data streams (Cloud) are mathematically identical.

## 🚀 Quick Start (Makefile)

This project includes a `Makefile` to streamline the developer experience.

| Command | Description |
| :--- | :--- |
| `make setup` | Install all Python dependencies. |
| `make local` | Run the full pipeline locally (Ingest → Clean → Analyze) using `src/pipeline/`. |
| `make cloud` | Trigger the live Cloud Functions on GCP using `src/cloud_functions/`. |
| `make backfill`| Run the historical data fetcher ("Smash & Grab" strategy). |
| `make test` | Run the Pytest suite to verify logic. |
| `make deploy` | Deploy infrastructure via Terraform. |
| `make clean` | Remove temporary cache files. |

## 🏗 Architecture & Design Decisions

The pipeline follows a **"Rich Schema"** philosophy, preserving critical financial metrics (FDV, Volume, Supply) from ingestion through to analytics to support deep-dive research.

### 1. 🥉 Bronze Layer (Ingestion)
* **Source:** CoinGecko API (`/coins/markets`).
* **Strategy - Local:** **"Fail Fast"** with Exponential Backoff (Retries on 429). Uses "Stealth Mode" headers to prevent IP bans during heavy backfills.
* **Strategy - Cloud:** **"Graceful Degradation"**. Returns empty lists on errors to prevent Cloud Scheduler retry storms.
* **Storage:** Google Cloud Storage (Raw JSON).

### 2. 🥈 Silver Layer (Cleaning & Deduplication)
* **Engine:** DuckDB (In-Memory).
* **Parity:** **100% SQL Logic Match** between Local and Cloud.
* **Calculations:**
    * **Safe FDV:** Calculates Fully Diluted Valuation, handling `NULL` Max Supply (e.g., ETH) correctly.
    * **Normalization:** Casts timestamps to UTC and standardizes column types.
* **Storage:** Snappy-compressed Parquet (`clean_prices_YYYYMMDD.parquet`).

### 3. 🥇 Gold Layer (Analytics & Alerting)
* **Engine:** DuckDB Window Functions.
* **Logic:** **State Management**. Merges new incoming data with the existing `analyzed_market_summary.parquet` to ensure Moving Averages and RSI are calculated over the full history, not just the current batch.
* **Indicators:**
    * **SMA_7:** 7-Day Simple Moving Average.
    * **RSI_14:** 14-Day Relative Strength Index (Momentum).
    * **Volatility:** Standard Deviation of price changes.
* **Storage:** Google Cloud Storage (Parquet - Analytics Ready).

## 🛠 Tech Stack

* **Language:** Python 3.12
* **Infrastructure:** Terraform (IaC) - *Manages IAM, Storage, and Compute.*
* **Data Processing:** DuckDB (OLAP Transformation)
* **Cloud:** Google Cloud Platform (Cloud Functions Gen 2, Storage, Scheduler, IAM)
* **Orchestration:** Eventarc (Triggers) & Custom Hybrid CLI (`run_pipeline.py`)

## 📂 Project Structure

```text
.
├── CONTRIBUTING.md
├── LICENSE
├── Makefile
├── README.md
├── SECURITY.md
├── data/                   # Local storage for hybrid/testing mode
│   ├── bronze/             # Raw JSON files
│   ├── gold/               # Final Analytics Parquet files
│   └── silver/             # Cleaned Parquet files
├── docs/
│   └── infrastructure_decisions.md
├── infra/                  # Terraform Infrastructure Code
│   ├── budget.tf
│   ├── functions.tf
│   ├── iam.tf
│   ├── provider.tf
│   ├── scheduler.tf
│   ├── storage.tf
│   └── variables.tf
├── src/
│   ├── cloud_functions/    # Production-ready Cloud Functions
│   │   ├── bronze/         # Ingestion (Smart Retries + Stealth)
│   │   ├── silver/         # Cleaning (Rich Schema Preservation)
│   │   └── gold/           # Analytics (Stateful Window Functions)
│   ├── pipeline/           # Local Python Scripts (Historical Backfills)
│   │   ├── bronze/         # ingest.py
│   │   ├── silver/         # clean.py
│   │   └── gold/           # analyze.py
│   ├── scripts/            # Utility Scripts
│   │   └── backfill.py     # "Smash & Grab" Historical Data Fetcher
│   ├── dashboard.py        # Streamlit Dashboard (Hybrid Mode)
│   ├── run_pipeline.py     # 🚀 Hybrid CLI Controller (Entry Point)
│   └── requirements.txt
└── tests/                  # Pytest Suite
    └── pipeline/
        ├── test_bronze.py
        ├── test_gold.py
        └── test_silver.py
```

## 🧪 Testing & Quality Assurance
This project uses **Pytest** to ensure reliability across all layers.

**Run the Suite**
```bash
make test
```

**Strategy**
1. **Bronze**: Mocks API responses to verify "Retry Logic" without hitting real endpoints.
2. **Silver**: Uses `tmp_path` to verify DuckDB SQL transformation logic and Schema Parity.
3. **Gold**: Mathematically verifies that **RSI** and **SMA** signals (`BUY`/`SELL`) trigger correctly on synthetic market data.

## 🚀 Deployment & Usage Guide
1. **Setup**
**Prerequisites**:
- Google Cloud SDK (gcloud) installed and authenticated.
- Terraform installed.
- Python 3.12+ installed.

**Environment Config**: Create a `.env` file in the root directory:
```bash
# --- Google Cloud Configuration ---
# The URL of your deployed Bronze Cloud Function
BRONZE_FUNCTION_URL="https://YOUR_REGION-YOUR_PROJECT.cloudfunctions.net/cdp-bronze-ingest"

# The name of your Gold Bucket (used by Dashboard to download results)
GOLD_BUCKET_NAME="cdp-gold-analyze-bucket-[id]"

# --- Alerting Configuration ---
# The Webhook URL for your Discord Server
DISCORD_WEBHOOK_URL="[https://discord.com/api/webhooks/](https://discord.com/api/webhooks/)..."

# --- Data Configuration ---
# List of cryptocurrency tokens for collecting data
CRYPTO_COINS="bitcoin,ethereum,solana,cardano,binancecoin,ripple,dogecoin,chainlink,uniswap,litecoin"

# --- Optional (For Local Development) ---
# Toggle for debug mode (True/False)
DEBUG_MODE="False"
```

2. **Infrastructure (IaC)**

Navigate to `infra/` and apply the Terraform configuration to provision Buckets, Service Accounts, and Cloud Functions.

```bash
make deploy
```

## 🛡 Security
- **Stealth Mode**: All ingestion scripts use browser-mimicking headers.
- **Secret Management**: Discord Webhooks and sensitive keys are injected via Environment Variables; never hardcoded.
- **Least Privilege**: Uses a dedicated `crypto-runner-sa` Service Account with restricted permissions.
