# 🪙 Crypto Data Platform

An end-to-end Data Engineering project built on **Google Cloud Platform (GCP)** using the **Medallion Architecture**.
This pipeline ingests real-time cryptocurrency data, cleans it, and calculates business metrics (Moving Averages) using high-performance SQL.

---

## 🏗 Architecture (Medallion Pattern)

| Layer | Status | Technology | Description |
| :--- | :--- | :--- | :--- |
| **Bronze** | ✅ Done | Python, GCS | Raw JSON data ingested from CoinGecko API. |
| **Silver** | ✅ Done | Pandas, GCS | Cleaned CSV data with standardized types and timestamps. |
| **Gold** | ✅ Done | **DuckDB**, SQL | Aggregated Parquet files with **7-Day Moving Averages**. |

---

## 🛠 Tech Stack

* **Language:** Python 3.12
* **Environment:** Miniforge (Conda)
* **Cloud:** Google Cloud Storage (GCS)
* **Infrastructure:** Terraform (IaC)
* **Analytics:** DuckDB (In-memory SQL OLAP)
* **Format:** JSON (Raw) -> CSV (Processed) -> Parquet (Analytics)

---

## 🚀 How to Run Locally

### 1. Environment Setup
```bash
conda env create -f environment.yaml

conda activate crypto-env
```

### 2. Infrastructure (Terraform)
```bash
cd infra
terraform init
terraform apply
```

### 3. Data Pipeline Execution
#### Step 1: Ingest Raw Data (Bronze)
```bash
python src/bronze/ingest.py
```

#### Step 2: Clean & Standardize (Silver)
```bash
python src/silver/clean.py
```

#### Step 3: Analytics & Aggregation (Gold)
- Downloads Silver data locally.
- Runs DuckDB SQL Window Functions to calculate volatility and moving averages.
- Uploads Parquet files to the Gold bucket.

```bash
python src/gold/aggregate.py
```

## 📂 Project Structure
```plaintext
├── data/                  # Local temp data (ignored by Git)
├── infra/                 # Terraform Infrastructure as Code
│   ├── main.tf            # Bucket definitions (Bronze, Silver, Gold)
│   └── terraform.tfvars   # Project variables
├── src/
│   ├── bronze/            # Ingestion Scripts
│   ├── silver/            # Transformation Scripts
│   └── gold/              # Aggregation Scripts (DuckDB)
├── environment.yaml       # Conda Environment Definition
└── requirements.txt       # Cloud Deployment Dependencies
```