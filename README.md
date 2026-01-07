# 🪙 Crypto Data Platform

A serverless end-to-end data engineering pipeline that extracts cryptocurrency market data, processes it, and stores it in a Data Lake on Google Cloud Platform (GCP).

## 🏗️ Architecture
**Medallion Architecture Flow:**
1.  **Extract (Bronze):** Python script fetches real-time prices (Bitcoin, Ethereum, Solana) from CoinGecko API.
2.  **Load:** Raw JSON data is uploaded to **Google Cloud Storage (GCS)**.
3.  **Transform (Silver):** Python script cleans the JSON, flattens nested structures, adds timestamps, and saves as structured **CSV**.
4.  **Infrastructure:** All cloud resources (Bronze & Silver buckets) are provisioned via **Terraform**.

## 🛠️ Tech Stack
* **Language:** Python 3.12 (Pandas, Requests, GCSFS)
* **Cloud:** Google Cloud Platform (GCS)
* **IaC:** Terraform
* **Containerization:** *[Planned]* Docker
* **Orchestration:** *[Planned]* GitHub Actions

## 🚀 Setup & Usage

### 1. Prerequisites
* Google Cloud SDK (`gcloud`)
* Terraform
* Python 3.x

### 2. Infrastructure Setup
```bash
cd infra
# Initialize and Apply Terraform
terraform init
terraform apply
```

### 3. Run the Pipeline
**Step 1: Ingest Data (Bronze Layer)** Fetches live data and saves to the Raw Bucket.
```bash
python src/bronze/ingest.py
```

**Step 2: Transform Data (Silver Layer)** Cleans the latest raw file and saves to the Clean Bucket.
```bash
python src/silver/clean.py
```

## 📂 Project Structure
```bash
├── data/           # Local data (gitignored)
├── infra/          # Terraform IaaC code
├── src/
│   ├── bronze/     # Ingestion scripts (API -> GCS)
│   ├── silver/     # Transformation scripts (GCS -> GCS)
│   └── gold/       # Feature Engineering (ML Ready)
└── README.md
```
