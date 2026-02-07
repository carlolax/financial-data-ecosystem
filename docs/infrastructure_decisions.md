# Infrastructure Architecture & Decisions

## 1. High-Level Strategy: "Serverless First"
This project utilizes a **Serverless Architecture** on Google Cloud Platform (GCP). 
By choosing Serverless over traditional VM-based infrastructure, we shift the operational burden (patching, networking, scaling) to the cloud provider, allowing us to focus purely on **Data Quality** and **Business Logic**.

## 2. The "Big 6" Infrastructure Mapping
How this project fulfills core infrastructure requirements without managing servers:

| Requirement | Traditional Approach (VMs) | Our Approach (Serverless) | Benefit |
| :--- | :--- | :--- | :--- |
| **Compute** | EC2 / Compute Engine | **Cloud Functions (Python)** | Zero idle cost; automatic scaling from 0 to 1000+. |
| **Network** | VPCs, Subnets, Firewalls | **IAM & Service Accounts** | Security is identity-based, not IP-based. |
| **Storage** | EBS / Hard Drives | **Cloud Storage (GCS)** | Decoupled storage; infinite durability; 99.99% availability. |
| **Database** | Postgres / MySQL Server | **DuckDB + Parquet** | OLAP capabilities without managing a database server. |
| **State** | Local File System | **Terraform State (GCS)** | Infrastructure state is locked and shared remotely. |
| **Security** | SSH Keys / VPNs | **IAM Roles (Least Privilege)** | Granular permission control per function/bucket. |

## 3. Storage Design: The Medallion Architecture
Data flows through three distinct stages of quality:

1.  **Bronze (Raw):** `cdp-bronze-lake-[id]`
    * **Format:** JSON (Raw API response).
    * **Retention:** 30 days (Lifecycle Rule).
    * **Goal:** Immutable record of ingestion.

2.  **Silver (Clean):** `cdp-silver-clean-[id]`
    * **Format:** Parquet (Compressed, Columnar).
    * **Retention:** Indefinite.
    * **Goal:** The Single Source of Truth. Deduplicated and typed.

3.  **Gold (Aggregated):** `cdp-gold-analyze-[id]`
    * **Format:** Parquet.
    * **Goal:** Business-level aggregates (Daily Average, Volatility) ready for dashboards.

## 4. Infrastructure as Code (IaC)
All resources are provisioned via **Terraform**, ensuring the environment is reproducible and version-controlled.
- **Global Uniqueness:** Random IDs are appended to bucket names to prevent global namespace collisions.
- **State Locking:** GCP Backend prevents concurrent updates to infrastructure.