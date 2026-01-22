# Contributing to Crypto Data Platform

Thank you for your interest in contributing! This project is a Serverless Data Lakehouse built on GCP, Python, and Terraform.

## 🛠 Development Environment

### Option A: GitHub Codespaces (Recommended)
This project is configured with a `.devcontainer` specification.
1. Click the green **Code** button in GitHub.
2. Select **Codespaces** -> **Create codespace on main**.
3. The environment will auto-provision with Python 3.12, Terraform, and the Google Cloud CLI pre-installed.

### Option B: Local Development
If developing locally, you will need:
- Python 3.10+
- Terraform 1.5+
- Google Cloud SDK (gcloud)

## 🚀 Deployment Workflow
I use **Infrastructure as Code (IaC)**. Please do not make manual changes in the GCP Console.

1. **Infrastructure Changes:**
   - Modify files in `infra/`.
   - Run `terraform fmt` to ensure styling is correct.
   - Run `terraform plan` locally to verify changes.

2. **Cloud Functions:**
   - Logic resides in `src/cloud_functions/`.
   - Ensure you update `requirements.txt` if adding new libraries.

## 🧪 Testing
Run the unit test suite before submitting a PR:
```bash
pytest tests/
```

## Style Guide
- **Python**: I follow PEP 8.
- **Terraform**: I follow standard HCL formatting (`terraform fmt`).

## Pull Request Process
1. Fork the repository and create your branch from `main`.
2. Ensure CI passes (GitHub Actions will run Terraform formatting checks).
3. Update the README.md with details of changes to the interface or architecture.