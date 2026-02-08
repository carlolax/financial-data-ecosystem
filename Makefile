# Makefile for Crypto Data Platform

# Variables
PYTHON = python
PIP = pip

.PHONY: setup test local cloud deploy clean

# 📦 Setup: Install dependencies
setup:
	$(PIP) install -r requirements.txt
	@echo "Dependencies installed."

# 🧪 Test: Run all unit tests
test:
	pytest tests/

# 🏠 Local: Run pipeline locally
local:
	$(PYTHON) src/run_pipeline.py --mode local

# ☁️ Cloud: Run pipeline in GCP
cloud:
	$(PYTHON) src/run_pipeline.py --mode cloud

# 🚀 Deploy: Apply Terraform (Infrastructure)
deploy:
	cd infra && terraform apply -auto-approve

# 🧹 Clean: Remove cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@echo "Cleaned up cache files."
