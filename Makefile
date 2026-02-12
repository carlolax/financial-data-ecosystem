# Makefile for Crypto Data Platform

# Variables
PYTHON = python
PIP = pip

.PHONY: setup test local cloud deploy clean backfill help

# 🆘 Help: Show available commands
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  setup    📦 Install dependencies"
	@echo "  test     🧪 Run unit tests"
	@echo "  local    🏠 Run pipeline locally (Bronze -> Silver -> Gold)"
	@echo "  cloud    ☁️  Trigger Cloud Functions (via run_pipeline.py)"
	@echo "  backfill 📜 Run the historical backfill script"
	@echo "  deploy   🚀 Deploy Infrastructure (Terraform)"
	@echo "  clean    🧹 Remove cache files"

# 📦 Setup: Install dependencies
setup:
	$(PIP) install -r requirements.txt
	@echo "✅ Dependencies installed."

# 🧪 Test: Run all unit tests
test:
	PYTHONPATH=. pytest tests/ -v

# 🏠 Local: Run pipeline locally
local:
	$(PYTHON) src/run_pipeline.py --mode local

# ☁️ Cloud: Run pipeline in GCP
cloud:
	$(PYTHON) src/run_pipeline.py --mode cloud

# 📜 Backfill: Run the historical data script
backfill:
	$(PYTHON) src/scripts/backfill.py

# 🚀 Deploy: Apply Terraform (Infrastructure)
deploy:
	cd infra && terraform apply -auto-approve

# 🧹 Clean: Remove cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@echo "✨ Cleaned up cache files."
