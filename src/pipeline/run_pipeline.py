import sys
from pathlib import Path

# --- SETUP ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# --- IMPORTS ---
from src.pipeline.bronze.ingest import process_data_ingestion
from src.pipeline.silver.clean import process_data_cleaning
from src.pipeline.gold.analyze import process_data_analytics

def run_pipeline():
    """
    Orchestrates the local data pipeline: Bronze -> Silver -> Gold.
    """
    print(f"🚀 Initializing Crypto Data Pipeline from: {PROJECT_ROOT}\n")

    try:
        # --- STEP 1: BRONZE (Data Ingest) ---
        raw_file = process_data_ingestion()
        print(f"✅ [Bronze] Ingestion Complete. Raw file: {raw_file.name}\n")

        # --- STEP 2: SILVER (Data Clean) ---
        clean_file = process_data_cleaning()
        print(f"✅ [Silver] Transformation Complete. Parquet file: {clean_file.name}\n")

        # --- STEP 3: GOLD (Data Analyze) ---
        final_report = process_data_analytics()
        print(f"✅ [Gold] Analysis Complete. Report: {final_report.name}\n")

        print("🎉 Pipeline Finished Successfully. Data is ready for the Dashboard Visualization.")

    except Exception as error:
        # This catches any 'errors' from the steps above
        print(f"\n❌ Pipeline Failed during execution: {error}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
    