import sys
import argparse
import subprocess
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import shutil
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# --- SETUP ---
# 1. Automatically find the .env file (walks up directories)
env_path = find_dotenv()

if env_path:
    print(f"✅ Configuration loaded from: {env_path}")
    load_dotenv(env_path)
else:
    print("❌ CRITICAL ERROR: .env file NOT FOUND.")
    print("   Please ensure you have created a file named '.env' in the project root.")

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent.parent
FUNCTION_URL = os.getenv("BRONZE_FUNCTION_URL")
GOLD_BUCKET_NAME = os.getenv("GOLD_BUCKET_NAME", "Unknown Bucket")
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"

# --- LOCAL MODULES IMPORT ---
# This ensures I can import 'src.pipeline' regardless of how the script is run
sys.path.append(str(BASE_DIR))

try:
    from src.pipeline.bronze import ingest
    from src.pipeline.silver import clean
    from src.pipeline.gold import analyze
except ImportError as error:
    print(f"\n❌ IMPORT ERROR: {error}")
    print("   Attempting fallback import.")
    try:
        # Fallback for when running directly inside src/
        from pipeline.bronze import ingest
        from pipeline.silver import clean
        from pipeline.gold import analyze
    except ImportError as error:
        print(f"   ❌ CRITICAL: Could not import pipeline modules.")
        print(f"   Details: {error}")
        sys.exit(1)

# ==========================================
# ☁️ CLOUD LOGIC
# ==========================================
def get_gcloud_token() -> str | None:
    """
    Retrieves an OIDC identity token using the installed gcloud CLI.

    This function is a fallback mechanism for local development. When the standard
    Python Google Auth library fails (common in local environments without 
    Service Account keys), this function invokes the 'gcloud auth print-identity-token' 
    shell command to generate a valid credential.

    Returns:
        str: A valid OIDC Identity Token if successful.
        None: If the gcloud tool is not installed or the command fails.
    """
    if not shutil.which("gcloud"):
        print("❌ Error: 'gcloud' CLI not found on PATH.")
        return None
    try:
        return subprocess.check_output(
            ["gcloud", "auth", "print-identity-token"], text=True
        ).strip()
    except subprocess.CalledProcessError as error:
        print(f"❌ gcloud Auth Failed: {error}")
        return None

def get_id_token(url: str) -> str | None:
    """
    Generates a Google OIDC token suitable for invoking Cloud Functions.

    It implements a 'Hybrid Strategy' for authentication:
    1. Primary: Attempts to use the official 'google-auth' library (Best for CI/CD).
    2. Fallback: Switches to the 'gcloud' CLI if the library fails (Best for Local Dev).

    Args:
        url (str): The target Audience URL (the Cloud Function endpoint) the token is intended for.

    Returns:
        str: A valid Bearer token for the HTTP Authorization header.
        None: If authentication fails via all available methods.
    """
    try:
        auth_req = google.auth.transport.requests.Request()
        return google.oauth2.id_token.fetch_id_token(auth_req, url)
    except Exception:
        if DEBUG_MODE:
            print("⚠️  Python Auth library failed. Switching to gcloud CLI.")
        return get_gcloud_token()

def run_cloud_pipeline():
    """
    Triggers the remote Cloud Data Pipeline via an authenticated HTTP request.

    This function acts as a remote control. It does not execute data processing logic locally.
    Instead, it:
    1. Authenticates the user via Google Cloud IAM.
    2. Sends a POST request to the deployed Bronze Layer Cloud Function.
    3. Prints the response status (Success/Failure) to the console.

    Usage:
        Run this when you want to test the *actual* deployed infrastructure on GCP.
    """
    if not FUNCTION_URL:
        print("❌ Error: 'BRONZE_FUNCTION_URL' not found in .env file.")
        return

    print(f"\n☁️  STARTING CLOUD PIPELINE.")
    print(f"🔗 Target: {FUNCTION_URL}")

    print("🔑 Authenticating.")
    token = get_id_token(FUNCTION_URL)

    if not token:
        print("❌ Critical Auth Failure: Could not generate OIDC token.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        print("📡 Sending Trigger.")
        response = requests.post(FUNCTION_URL, headers=headers, json={})

        if response.status_code == 200:
            print("✅ SUCCESS: Cloud Pipeline Triggered.")
        else:
            print(f"❌ FAILED. Status: {response.status_code}")
            print(f"   Details: {response.text}")

    except Exception as error:
        print(f"❌ Network Error: {error}")

# ==========================================
# 💻 LOCAL LOGIC
# ==========================================
def run_local_pipeline():
    """
    Executes the entire Data Pipeline logic locally on the host machine.

    This function runs the Python logic directly using the local CPU and File System,
    bypassing Google Cloud entirely. It is useful for debugging logic changes,
    testing new features, or running historical backfills.

    Workflow:
    1. Bronze Layer (Ingest): Fetches data from CoinGecko and saves JSON to 'data/bronze/'.
    2. Silver Layer (Clean): Scans 'data/bronze/', cleans data via DuckDB, saves Parquet to 'data/silver/'.
    3. Gold Layer (Analyze): Scans 'data/silver/', runs financial models, saves Parquet to 'data/gold/'.
    """
    print(f"\n💻 STARTING LOCAL PIPELINE.")

    try:
        # Step 1: Bronze Layer - Ingestion
        print("\n👇 [1/3] BRONZE LAYER (Ingestion)")
        ingest.process_ingestion()

        # Step 2: Silver Layer - Cleaning
        print("\n👇 [2/3] SILVER LAYER (Cleaning)")
        clean.process_cleaning()

        # Step 3: Gold Layer - Analysis
        print("\n👇 [3/3] GOLD LAYER (Analysis)")
        analyze.process_analysis()

        print("\n🎉 Local Pipeline Finished Successfully.")

    except Exception as error:
        print(f"\n❌ Local Pipeline Failed: {error}")
        if DEBUG_MODE:
            import traceback
            traceback.print_exc()

# ==========================================
# 🎮 MAIN CONTROLLER
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto Pipeline Controller")
    parser.add_argument(
        "--mode", 
        choices=["local", "cloud"], 
        default="local",
        help="Choose execution mode: 'local' (laptop) or 'cloud' (GCP)."
    )

    args = parser.parse_args()

    # --- STARTUP SUMMARY ---
    print("="*40)
    print(f"🚀 CRYPTO PIPELINE CONTROLLER")
    print(f"   Mode:  {args.mode.upper()}")
    print(f"   Debug: {DEBUG_MODE}")
    print("="*40)

    if args.mode == "local":
        run_local_pipeline()
    elif args.mode == "cloud":
        run_cloud_pipeline()
