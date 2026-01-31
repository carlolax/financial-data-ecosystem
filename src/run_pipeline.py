import sys
import argparse
import subprocess
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import shutil
import os
from dotenv import load_dotenv

# --- SETUP ---
load_dotenv()

# --- CONFIGURATION ---
FUNCTION_URL = os.getenv("BRONZE_FUNCTION_URL")

# --- LOCAL MODULES IMPORT ---
try:
    from pipeline.bronze.ingest import process_ingestion
    from pipeline.silver.clean import process_cleaning
    from pipeline.gold.analyze import process_analysis
except ImportError:
    # Fallback: If running from root without src on path, try appending it manually
    # This makes the script robust regardless of how you run it.
    sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
    try:
        from pipeline.bronze.ingest import process_ingestion
        from pipeline.silver.clean import process_cleaning
        from pipeline.gold.analyze import process_analysis
    except ImportError as error:
        print(f"\n❌ CRITICAL ERROR: Could not import local pipeline modules.")
        print(f"   Details: {error}")
        print("   Make sure you are running this script from the project root.\n")
        sys.exit(1)

# ==========================================
# ☁️ CLOUD LOGIC
# ==========================================
def get_gcloud_token():
    """
    Fallback Authentication: Uses the installed 'gcloud' CLI to generate a token.

    Why this is needed:
    The standard Python Auth library often restricts User Credentials (like local 
    'gcloud auth login' sessions) from generating OIDC Identity Tokens directly. 
    The CLI tool allows developers to bypass this restriction for local testing.

    Returns:
        str: A valid OIDC Identity Token.
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

def get_id_token(url):
    """
    Generates a Google OIDC token using a Hybrid Strategy.

    Strategy:
    1. Try the official 'google-auth' library first.
       - Best for Production environments (Cloud Run, Compute Engine, etc).
    2. Fallback to 'gcloud' CLI if the library fails.
       - Best for Local Development (Laptop).

    Args:
        url (str): The target URL (Audience) the token is intended for.

    Returns:
        str: A valid Bearer token for the Authorization header.
    """
    try:
        auth_req = google.auth.transport.requests.Request()
        return google.oauth2.id_token.fetch_id_token(auth_req, url)
    except Exception:
        print("⚠️  Python Auth failed. Switching to gcloud CLI.")
        return get_gcloud_token()

def run_cloud_pipeline():
    """
    Triggers the remote Cloud Data Pipeline via HTTP.

    This function acts as the 'Remote Control'. It does not process data locally;
    instead, it sends an authenticated signal to the Cloud Bronze Layer to start
    the Event-Driven workflow (Bronze -> Silver -> Gold).
    """
    if not FUNCTION_URL:
        print("❌ Error: 'BRONZE_FUNCTION_URL' not found in .env file.")
        return

    print(f"\n☁️  STARTING CLOUD PIPELINE.")
    print(f"🔗 Target: {FUNCTION_URL}")

    print("🔑 Authenticating.")
    token = get_id_token(FUNCTION_URL)
    if not token:
        print("❌ Critical Auth Failure.")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        print("📡 Sending Trigger.")
        response = requests.post(FUNCTION_URL, headers=headers, json={})
        if response.status_code == 200:
            print("✅ SUCCESS. Cloud Pipeline Triggered.")
            print(f"RESPONSE: {response.text}")
        else:
            print(f"❌ FAILED. Status: {response.status_code} - {response.text}")
    except Exception as error:
        print(f"❌ Network Error: {error}")

# ==========================================
# 💻 LOCAL LOGIC
# ==========================================
def run_local_pipeline():
    """
    Executes the entire Data Pipeline logic locally on the host machine.

    This bypasses the cloud infrastructure and runs the Python logic directly
    using the local CPU and File System. 

    Workflow:
    1. Bronze: Calls CoinGecko API -> Saves JSON to local 'data/bronze/'
    2. Silver: Reads JSON -> Cleans with DuckDB -> Saves Parquet to 'data/silver/'
    3. Gold: Reads Parquet -> Analyzes with DuckDB -> Saves Report to 'data/gold/'
    """
    print(f"\n💻 STARTING LOCAL PIPELINE.")

    try:
        # Step 1: Bronze Layer - Ingestion
        print("   [1/3] Running Bronze (Ingest).")
        raw_file = process_ingestion()
        print(f"   ✅ Bronze Complete. File: {raw_file.name}")

        # Step 2: Silver Layer - Cleaning
        print("   [2/3] Running Silver (Clean).")
        clean_file = process_cleaning()
        print(f"   ✅ Silver Complete. File: {clean_file.name}")

        # Step 3: Gold Layer - Analysis
        print("   [3/3] Running Gold (Analyze).")
        final_report = process_analysis()
        print(f"   ✅ Gold Complete. Report: {final_report.name}")

        print("🎉 Local Pipeline Finished Successfully.")

    except Exception as error:
        print(f"❌ Local Pipeline Failed: {error}")

# ==========================================
# 🎮 MAIN CONTROLLER
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto Pipeline Controller")
    parser.add_argument(
        "--mode", 
        choices=["local", "cloud", "all"], 
        default="cloud",
        help="Choose execution mode: 'local' (laptop), 'cloud' (GCP), or 'all' (both)."
    )

    args = parser.parse_args()

    print(f"🚀 EXECUTING MODE: {args.mode.upper()}")

    if args.mode in ["local", "all"]:
        run_local_pipeline()

    if args.mode in ["cloud", "all"]:
        run_cloud_pipeline()
