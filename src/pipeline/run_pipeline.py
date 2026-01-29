import sys
import subprocess
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import shutil

# --- CONFIGURATION ---
FUNCTION_URL = "https://cdp-bronze-ingest-v2-ckqzjlvcxa-uc.a.run.app" 

def get_gcloud_token():
    """
    Fallback Authentication: Uses the local 'gcloud' CLI.
    
    Why:
    The standard Python Auth library often restricts User Credentials (like local 
    'gcloud auth login' sessions) from generating OIDC Identity Tokens. 
    The CLI tool bypasses this restriction for local development.

    Returns:
        str: A valid OIDC Identity Token.
        None: If the gcloud tool is not installed or the command fails.
    """
    if not shutil.which("gcloud"):
        print("❌ Error: 'gcloud' CLI not found on PATH.")
        return None

    try:
        # Run the command: gcloud auth print-identity-token
        token = subprocess.check_output(
            ["gcloud", "auth", "print-identity-token"],
            text=True
        ).strip()
        return token
    except subprocess.CalledProcessError as error:
        print(f"❌ gcloud Auth Failed: {error}")
        return None

def get_id_token(url):
    """
    Primary Authentication: Generates a Google OIDC token.

    Strategy:
    1. Try the official 'google-auth' library first.
       - Best for Production (Cloud Run, Compute Engine, etc).
    2. Fallback to 'gcloud' CLI if the library fails.
       - Best for Local Development (Laptop).

    Args:
        url (str): The target URL (Audience) for the token.

    Returns:
        str: A valid Bearer token for the Authorization header.
    """
    try:
        # 1. Try the Official Python Way (Best for Production/Servers)
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        return token
    except Exception:
        # 2. Fallback to CLI (Best for Local/Laptop)
        print("⚠️  Python Auth failed (normal for local users). Switching to gcloud CLI.")
        return get_gcloud_token()

def trigger_cloud_pipeline():
    """
    The 'Remote Control' for the Cloud Data Lakehouse.

    This function acts as the external trigger for the Event-Driven Architecture.
    It performs the following client-side orchestration:
    1. Authenticates: Uses a Hybrid Strategy (Library -> CLI Fallback).
    2. Requests: Sends a POST request to the Bronze Layer (Ingestion Endpoint).
    3. Validates: Checks the HTTP response to ensure the pipeline started.

    Note:
    Once triggered, the pipeline runs asynchronously in the cloud:
    Bronze (Ingest) -> Silver (Clean) -> Gold (Financial Analysis).
    """
    print(f"🚀 Connecting to Cloud Pipeline.")
    print(f"🔗 Target: {FUNCTION_URL}")

    # 1. Authentication
    print("🔑 Authenticating.")
    token = get_id_token(FUNCTION_URL)

    if not token:
        print("❌ Critical Auth Failure. Could not generate token.")
        sys.exit(1)

    # 2. The Request
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        print("📡 Sending Trigger.")
        response = requests.post(FUNCTION_URL, headers=headers, json={})

        # 3. Handle Response
        if response.status_code == 200:
            print("\n✅ SUCCESS! Cloud Pipeline Triggered.")
            print("--------------------------------------------------")
            print(f"RESPONSE: {response.text}")
            print("--------------------------------------------------")
            print("💡 Monitor the 'Gold' logs in ~60 seconds for results.")
        else:
            print(f"\n❌ FAILED. Status Code: {response.status_code}")
            print(f"Details: {response.text}")

    except Exception as error:
        print(f"\n❌ Network Error: {error}")

if __name__ == "__main__":
    if "PLACEHOLDER" in FUNCTION_URL:
        print("⚠️  PLEASE UPDATE 'FUNCTION_URL' IN THE SCRIPT FIRST!")
    else:
        trigger_cloud_pipeline()
