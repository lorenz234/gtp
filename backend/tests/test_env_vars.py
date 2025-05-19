import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()
# Add the parent directory to sys.path to import modules from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def check_env_vars():
    print("\n--- Checking Environment Variables ---")
    
    # Check GCS_BUCKET_NAME
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    print(f"GCS_BUCKET_NAME: {'✓ Set' if bucket_name else '✗ Not set'}")
    if bucket_name:
        print(f"  Value: {bucket_name}")
    
    # Check GOOGLE_CREDENTIALS
    google_credentials = os.getenv("GOOGLE_CREDENTIALS")
    print(f"GOOGLE_CREDENTIALS: {'✓ Set' if google_credentials else '✗ Not set'}")
    
    if google_credentials:
        # Debug: Print the raw value to inspect it
        print(f"  Raw value: {repr(google_credentials)}")
        
        try:
            # Attempt to parse the JSON string
            json_google_credentials = json.loads(google_credentials)
            print(f"  Parsed JSON: {json_google_credentials}")
        except json.JSONDecodeError as e:
            print(f"  Error parsing GOOGLE_CREDENTIALS: {e}")
            print("  Ensure the GOOGLE_CREDENTIALS environment variable contains valid JSON.")
    else:
        print("  GOOGLE_CREDENTIALS is not set.")

if __name__ == "__main__":
    check_env_vars()