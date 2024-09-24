import json
import requests

# Step 1: Download the master.json (manifest.json)
manifest_url = "https://export.sourcify.dev/manifest.json"
manifest_file = "master.json"

print(f"Downloading {manifest_url}...")

# Download the manifest file
response = requests.get(manifest_url)
if response.status_code == 200:
    with open(manifest_file, 'wb') as f:
        f.write(response.content)
    print(f"{manifest_file} downloaded successfully.")
else:
    print(f"Failed to download {manifest_file}. Status code: {response.status_code}")
    exit()

# Step 2: Load the JSON data from the downloaded master.json
with open(manifest_file) as f:
    data = json.load(f)

# URL base
base_url = "https://export.sourcify.dev/"

# Step 3: Extract the 'contract_deployments' files
contract_deployments_files = data["files"]["contract_deployments"]

# Step 4: Download each 'contract_deployments' file
for file_path in contract_deployments_files:
    file_url = base_url + file_path
    file_name = file_path.split('/')[-1]  # Extracting the file name from the path
    print(f"Downloading {file_url}...")
    
    # Download the file
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(file_name, 'wb') as f:
            f.write(response.content)
        print(f"{file_name} downloaded successfully.")
    else:
        print(f"Failed to download {file_name}. Status code: {response.status_code}")
