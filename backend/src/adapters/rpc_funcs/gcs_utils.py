import os
import json
import time
from google.cloud import storage
from google.oauth2 import service_account
import io

def connect_to_gcs():
    """
    Establishes a connection to Google Cloud Storage using credentials from environment variables.
    
    Returns:
        tuple: A tuple containing the GCS client object and the bucket name.

    Raises:
        ConnectionError: If the connection to GCS fails.
    """
    try:
        # Get the GCS bucket name from environment variables
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        
        if not bucket_name:
            raise EnvironmentError("GCS bucket name not found in environment variables.")
        
        # Parse the JSON credentials using the GOOGLE_CREDENTIALS environment variable
        credentials_json = os.getenv('GOOGLE_CREDENTIALS')
        credentials_info = json.loads(credentials_json)
        
        
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        # Create a GCS client
        gcs = storage.Client(credentials=credentials)
        
        return gcs, bucket_name
    except Exception as e:
        print("ERROR: An error occurred while connecting to GCS:", str(e))
        raise ConnectionError(f"An error occurred while connecting to GCS: {str(e)}")

def check_gcs_connection(gcs_connection):
    """
    Checks if the connection to GCS is established.
    
    Args:
        gcs_connection: The GCS connection object.

    Returns:
        bool: True if the connection is valid, False otherwise.
    """
    return gcs_connection is not None

def save_data_for_range_gcs(df, block_start, block_end, chain, bucket_name):
    """
    Saves the transaction data for a range of blocks to a GCS bucket in parquet format.
    Uses the structure: gcs_bucket_name/{chain_name}/{YYYY-MM-DD}/{file}
    
    Args:
        df (pd.DataFrame): The DataFrame containing transaction data.
        block_start (int): The starting block number.
        block_end (int): The ending block number.
        chain (str): The name of the blockchain chain.
        bucket_name (str): The name of the GCS bucket.
    """
    # Convert any 'object' dtype columns to string
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].apply(str)
            except Exception as e:
                raise e

    # Generate the filename
    filename = f"{chain}_tx_{block_start}_{block_end}.parquet"
    
    # Get the current date for folder structure
    current_date = time.strftime("%Y-%m-%d")
    
    # Create GCS file path
    file_key = f"{chain}/{current_date}/{filename}"
    
    # Connect to GCS
    gcs, _ = connect_to_gcs()
    bucket = gcs.bucket(bucket_name)
    
    # Convert DataFrame to parquet and upload to GCS
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Upload to GCS
    blob = bucket.blob(file_key)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    print(f"...saved data to GCS: {file_key}")

def upload_parquet_to_gcs(bucket_name, path_name, df):
    """
    Uploads a pandas DataFrame as a parquet file to Google Cloud Storage.
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        path_name (str): The path where the parquet file will be stored.
        df (pd.DataFrame): The DataFrame to upload.
    """
    # Connect to GCS
    gcs, _ = connect_to_gcs()
    bucket = gcs.bucket(bucket_name)
    
    # Convert DataFrame to parquet and upload to GCS
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Create the blob
    blob = bucket.blob(f"{path_name}.parquet")
    
    # Upload the parquet data
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    print(f"...uploaded to GCS: {path_name}.parquet")
