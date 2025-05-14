import os
import sys
import pandas as pd
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.adapters.rpc_funcs.utils import connect_to_gcs, check_gcs_connection, save_data_for_range

def test_gcs_connection():
    """Test that we can establish a GCS connection directly."""
    print("\n--- Testing GCS Connection ---")
    try:
        gcs, bucket_name = connect_to_gcs()
        print(f"GCS Connection established: {gcs is not None}")
        print(f"Bucket name: {bucket_name}")
        
        # Test if the connection is valid using check_gcs_connection
        is_valid = check_gcs_connection(gcs)
        print(f"GCS Connection is valid: {is_valid}")
        
        # Try to list buckets to further verify connection
        try:
            buckets = list(gcs.list_buckets())
            print(f"Successfully listed {len(buckets)} buckets")
            
            # Try to access the specific bucket
            bucket = gcs.bucket(bucket_name)
            blobs = list(bucket.list_blobs(max_results=5))
            print(f"Successfully listed {len(blobs)} blobs in bucket '{bucket_name}' (showing max 5)")
            for blob in blobs:
                print(f" - {blob.name}")
        except Exception as e:
            print(f"Error accessing bucket: {str(e)}")
        
        return gcs, bucket_name
    except Exception as e:
        print(f"Error establishing GCS connection: {str(e)}")
        return None, None

def test_gcs_compatibility():
    """Test that the GCS compatibility layer is working."""
    print("\n--- Testing GCS Compatibility Layer ---")
    try:
        gcs, bucket_name = connect_to_gcs()
        print(f"GCS Connection established: {gcs is not None}")
        print(f"Bucket name: {bucket_name}")
        
        is_valid = check_gcs_connection(gcs)
        print(f"GCS Connection is valid: {is_valid}")
        
        return gcs, bucket_name
    except Exception as e:
        print(f"Error using GCS compatibility layer: {str(e)}")
        return None, None

def test_save_data_for_range():
    """Test saving data to GCS using the save_data_for_range function."""
    print("\n--- Testing save_data_for_range ---")
    gcs, bucket_name = test_gcs_connection()
    if not gcs or not bucket_name:
        print("Skipping test_save_data_for_range due to connection error")
        return
    
    try:
        # Create a simple test dataframe with a specific timestamp
        specific_date = datetime(2023, 5, 15, 12, 0, 0)  # Using May 15, 2023 as a fixed date
        expected_date_str = "2023-05-15"
        
        df = pd.DataFrame({
            'block_number': [1, 2, 3],
            'tx_hash': ['0x123', '0x456', '0x789'],
            'from_address': ['0xabc', '0xdef', '0xghi'],
            'to_address': ['0x123', '0x456', '0x789'],
            'value': [1.0, 2.0, 3.0],
            'gas_price': [10, 20, 30],
            'gas_used': [100, 200, 300],
            'block_timestamp': [specific_date, specific_date, specific_date]
        })
        
        # Define test parameters
        block_start = 1
        block_end = 3
        chain = 'test_chain'
        
        # Save the data
        print(f"Saving test data for blocks {block_start} to {block_end} in chain '{chain}'")
        save_data_for_range(df, block_start, block_end, chain, bucket_name)
        
        # Verify the file was created by listing blobs with the prefix using the expected date
        prefix = f"{chain}/{expected_date_str}/{chain}_tx_{block_start}_{block_end}"
        bucket = gcs.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if blobs:
            print(f"Successfully found {len(blobs)} files with prefix '{prefix}':")
            for blob in blobs:
                print(f" - {blob.name} ({blob.size} bytes)")
                # Verify the date in the path matches our expected date
                expected_path = f"{chain}/{expected_date_str}/{chain}_tx_{block_start}_{block_end}.parquet"
                if blob.name == expected_path:
                    print(f"✅ Path matches expected format with timestamp-derived date: {expected_date_str}")
                else:
                    print(f"❌ Path doesn't match expected format: {blob.name} vs {expected_path}")
        else:
            print(f"No files found with prefix '{prefix}'")
            
        # Also test with Unix timestamp format
        print("\nTesting with Unix timestamp format:")
        unix_df = pd.DataFrame({
            'block_number': [4, 5, 6],
            'tx_hash': ['0xabc', '0xdef', '0xghi'],
            'from_address': ['0x123', '0x456', '0x789'],
            'to_address': ['0xabc', '0xdef', '0xghi'],
            'value': [4.0, 5.0, 6.0],
            'gas_price': [40, 50, 60],
            'gas_used': [400, 500, 600],
            'block_timestamp': [specific_date.timestamp(), specific_date.timestamp(), specific_date.timestamp()]
        })
        
        # Save the data with Unix timestamps
        block_start = 4
        block_end = 6
        print(f"Saving test data with Unix timestamps for blocks {block_start} to {block_end} in chain '{chain}'")
        save_data_for_range(unix_df, block_start, block_end, chain, bucket_name)
        
        # Verify file was created with the same expected date
        prefix = f"{chain}/{expected_date_str}/{chain}_tx_{block_start}_{block_end}"
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if blobs:
            print(f"Successfully found {len(blobs)} files with prefix '{prefix}':")
            for blob in blobs:
                print(f" - {blob.name} ({blob.size} bytes)")
                expected_path = f"{chain}/{expected_date_str}/{chain}_tx_{block_start}_{block_end}.parquet"
                if blob.name == expected_path:
                    print(f"✅ Path matches expected format with Unix timestamp-derived date: {expected_date_str}")
                else:
                    print(f"❌ Path doesn't match expected format: {blob.name} vs {expected_path}")
        else:
            print(f"No files found with prefix '{prefix}'")
            
    except Exception as e:
        print(f"Error in test_save_data_for_range: {str(e)}")

if __name__ == "__main__":
    print("Starting GCS Migration Tests")
    test_gcs_connection()
    test_gcs_compatibility()
    test_save_data_for_range()
    print("\nGCS Migration Tests Completed") 