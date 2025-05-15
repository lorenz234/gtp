import os
import sys
import pandas as pd
from datetime import datetime
import argparse
import time
import traceback
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.adapters.rpc_funcs.utils import connect_to_gcs, check_gcs_connection, connect_to_node, get_chain_config
from src.adapters.rpc_funcs.utils import fetch_data_for_range, save_data_for_range
from src.db_connector import DbConnector

def test_timestamp_to_date_extraction(start_block=15590154, batch_size=2):
    """
    Test that save_data_for_range correctly extracts the date from block timestamps.
    Gets real data from Lisk RPC nodes.
    
    Args:
        start_block: Starting block number to fetch
        batch_size: Number of blocks to process
    """
    print(f"\n===== Testing GCS Date Extraction From Real Block Timestamps =====")
    
    # Step 1: Test GCS connection
    print("\n--- Testing GCS Connection ---")
    try:
        gcs, bucket_name = connect_to_gcs()
        if not check_gcs_connection(gcs):
            print("❌ Failed to connect to GCS. Aborting test.")
            return
        print(f"✅ Successfully connected to GCS bucket: {bucket_name}")
    except Exception as e:
        print(f"❌ Error connecting to GCS: {str(e)}")
        return
    
    # Step 2: Connect to Lisk RPC and fetch real data
    print("\n--- Fetching Real Block Data from Lisk RPC ---")
    
    chain_name = "lisk"
    end_block = start_block + batch_size - 1
    
    # Connect to database to get RPC configs
    db_connector = DbConnector()
    active_rpc_configs, _ = get_chain_config(db_connector, chain_name)
    
    # Try to connect to an RPC node
    node_connection = None
    rpc_url = None
    
    for rpc_config in active_rpc_configs:
        try:
            print(f"Trying to connect to {rpc_config['url']}...")
            node_connection = connect_to_node(rpc_config)
            rpc_url = rpc_config['url']
            print(f"✅ Successfully connected to Lisk node at {rpc_url}")
            break
        except Exception as e:
            print(f"Failed to connect to {rpc_config['url']}: {str(e)}")
            continue
    
    if not node_connection:
        print("❌ Could not connect to any Lisk node. Aborting test.")
        return
    
    # Get the current time to track how long the process takes
    start_time = time.time()
    
    # Use fetch_data_for_range instead of manual implementation
    print(f"Fetching data for blocks {start_block} to {end_block}...")
    df = fetch_data_for_range(node_connection, start_block, end_block)
    
    # If no data was found, abort the test
    if df is None or df.empty:
        print("❌ No data was returned for the specified blocks. Try a different block range.")
        return
    
    # Check if we need to add a proper block_timestamp column
    if not any(col in df.columns for col in ['block_timestamp']):
        print("Adding block_timestamp column to DataFrame...")
        # Try to get the timestamp from first block for all rows
        try:
            block = node_connection.eth.get_block(start_block)
            if block and 'timestamp' in block:
                # Convert to datetime or leave as numeric
                timestamp = block['timestamp']
                print(f"Using timestamp from block: {timestamp} (type: {type(timestamp)})")
                # Add it to all rows
                df['block_timestamp'] = timestamp
        except Exception as e:
            print(f"Could not get block timestamp: {str(e)}")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Fetched data in {elapsed_time:.2f} seconds")
    
    print(f"\n✅ Created DataFrame with {len(df)} transactions")
    
    # Debug: Print column names and types
    print("\nDataFrame columns:")
    for col in df.columns:
        print(f" - {col} (Type: {df[col].dtype})")
    
    # Extract expected timestamp from first block in the dataframe
    expected_timestamp = None
    block_timestamp_col = None
    
    # Check for the various timestamp column formats
    if 'block_timestamp' in df.columns:
        block_timestamp_col = 'block_timestamp'
        print(f"Found 'block_timestamp' column for testing")
    
    if block_timestamp_col:
        # Get the timestamp of the first block
        first_block_data = df[df['blockNumber'] == start_block]
        if not first_block_data.empty:
            expected_timestamp = first_block_data[block_timestamp_col].iloc[0]
            print(f"Found timestamp for block {start_block}: {expected_timestamp} (type: {type(expected_timestamp)})")
    
    if expected_timestamp is None:
        print("❌ Could not find timestamp in the dataframe. Check the fetch_data_for_range implementation.")
        return
    
    # Get the expected date string for verification
    try:
        if isinstance(expected_timestamp, datetime):
            expected_date = expected_timestamp.strftime("%Y-%m-%d")
            print(f"Timestamp is datetime object, converted to: {expected_date}")
        elif isinstance(expected_timestamp, (int, float, np.int64, np.float64)):
            # Handle Unix timestamp, ensuring it uses the exact same conversion as the save function
            timestamp_value = float(expected_timestamp)
            expected_date = datetime.fromtimestamp(timestamp_value).strftime("%Y-%m-%d")
            print(f"Timestamp is numeric ({type(expected_timestamp)}), converted {timestamp_value} to: {expected_date}")
        elif isinstance(expected_timestamp, str):
            # Try to parse string timestamp
            try:
                expected_date = datetime.fromisoformat(expected_timestamp).strftime("%Y-%m-%d")
                print(f"Timestamp is string (ISO format), converted to: {expected_date}")
            except:
                try:
                    # Try alternate format
                    expected_date = datetime.strptime(expected_timestamp, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                    print(f"Timestamp is string (standard format), converted to: {expected_date}")
                except:
                    # Fallback to current date
                    expected_date = time.strftime("%Y-%m-%d")
                    print(f"Couldn't parse string timestamp, using current date: {expected_date}")
        else:
            # Fallback to current date
            expected_date = time.strftime("%Y-%m-%d")
            print(f"Unknown timestamp type, using current date: {expected_date}")
    except Exception as e:
        print(f"Error processing timestamp: {str(e)}")
        expected_date = time.strftime("%Y-%m-%d")
        print(f"Using current date due to error: {expected_date}")
    
    print(f"\nFirst block timestamp: {expected_timestamp}")
    print(f"Expected date in GCS path: {expected_date}")
    
    # Step 3: Save data to GCS
    print("\n--- Saving Data to GCS ---")
    try:
        # Call the function we're testing
        save_data_for_range(df, start_block, end_block, chain_name, bucket_name)
        print(f"✅ Data saved to GCS")
        
        # Step 4: Verify the file was created with the correct date
        print("\n--- Verifying File Path ---")
        expected_path = f"{chain_name}/{expected_date}/{chain_name}_tx_{start_block}_{end_block}"
        bucket = gcs.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=expected_path))
        
        if blobs:
            print(f"✅ Successfully found {len(blobs)} files with prefix '{expected_path}'")
            for blob in blobs:
                print(f" - {blob.name} ({blob.size} bytes)")
                
                # Verify exact path match
                expected_full_path = f"{chain_name}/{expected_date}/{chain_name}_tx_{start_block}_{end_block}.parquet"
                if blob.name == expected_full_path:
                    print(f"✅ PASSED: Path matches expected format with timestamp-derived date")
                    print(f"   Expected: {expected_full_path}")
                    print(f"   Actual:   {blob.name}")
                else:
                    print(f"❌ FAILED: Path doesn't match expected format")
                    print(f"   Expected: {expected_full_path}")
                    print(f"   Actual:   {blob.name}")
        else:
            print(f"❌ FAILED: No files found with prefix '{expected_path}'")
            
            # Check if file was saved with current date instead
            current_date = time.strftime("%Y-%m-%d")
            alternate_path = f"{chain_name}/{current_date}"
            
            alt_blobs = list(bucket.list_blobs(prefix=alternate_path))
            if alt_blobs:
                print(f"⚠️ Found files with current date instead of timestamp date:")
                for blob in alt_blobs[:3]:  # Show up to 3 files
                    print(f" - {blob.name}")
                print("This suggests save_data_for_range is not using the timestamp from the dataframe.")
            
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")
        traceback.print_exc()
    
    print("\n===== Test Completed =====")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test GCS timestamp to date extraction with real RPC data')
    parser.add_argument('--start-block', type=int, default=15590154, help='Starting block number to fetch')
    parser.add_argument('--batch-size', type=int, default=2, help='Number of blocks to fetch')
    
    args = parser.parse_args()
    
    test_timestamp_to_date_extraction(args.start_block, args.batch_size) 