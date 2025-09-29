#!/usr/bin/env python3
"""
Real integration test for user ops with actual database and RPC connections.
This script tests the complete pipeline as it would run in a DAG.
"""

import sys
import os
from datetime import datetime

# Add the backend src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from src.adapters.rpc_funcs.utils import fetch_and_process_range, Web3CC
    from src.db_connector import DbConnector
    from src.adapters.rpc_funcs.gcs_utils import connect_to_gcs
    print("✓ Successfully imported all required modules")
except ImportError as e:
    print(f"✗ Failed to import modules: {e}")
    sys.exit(1)

def test_real_integration(chain="ethereum", block_number=None):
    """
    Test the real integration with actual database and RPC connections.
    
    Args:
        chain (str): Chain to test (default: ethereum)
        block_number (int): Specific block to test (default: latest - 10)
    """
    print(f"\n=== Testing Real Integration for {chain} ===")
    
    # 1. Set up database connection
    try:
        print("1. Setting up database connection...")
        db_connector = DbConnector()
        print("   ✓ Database connection established")
    except Exception as e:
        print(f"   ✗ Database connection failed: {e}")
        return False
    
    # 2. Set up RPC connection
    try:
        print("2. Setting up RPC connection...")
        # Use a public RPC endpoint for testing
        rpc_config = {
            'url': 'https://rpc.flashbots.net',  # Ethereum mainnet
            'workers': 1,
            'max_req': 10,
            'max_tps': 5
        }
        
        w3 = Web3CC(rpc_config)
        print(f"   ✓ Connected to RPC: {rpc_config['url']}")
        
        # Get latest block number if not specified
        if block_number is None:
            latest_block = w3.eth.block_number
            block_number = latest_block - 10  # Use a block that's 10 blocks old to ensure finality
            print(f"   ✓ Using block {block_number} (latest - 10)")
        else:
            print(f"   ✓ Using specified block {block_number}")
            
    except Exception as e:
        print(f"   ✗ RPC connection failed: {e}")
        return False
    
    # 3. Set up GCS connection (required for the function)
    try:
        print("3. Setting up GCS connection...")
        gcs_connection, bucket_name = connect_to_gcs()
        if gcs_connection:
            print(f"   ✓ GCS connected, bucket: {bucket_name}")
        else:
            print("   ! GCS connection failed, but continuing (data won't be saved to GCS)")
            bucket_name = "test-bucket"
    except Exception as e:
        print(f"   ! GCS setup failed: {e}, continuing without GCS")
        bucket_name = "test-bucket"
    
    # 4. Test the actual integration
    try:
        print(f"4. Processing block {block_number} with user ops integration...")
        
        table_name = f'{chain}_tx'
        rpc_url = rpc_config['url']
        
        # This is the actual function call that would happen in your DAG
        rows_uploaded = fetch_and_process_range(
            current_start=block_number,
            current_end=block_number,  # Process just one block
            chain=chain,
            w3=w3,
            table_name=table_name,
            bucket_name=bucket_name,
            db_connector=db_connector,
            rpc_url=rpc_url
        )
        
        print(f"   ✓ Successfully processed block {block_number}")
        print(f"   ✓ Uploaded {rows_uploaded} transaction rows")
        
    except Exception as e:
        print(f"   ✗ Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # 5. Verify the data was inserted
    try:
        print("5. Verifying data insertion...")
        
        # Check transaction data
        tx_query = f"""
        SELECT COUNT(*) as tx_count 
        FROM {table_name} 
        WHERE block_number = {block_number}
        """
        tx_result = db_connector.execute_query(tx_query, load_df=True)
        tx_count = tx_result['tx_count'].iloc[0] if not tx_result.empty else 0
        print(f"   ✓ Found {tx_count} transactions in {table_name}")
        
        # Check user ops data
        uops_query = f"""
        SELECT COUNT(*) as uops_count,
               COUNT(DISTINCT tx_hash) as unique_tx_count,
               origin_key,
               tree,
               is_decoded,
               has_bytes
        FROM uops 
        WHERE origin_key = '{chain}'
        AND tx_hash IN (
            SELECT tx_hash FROM {table_name} 
            WHERE block_number = {block_number}
        )
        GROUP BY origin_key, tree, is_decoded, has_bytes
        ORDER BY uops_count DESC
        """
        
        uops_result = db_connector.execute_query(uops_query, load_df=True)
        
        if not uops_result.empty:
            total_uops = uops_result['uops_count'].sum()
            unique_tx_with_uops = uops_result['unique_tx_count'].sum()
            print(f"   ✓ Found {total_uops} user operations from {unique_tx_with_uops} transactions")
            print("   ✓ User ops breakdown:")
            for _, row in uops_result.iterrows():
                print(f"     - {row['tree']}: {row['uops_count']} ops (decoded: {row['is_decoded']}, has_bytes: {row['has_bytes']})")
        else:
            print("   ℹ No user operations found (normal if block has no complex transactions)")
        
    except Exception as e:
        print(f"   ✗ Data verification failed: {e}")
        return False
    
    print(f"\n✓ Real integration test completed successfully!")
    print(f"✓ Block {block_number} processed with {tx_count} transactions and {total_uops if 'total_uops' in locals() else 0} user operations")
    return True

def main():
    """Main test function"""
    print("Real User Ops Integration Test")
    print("=" * 50)
    
    # You can specify a specific block number here if you want to test a particular block
    # Block 23296493 is known to have user operations (from UserOps_final.ipynb)
    test_block = 23296493  # Set to specific block number or None for auto-selection
    
    success = test_real_integration(chain="ethereum", block_number=test_block)
    
    if success:
        print("\n🎉 Integration test PASSED! Your user ops extraction is working in production!")
    else:
        print("\n❌ Integration test FAILED! Check the errors above.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
