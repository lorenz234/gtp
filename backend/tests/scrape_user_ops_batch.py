#!/usr/bin/env python3
"""
Batch scraper for user ops across multiple chains and block ranges.
This script processes the requested block ranges for final verification.
"""

import sys
import os
from datetime import datetime
import time
import pandas as pd

# Suppress pandas output to prevent massive terminal spam
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 5)
pd.set_option('display.max_colwidth', 50)

# Add the backend src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from src.adapters.rpc_funcs.utils import fetch_and_process_range, Web3CC, load_4bytes_data
    from src.db_connector import DbConnector
    from src.adapters.rpc_funcs.gcs_utils import connect_to_gcs
    print("✓ Successfully imported all required modules")
except ImportError as e:
    print(f"✗ Failed to import modules: {e}")
    sys.exit(1)

# Chain configurations with their RPC endpoints
CHAIN_CONFIGS = {
    'ethereum': {
        'rpc_url': 'https://rpc.flashbots.net',
        'workers': 2,
        'max_req': 15,
        'max_tps': 8
    },
    'base': {
        'rpc_url': 'https://base-rpc.publicnode.com',
        'workers': 1,
        'max_req': 10,
        'max_tps': 5
    },
    'worldchain': {
        'rpc_url': 'https://worldchain-mainnet.g.alchemy.com/public',
        'workers': 1,
        'max_req': 10,
        'max_tps': 5
    },
    'optimism': {
        'rpc_url': 'https://mainnet.optimism.io',
        'workers': 2,
        'max_req': 15,
        'max_tps': 8
    }
}

def setup_connections():
    """Set up database connection"""
    # Database connection
    try:
        print("Setting up database connection...")
        db_connector = DbConnector()
        print("   ✓ Database connection established")
    except Exception as e:
        print(f"   ✗ Database connection failed: {e}")
        return None, None, None
    
    return db_connector

def process_chain_range(chain, start_block, end_block, db_connector):
    """Process a block range for a specific chain"""
    print(f"\n{'='*60}")
    print(f"🔄 Processing {chain.upper()}: blocks {start_block} to {end_block}")
    print(f"{'='*60}")
    
    # Get chain configuration
    if chain not in CHAIN_CONFIGS:
        print(f"   ✗ Unknown chain: {chain}")
        return False
    
    config = CHAIN_CONFIGS[chain]
    
    # Set up RPC connection
    try:
        print(f"1. Connecting to {chain} RPC...")
        rpc_config = {
            'url': config['rpc_url'],
            'workers': config['workers'],
            'max_req': config['max_req'],
            'max_tps': config['max_tps']
        }
        
        w3 = Web3CC(rpc_config)
        print(f"   ✓ Connected to {chain} RPC: {config['rpc_url']}")
        
        # Verify connection by getting latest block
        latest_block = w3.eth.block_number
        print(f"   ✓ Latest block: {latest_block}")
        
    except Exception as e:
        print(f"   ✗ RPC connection failed: {e}")
        return False
    
    df_4bytes = load_4bytes_data()
    
    # Process the block range in smaller chunks to avoid timeouts
    chunk_size = 25  # Process 25 blocks at a time to avoid rate limits
    blocks_processed = 0
    total_blocks = end_block - start_block + 1
    total_transactions = 0
    total_user_ops = 0
    
    try:
        print(f"2. Processing {total_blocks} blocks in chunks of {chunk_size}...")
        table_name = f'{chain}_tx'
        
        start_time = time.time()
        
        # Process in chunks
        for chunk_start in range(start_block, end_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_block)
            
            print(f"   Processing chunk: blocks {chunk_start} to {chunk_end}")
            
            try:
                # Process the chunk
                rows_uploaded = fetch_and_process_range(
                    current_start=chunk_start,
                    current_end=chunk_end,
                    chain=chain,
                    w3=w3,
                    table_name=table_name,
                    bucket_name=None,
                    db_connector=db_connector,
                    rpc_url=config['rpc_url'],
                    df_4bytes=df_4bytes
                )
                
                blocks_in_chunk = chunk_end - chunk_start + 1
                blocks_processed += blocks_in_chunk
                total_transactions += rows_uploaded
                
                print(f"   ✓ Chunk complete: {rows_uploaded} transactions from {blocks_in_chunk} blocks")
                
                # Small delay between chunks to be nice to RPCs
                if chunk_end < end_block:
                    time.sleep(2)
                    
            except Exception as e:
                print(f"   ✗ Chunk failed (blocks {chunk_start}-{chunk_end}): {e}")
                # Continue with next chunk
                continue
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"   ✓ Successfully processed {blocks_processed}/{total_blocks} blocks")
        print(f"   ✓ Total transactions: {total_transactions}")
        print(f"   ✓ Processing time: {processing_time:.1f}s")
        
    except Exception as e:
        print(f"   ✗ Processing failed: {e}")
        return False
    
    # Verify the data was inserted
    try:
        print("3. Verifying data insertion...")
        
        # Check transaction data
        tx_query = f"""
        SELECT COUNT(*) as tx_count,
               MIN(block_number) as min_block,
               MAX(block_number) as max_block
        FROM {table_name} 
        WHERE block_number BETWEEN {start_block} AND {end_block}
        """
        tx_result = db_connector.execute_query(tx_query, load_df=True)
        
        if not tx_result.empty:
            tx_count = tx_result['tx_count'].iloc[0]
            min_block = tx_result['min_block'].iloc[0]
            max_block = tx_result['max_block'].iloc[0]
            print(f"   ✓ Found {tx_count} transactions")
            print(f"   ✓ Block range in DB: {min_block} to {max_block}")
        else:
            print("   ⚠ No transaction data found")
            tx_count = 0
        
        # Check user ops data
        uops_query = f"""
        SELECT COUNT(*) as uops_count,
               COUNT(DISTINCT tx_hash) as unique_tx_count,
               COUNT(DISTINCT block_number) as unique_blocks
        FROM uops 
        WHERE origin_key = '{chain}'
        AND block_number BETWEEN {start_block} AND {end_block}
        """
        
        uops_result = db_connector.execute_query(uops_query, load_df=True)
        
        if not uops_result.empty:
            total_uops = uops_result['uops_count'].iloc[0]
            unique_tx_with_uops = uops_result['unique_tx_count'].iloc[0]
            unique_blocks = uops_result['unique_blocks'].iloc[0]
            print(f"   ✓ Found {total_uops} user operations")
            print(f"   ✓ From {unique_tx_with_uops} transactions across {unique_blocks} blocks")
        else:
            print("   ℹ No user operations found (normal if no complex transactions)")
            total_uops = 0
        
        print(f"\n✅ {chain.upper()} COMPLETE: {tx_count} transactions, {total_uops} user ops")
        return True
        
    except Exception as e:
        print(f"   ✗ Data verification failed: {e}")
        return False

def main():
    """Main batch processing function"""
    print("🚀 BATCH USER OPS SCRAPER")
    print("=" * 60)
    print("Processing requested block ranges for final verification")
    print("=" * 60)
    
    # Define the requested ranges
    ranges_to_process = [
        ('ethereum', 23296496, 23296545),    # 50 blocks
        ('base', 35140166, 35140215),        # 50 blocks  
        ('worldchain', 18868253, 18868302),  # 50 blocks
        ('optimism', 140738168, 140738267)   # 100 blocks
    ]
    
    # Setup shared connections
    db_connector = setup_connections()
    if not db_connector:
        print("❌ Failed to set up connections")
        return False
    
    # Process each chain
    results = {}
    total_start_time = time.time()
    
    for chain, start_block, end_block in ranges_to_process:
        success = process_chain_range(chain, start_block, end_block, db_connector)
        results[chain] = success
        
        if not success:
            print(f"❌ Failed to process {chain}")
        
        # Delay between chains to be nice to RPCs
        if chain != ranges_to_process[-1][0]:  # Not the last one
            print("   ⏸ Waiting 10 seconds before next chain...")
            time.sleep(10)
    
    total_time = time.time() - total_start_time
    
    # Final summary
    print(f"\n{'='*60}")
    print("📊 FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Total processing time: {total_time:.1f} seconds")
    
    successful_chains = sum(1 for success in results.values() if success)
    total_chains = len(results)
    
    for chain, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{chain.ljust(12)}: {status}")
    
    if successful_chains == total_chains:
        print(f"\n🎉 ALL {total_chains} CHAINS PROCESSED SUCCESSFULLY!")
        print("Ready for final data verification! 🚀")
    else:
        print(f"\n⚠️  {successful_chains}/{total_chains} chains successful")
        print("Check the errors above for failed chains.")
    
    return successful_chains == total_chains

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
