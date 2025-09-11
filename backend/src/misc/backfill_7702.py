#!/usr/bin/env python3
"""
EIP-7702 Authorization List Backfill Script

Multi-threaded backfill of 7702 authorization data for all type 4 transactions.
Uses parallel processing to handle multiple chains simultaneously with proper
error handling and progress tracking.

Usage:
    python backfill_7702.py [--chain CHAIN] [--dry-run] [--workers N]
"""

import sys
import getpass
import argparse
import json
import os
from datetime import datetime, date
import pandas as pd
from sqlalchemy import text
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import queue

# Add backend path
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from src.db_connector import DbConnector
from src.adapters.rpc_funcs.utils import (
    extract_authorization_list, 
    process_authorization_list_data,
    get_chain_config,
    connect_to_node
)

# Progress tracking
PROGRESS_FILE = "backfill_7702_progress.json"

# Thread-safe progress tracking
progress_lock = threading.Lock()
progress_bars = {}  # Per-chain progress bars

def load_progress():
    """Load progress from JSON file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load progress file: {e}")
    return {}

def save_progress(progress_data):
    """Save progress to JSON file."""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress_data, f, indent=2, default=str)
    except Exception as e:
        print(f"Warning: Could not save progress: {e}")

def update_chain_progress(chain, latest_block, latest_date, total_processed, status='in_progress'):
    """Update progress for a specific chain - thread safe."""
    with progress_lock:
        progress = load_progress()
        
        if 'chains' not in progress:
            progress['chains'] = {}
        
        progress['chains'][chain] = {
            'latest_block_processed': latest_block,
            'latest_date_processed': str(latest_date),  # Ensure it's a string
            'total_transactions_processed': total_processed,
            'last_updated': datetime.now().isoformat(),
            'status': status  # in_progress, completed, failed, skipped
        }
        
        progress['last_run'] = datetime.now().isoformat()
        
        save_progress(progress)

def get_chain_resume_block(chain, min_block):
    """Get the block to resume from for a chain."""
    progress = load_progress()
    
    if 'chains' in progress and chain in progress['chains']:
        resume_block = progress['chains'][chain].get('latest_block_processed', min_block - 1) + 1
        print(f"   📂 Resuming {chain} from block {resume_block} (previous progress found)")
        return resume_block
    else:
        print(f"   🆕 Starting {chain} from block {min_block} (no previous progress)")
        return min_block


def check_table_schema(db_connector, table_name):
    """Check if a table has the required tx_type column."""
    try:
        with db_connector.engine.connect() as conn:
            schema_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
            AND column_name = 'tx_type'
            """
            result = conn.execute(text(schema_query), {"table_name": table_name}).fetchone()
            return result is not None
    except Exception as e:
        print(f"❌ Error checking schema for {table_name}: {e}")
        return False

def get_all_tx_tables(db_connector):
    """Get all transaction tables in the database."""
    with db_connector.engine.connect() as conn:
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%_tx' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        tables = [row[0] for row in conn.execute(text(tables_query))]
        return tables

def get_chains_from_tables(tables):
    """Extract chain names from table names."""
    return [table.replace('_tx', '') for table in tables]

def get_chains_with_type4_txs(db_connector, start_date='2024-04-01'):
    """Get all chains that have type 4 transactions since the start date, with schema validation."""
    chains_with_type4 = []
    skipped_chains = []
    
    # Get all transaction tables
    tables = get_all_tx_tables(db_connector)
    print(f"Checking {len(tables)} transaction tables for type 4 transactions...")
    
    with db_connector.engine.connect() as conn:
        for table in tables:
            chain_name = table.replace('_tx', '')
            
            # First check if table has tx_type column
            if not check_table_schema(db_connector, table):
                print(f"⚠️  Skipping {chain_name}: missing tx_type column")
                skipped_chains.append(chain_name)
                continue
            
            try:
                # Check if table has type 4 transactions
                check_query = f"""
                SELECT COUNT(*) 
                FROM {table} 
                WHERE tx_type = '4' 
                AND block_date >= :start_date
                LIMIT 1
                """
                result = conn.execute(text(check_query), {"start_date": start_date}).scalar()
                
                if result > 0:
                    chains_with_type4.append(chain_name)
                    print(f"✅ Found type 4 transactions in {chain_name}")
                else:
                    print(f"⏭️  No type 4 transactions in {chain_name}")
                    
            except Exception as e:
                print(f"❌ Error checking {table}: {e}")
                skipped_chains.append(chain_name)
                continue
    
    if skipped_chains:
        print(f"\n⚠️  Skipped {len(skipped_chains)} chains due to schema issues: {skipped_chains}")
    
    return chains_with_type4


def get_overall_block_range_for_chain(db_connector, chain, start_date='2024-04-01'):
    """Get the overall block range for a chain (not just type 4 transactions)."""
    table_name = f"{chain}_tx"
    
    query = f"""
    SELECT 
        MIN(block_number) as min_block,
        MAX(block_number) as max_block,
        MIN(block_date) as min_date,
        MAX(block_date) as max_date
    FROM {table_name}
    WHERE block_date >= :start_date
    """
    
    try:
        with db_connector.engine.connect() as conn:
            result = pd.read_sql(text(query), conn, params={"start_date": start_date})
            if result.iloc[0]['min_block'] is None:
                return None, None, None, None
            return (
                int(result.iloc[0]['min_block']), 
                int(result.iloc[0]['max_block']), 
                result.iloc[0]['min_date'],
                result.iloc[0]['max_date']
            )
    except Exception as e:
        print(f"Error getting block range for {chain}: {e}")
        return None, None, None, None

def check_type4_exists_in_range(db_connector, chain, start_block, end_block):
    """Quick check if any type 4 transactions exist in the block range."""
    table_name = f"{chain}_tx"
    
    query = f"""
    SELECT COUNT(*) as count
    FROM {table_name}
    WHERE tx_type = '4'
    AND block_number >= :start_block
    AND block_number <= :end_block
    LIMIT 1
    """
    
    try:
        with db_connector.engine.connect() as conn:
            result = conn.execute(text(query), {
                "start_block": start_block,
                "end_block": end_block
            }).scalar()
            return result > 0
    except Exception as e:
        print(f"Error checking type 4 transactions for blocks {start_block}-{end_block}: {e}")
        return False

def get_type4_transactions_in_block_range(db_connector, chain, start_block, end_block):
    """Get type 4 transactions for a specific chain in a block range."""
    table_name = f"{chain}_tx"
    
    query = f"""
    SELECT 
        tx_hash,
        block_number,
        block_timestamp,
        block_date
    FROM {table_name}
    WHERE tx_type = '4'
    AND block_number >= :start_block
    AND block_number <= :end_block
    ORDER BY block_number
    """
    
    try:
        with db_connector.engine.connect() as conn:
            result = pd.read_sql(text(query), conn, params={
                "start_block": start_block,
                "end_block": end_block
            })
            return result
    except Exception as e:
        print(f"Error getting type 4 transactions for blocks {start_block}-{end_block}: {e}")
        return pd.DataFrame()

def calculate_progress(current_block, min_block, max_block, current_date, min_date, max_date):
    """Calculate progress by blocks and dates."""
    if max_block == min_block:
        block_progress = 100.0
    else:
        block_progress = ((current_block - min_block) / (max_block - min_block)) * 100
    
    if max_date == min_date:
        days_total = 1
        days_remaining = 0
    else:
        days_total = (max_date - min_date).days + 1
        days_remaining = (max_date - current_date).days
    
    return block_progress, days_total, max(0, days_remaining)


def check_existing_auth_data(db_connector, tx_hashes):
    """Check which transactions already have authorization data."""
    if not tx_hashes:
        return set()
    
    # Convert to hex format for comparison
    hex_hashes = []
    for tx_hash in tx_hashes:
        if isinstance(tx_hash, bytes):
            hex_hashes.append(tx_hash.hex())
        elif isinstance(tx_hash, str):
            if tx_hash.startswith('\\x'):
                hex_hashes.append(tx_hash[2:])
            elif tx_hash.startswith('0x'):
                hex_hashes.append(tx_hash[2:])
            else:
                hex_hashes.append(tx_hash)
    
    if not hex_hashes:
        return set()
    
    placeholders = ','.join([':hash' + str(i) for i in range(len(hex_hashes))])
    query = f"""
    SELECT DISTINCT encode(tx_hash, 'hex') as tx_hash_hex
    FROM authorizations_7702
    WHERE encode(tx_hash, 'hex') IN ({placeholders})
    """
    
    # Create parameter dict
    params = {f'hash{i}': hex_hash for i, hex_hash in enumerate(hex_hashes)}
    
    try:
        with db_connector.engine.connect() as conn:
            result = conn.execute(text(query), params)
            return {row[0] for row in result}
    except Exception as e:
        print(f"Error checking existing auth data: {e}")
        return set()


def process_chain_transactions(db_connector, chain, w3, transactions, dry_run=False):
    """Process transactions for a specific chain to extract authorization lists directly from DB tx_hashes."""
    if transactions.empty:
        print(f"No transactions to process for {chain}")
        return 0
    
    print(f"Processing {len(transactions)} transactions for {chain}")
    
    auth_data_list = []
    processed_count = 0
    
    # Process transactions in batches
    batch_size = 50  # Process 50 transactions at a time
    for i in range(0, len(transactions), batch_size):
        batch_transactions = transactions.iloc[i:i + batch_size]
        print(f"  Processing transaction batch {i//batch_size + 1}: {len(batch_transactions)} transactions")
        
        # Process each transaction directly using its hash from DB
        batch_tx_details = []
        for idx, (_, tx_row) in enumerate(batch_transactions.iterrows()):
            try:
                # Convert memoryview to hex string if needed
                tx_hash = tx_row['tx_hash']
                if isinstance(tx_hash, memoryview):
                    tx_hash_hex = '0x' + tx_hash.tobytes().hex()
                else:
                    tx_hash_hex = str(tx_hash)
                
                # Get transaction details from RPC using the DB hash
                tx_details = w3.eth.get_transaction(tx_hash_hex)
                
                # Verify it's a type 4 transaction
                if tx_details.get('type') == 4:
                    # Check for authorization list
                    if 'authorizationList' in tx_details and tx_details['authorizationList']:
                        # Convert to dict and add to batch
                        tx_dict = dict(tx_details)
                        tx_dict['block_timestamp'] = tx_row['block_timestamp']
                        tx_dict['block_date'] = tx_row['block_date']
                        tx_dict['block_number'] = tx_row['block_number']
                        batch_tx_details.append(tx_dict)
                    
            except Exception as e:
                print(f"      ❌ Error fetching transaction {tx_hash_hex}: {e}")
                continue
        
        # Process authorization lists for this batch
        if batch_tx_details:
            try:
                # Extract authorization lists using existing function
                # We need to provide block_timestamp, block_date, and block_number
                # Since we have multiple blocks, we'll process each transaction individually
                for tx_detail in batch_tx_details:
                    auth_df = extract_authorization_list(
                        [tx_detail], 
                        tx_detail['block_timestamp'], 
                        tx_detail['block_date'], 
                        tx_detail['block_number']
                    )
                    
                    if not auth_df.empty:
                        auth_data_list.append(auth_df)
                        processed_count += 1
                        print(f"      ✅ Extracted {len(auth_df)} authorization records")
                    else:
                        print(f"      ⚠️  No authorization data extracted")
                        
            except Exception as e:
                print(f"    ❌ Error processing authorization lists: {e}")
                continue
        else:
            print(f"    ⚠️  No transactions with authorization lists in this batch")
    
    # Combine all authorization data
    if auth_data_list:
        all_auth_data = pd.concat(auth_data_list, ignore_index=True)
        
        if not dry_run:
            # Process and insert authorization data
            processed_auth_data = process_authorization_list_data(all_auth_data, chain)
            
            # Prepare data for database insertion (same as utils.py)
            processed_auth_data.drop_duplicates(subset=['tx_hash', 'tx_index'], inplace=True)
            processed_auth_data.set_index(['tx_hash', 'tx_index'], inplace=True)
            
            # Insert into database using db_connector (same as utils.py)
            db_connector.upsert_table('authorizations_7702', processed_auth_data, if_exists='update')
            print(f"  📝 Inserted {len(processed_auth_data)} authorization records into database")
            
        return len(all_auth_data)
    else:
        print(f"⚠️  No authorization lists found in {len(transactions)} type 4 transactions")
        return 0




def test_block_range(db_connector, chain, start_block, end_block):
    """Test a specific block range for type 4 transactions."""
    table_name = f"{chain}_tx"
    
    print(f"\n🔍 Testing {chain} blocks {start_block} to {end_block} for type 4 transactions...")
    
    try:
        with db_connector.engine.connect() as conn:
            # Check all transaction types in this range
            all_types_query = f"""
            SELECT 
                tx_type,
                COUNT(*) as count,
                MIN(block_number) as min_block,
                MAX(block_number) as max_block
            FROM {table_name}
            WHERE block_number >= :start_block
            AND block_number <= :end_block
            GROUP BY tx_type
            ORDER BY tx_type
            """
            
            all_types = pd.read_sql(text(all_types_query), conn, params={
                "start_block": start_block,
                "end_block": end_block
            })
            
            print(f"📊 Transaction types found in blocks {start_block}-{end_block}:")
            if all_types.empty:
                print("   ❌ No transactions found in this block range!")
                return
            
            for _, row in all_types.iterrows():
                print(f"   Type {row['tx_type']}: {row['count']} transactions (blocks {row['min_block']}-{row['max_block']})")
            
            # Specifically check type 4 transactions
            type4_query = f"""
            SELECT 
                block_number,
                tx_hash,
                block_date,
                block_timestamp
            FROM {table_name}
            WHERE tx_type = '4'
            AND block_number >= :start_block
            AND block_number <= :end_block
            ORDER BY block_number
            LIMIT 10
            """
            
            type4_txs = pd.read_sql(text(type4_query), conn, params={
                "start_block": start_block,
                "end_block": end_block
            })
            
            if not type4_txs.empty:
                print(f"\n✅ Found {len(type4_txs)} type 4 transactions (showing first 10):")
                for _, tx in type4_txs.iterrows():
                    # Convert memoryview to hex string
                    tx_hash = tx['tx_hash']
                    if isinstance(tx_hash, memoryview):
                        tx_hash = '0x' + tx_hash.tobytes().hex()
                    print(f"   Block {tx['block_number']}: {tx_hash} ({tx['block_date']})")
            else:
                print(f"\n❌ No type 4 transactions found in blocks {start_block}-{end_block}")
                
    except Exception as e:
        print(f"❌ Error testing block range: {e}")

def process_single_chain(chain, db_connector, start_date, block_batch_size, dry_run):
    """Process a single chain - designed to run in a thread."""
    try:
        print(f"\n🚀 Worker started for chain: {chain}")
        
        # Get overall block range for this chain
        min_block, max_block, min_date, max_date = get_overall_block_range_for_chain(db_connector, chain, start_date)
        
        if min_block is None:
            print(f"   ❌ No data found for {chain} since {start_date}")
            update_chain_progress(chain, 0, datetime.now().date(), 0, 'skipped')
            return {'chain': chain, 'status': 'skipped', 'processed': 0, 'error': 'No data'}
        
        total_blocks = max_block - min_block + 1
        print(f"   📊 {chain}: blocks {min_block} to {max_block} ({total_blocks:,} blocks)")
        
        # Get Web3 connection
        try:
            active_rpc_configs, _ = get_chain_config(db_connector, chain)
            
            if not active_rpc_configs:
                print(f"   ❌ No RPC config found for {chain}")
                update_chain_progress(chain, 0, datetime.now().date(), 0, 'failed')
                return {'chain': chain, 'status': 'failed', 'processed': 0, 'error': 'No RPC config'}
            
            rpc_config = active_rpc_configs[0]
            w3 = connect_to_node(rpc_config)
            if not w3 or not w3.is_connected():
                print(f"   ❌ Could not connect to {chain} RPC")
                update_chain_progress(chain, 0, datetime.now().date(), 0, 'failed')
                return {'chain': chain, 'status': 'failed', 'processed': 0, 'error': 'RPC connection failed'}
            
            print(f"   ✅ Connected to {chain} RPC")
            
        except Exception as e:
            print(f"   ❌ Error connecting to {chain}: {e}")
            update_chain_progress(chain, 0, datetime.now().date(), 0, 'failed')
            return {'chain': chain, 'status': 'failed', 'processed': 0, 'error': str(e)}
        
        # Check if we can resume from previous progress
        current_block = get_chain_resume_block(chain, min_block)
        chain_processed = 0
        
        # Create progress bar for this chain
        progress_desc = f"{chain:>15}"
        total_iterations = (max_block - current_block + block_batch_size) // block_batch_size
        
        with tqdm(total=total_iterations, desc=progress_desc, position=None, leave=True, 
                 bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
            
            batch_num = 1
            while current_block <= max_block:
                batch_end = min(current_block + block_batch_size - 1, max_block)
                
                # Quick check if this range has any type 4 transactions
                if not check_type4_exists_in_range(db_connector, chain, current_block, batch_end):
                    # Update progress even for empty batches
                    update_chain_progress(chain, batch_end, datetime.now().date(), chain_processed)
                    current_block = batch_end + 1
                    pbar.update(1)
                    continue
                
                # Get transactions for this block batch
                transactions = get_type4_transactions_in_block_range(db_connector, chain, current_block, batch_end)
                
                if not transactions.empty:
                    # Check existing authorization data for this batch
                    tx_hashes = []
                    for tx_hash in transactions['tx_hash'].tolist():
                        if isinstance(tx_hash, memoryview):
                            tx_hashes.append('0x' + tx_hash.tobytes().hex())
                        else:
                            tx_hashes.append(str(tx_hash))
                    existing_auth = check_existing_auth_data(db_connector, tx_hashes)
                    
                    if existing_auth:
                        # Filter out transactions with existing auth data
                        transactions = transactions[~transactions['tx_hash'].astype(str).str.replace('\\x', '').str.replace('0x', '').str.lower().isin(
                            [h.lower() for h in existing_auth]
                        )]
                    
                    if not transactions.empty:
                        # Get the latest date from this batch for progress tracking
                        batch_latest_date = transactions['block_date'].max()
                        
                        # Process this batch
                        batch_processed = process_chain_transactions(db_connector, chain, w3, transactions, dry_run)
                        
                        if batch_processed > 0:
                            chain_processed += batch_processed
                        
                        # Save progress after processing batch
                        update_chain_progress(chain, batch_end, batch_latest_date, chain_processed)
                    else:
                        # All transactions already processed
                        update_chain_progress(chain, batch_end, datetime.now().date(), chain_processed)
                
                current_block = batch_end + 1
                pbar.update(1)
                batch_num += 1
        
        print(f"   ✅ Completed {chain}: {chain_processed} total transactions processed")
        update_chain_progress(chain, max_block, max_date, chain_processed, 'completed')
        return {'chain': chain, 'status': 'completed', 'processed': chain_processed, 'error': None}
        
    except Exception as e:
        print(f"   ❌ Error processing {chain}: {e}")
        update_chain_progress(chain, 0, datetime.now().date(), 0, 'failed')
        return {'chain': chain, 'status': 'failed', 'processed': 0, 'error': str(e)}

def show_progress():
    """Show current progress from the progress file."""
    progress = load_progress()
    
    if not progress or 'chains' not in progress:
        print("No progress file found or no chains processed yet.")
        return
    
    print(f"\n📊 Current Progress (last run: {progress.get('last_run', 'unknown')})")
    print("=" * 80)
    
    for chain, data in progress['chains'].items():
        status = data.get('status', 'unknown')
        status_emoji = {'completed': '✅', 'in_progress': '🔄', 'failed': '❌', 'skipped': '⏭️'}.get(status, '❓')
        
        print(f"\n🔗 {chain.upper()} {status_emoji} ({status}):")
        print(f"   Latest block processed: {data.get('latest_block_processed', 'unknown')}")
        print(f"   Latest date processed:  {data.get('latest_date_processed', 'unknown')}")
        print(f"   Total transactions:     {data.get('total_transactions_processed', 0)}")
        print(f"   Last updated:           {data.get('last_updated', 'unknown')}")

def main():
    parser = argparse.ArgumentParser(description='Multi-threaded EIP-7702 backfill script with parallel processing')
    parser.add_argument('--chain', type=str, help='Process specific chain only (use "all" to process all chains)')
    parser.add_argument('--dry-run', action='store_true', help='Run without inserting data')
    parser.add_argument('--block-batch-size', type=int, default=100, help='Number of blocks to process in each batch (default: 100)')
    parser.add_argument('--start-date', type=str, default='2024-04-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads for parallel processing (default: 4)')
    parser.add_argument('--show-progress', action='store_true', help='Show current progress and exit')
    parser.add_argument('--reset-progress', action='store_true', help='Reset progress file and start fresh')
    parser.add_argument('--test-blocks', type=str, help='Test specific block range for type 4 transactions (format: chain:start:end)')
    
    args = parser.parse_args()
    
    if args.show_progress:
        show_progress()
        return
    
    if args.reset_progress:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print(f"🗑️  Progress file {PROGRESS_FILE} deleted. Starting fresh.")
        else:
            print("No progress file to reset.")
        return
    
    if args.test_blocks:
        try:
            parts = args.test_blocks.split(':')
            if len(parts) != 3:
                print("❌ Invalid format. Use: chain:start_block:end_block")
                print("   Example: --test-blocks ethereum:20000000:20000100")
                return
            
            chain, start_block, end_block = parts
            start_block, end_block = int(start_block), int(end_block)
            
            db_connector = DbConnector()
            test_block_range(db_connector, chain, start_block, end_block)
            return
            
        except ValueError:
            print("❌ Invalid block numbers. Use integers only.")
            return
        except Exception as e:
            print(f"❌ Error testing blocks: {e}")
            return
    
    print("🚀 Starting Multi-threaded EIP-7702 Authorization List Backfill")
    print(f"   Start date: {args.start_date}")
    print(f"   Dry run: {args.dry_run}")
    print(f"   Block batch size: {args.block_batch_size}")
    print(f"   Worker threads: {args.workers}")
    
    # Initialize database
    db_connector = DbConnector()
    
    # Get chains to process
    if args.chain and args.chain.lower() == 'all':
        print(f"   Processing ALL chains")
        chains_to_process = get_chains_with_type4_txs(db_connector, args.start_date)
        print(f"   Found {len(chains_to_process)} chains with type 4 transactions")
    elif args.chain:
        chains_to_process = [args.chain]
        print(f"   Processing single chain: {args.chain}")
    else:
        # Default behavior: find chains with type 4 transactions
        chains_to_process = get_chains_with_type4_txs(db_connector, args.start_date)
        print(f"   Found {len(chains_to_process)} chains with type 4 transactions")
    
    if not chains_to_process:
        print("❌ No chains to process!")
        return
    
    # Filter out already completed chains unless explicitly requested
    progress = load_progress()
    if progress and 'chains' in progress and not args.chain:
        remaining_chains = []
        for chain in chains_to_process:
            chain_status = progress['chains'].get(chain, {}).get('status', 'pending')
            if chain_status != 'completed':
                remaining_chains.append(chain)
            else:
                print(f"   ✅ Skipping {chain} (already completed)")
        chains_to_process = remaining_chains
    
    if not chains_to_process:
        print("✅ All chains already completed!")
        return
    
    print(f"\n🚀 Starting parallel processing of {len(chains_to_process)} chains with {args.workers} workers")
    print(f"   Chains to process: {chains_to_process}")
    
    # Use ThreadPoolExecutor for parallel processing
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all chain processing jobs
        future_to_chain = {
            executor.submit(
                process_single_chain, 
                chain, 
                DbConnector(), 
                args.start_date, 
                args.block_batch_size, 
                args.dry_run
            ): chain for chain in chains_to_process
        }
        
        print(f"\n📊 Processing {len(chains_to_process)} chains in parallel...")
        print("=" * 80)
        
        # Collect results as they complete
        for future in as_completed(future_to_chain):
            chain = future_to_chain[future]
            try:
                result = future.result()
                results.append(result)
                total_processed += result['processed']
                
                if result['status'] == 'completed':
                    print(f"\n✅ {chain}: Completed ({result['processed']} transactions)")
                elif result['status'] == 'failed':
                    print(f"\n❌ {chain}: Failed - {result['error']}")
                elif result['status'] == 'skipped':
                    print(f"\n⏭️  {chain}: Skipped - {result['error']}")
                    
            except Exception as exc:
                print(f"\n💥 {chain}: Unexpected error - {exc}")
                results.append({'chain': chain, 'status': 'failed', 'processed': 0, 'error': str(exc)})
    
    # Summary
    end_time = time.time()
    duration = end_time - start_time
    
    successful = [r for r in results if r['status'] == 'completed']
    failed = [r for r in results if r['status'] == 'failed']
    skipped = [r for r in results if r['status'] == 'skipped']
    
    print(f"\n" + "=" * 80)
    print(f"🎉 Multi-threaded Backfill Completed!")
    print(f"   Duration: {duration:.1f} seconds")
    print(f"   Total transactions processed: {total_processed}")
    print(f"   Successful chains ({len(successful)}): {[r['chain'] for r in successful]}")
    if failed:
        failed_info = [f"{r['chain']} ({r['error']})" for r in failed]
        print(f"   Failed chains ({len(failed)}): {failed_info}")
    if skipped:
        skipped_info = [f"{r['chain']} ({r['error']})" for r in skipped]
        print(f"   Skipped chains ({len(skipped)}): {skipped_info}")
    if args.dry_run:
        print("   This was a dry run - no data was inserted")
    
    print(f"\n📊 Final Status Summary:")
    for result in results:
        status_emoji = {'completed': '✅', 'failed': '❌', 'skipped': '⏭️'}.get(result['status'], '❓')
        print(f"   {status_emoji} {result['chain']:>15}: {result['status']} ({result['processed']} txs)")
        if result['error']:
            print(f"      └── Error: {result['error']}")


if __name__ == '__main__':
    main()
