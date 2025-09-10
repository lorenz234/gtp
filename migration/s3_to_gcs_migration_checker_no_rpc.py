#!/usr/bin/env python3

import os
import sys
import time
import json
import io
import pandas as pd
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple
import numpy as np

# Import GCS utilities
from backend.src.adapters.rpc_funcs.gcs_utils import connect_to_gcs
from backend.src.adapters.rpc_funcs.utils import connect_to_node, get_chain_config
from backend.src.db_connector import DbConnector

load_dotenv()

# Thread-local storage for clients
thread_local = threading.local()

def get_s3_client():
    """Creates and returns an S3 client."""
    if not hasattr(thread_local, 's3_client'):
        thread_local.s3_client = boto3.client('s3')
    return thread_local.s3_client

def get_gcs_client():
    """Creates a thread-local GCS client."""
    if not hasattr(thread_local, 'gcs'):
        gcs, _ = connect_to_gcs()
        thread_local.gcs = gcs
    return thread_local.gcs

def setup_rpc_connection(chain):
    """Set up RPC connection for the given chain."""
    try:
        db_connector = DbConnector()
        active_rpc_configs, _ = get_chain_config(db_connector, chain)
        
        if not active_rpc_configs:
            print(f"No RPC configurations found for chain: {chain}")
            return None
        
        # Try to connect to the first available RPC
        for rpc_config in active_rpc_configs:
            try:
                print(f"Attempting to connect to RPC: {rpc_config['url']}")
                w3 = connect_to_node(rpc_config)
                print(f"✅ Successfully connected to {chain} RPC: {rpc_config['url']}")
                return w3
            except Exception as e:
                print(f"Failed to connect to {rpc_config['url']}: {e}")
                continue
        
        print(f"❌ Failed to connect to any RPC for chain: {chain}")
        return None
        
    except Exception as e:
        print(f"Error setting up RPC connection for {chain}: {e}")
        return None

def list_s3_files(bucket_name, folder_prefix):
    """Lists all files in S3 folder with metadata (no download)."""
    print(f"Listing S3 files in folder '{folder_prefix}'...")
    try:
        s3_client = get_s3_client()
        
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        paginator = s3_client.get_paginator('list_objects_v2')
        result = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
        
        files_with_metadata = []
        for page in result:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"] != folder_prefix:
                        files_with_metadata.append({
                            'key': obj["Key"],
                            'size': obj.get('Size', 0),
                            'last_modified': obj.get('LastModified')
                        })
        
        print(f"Found {len(files_with_metadata)} files in S3")
        return files_with_metadata
        
    except ClientError as e:
        print(f"Error listing S3 files: {e}")
        return []

def extract_block_info(file_key):
    """Extracts block range from S3 filename."""
    file_name = os.path.basename(file_key)
    
    try:
        # Handle format: optimism_tx_101157267-101177266.parquet
        if '-' in file_name:
            name_parts = file_name.split('_')
            for part in name_parts:
                if '-' in part:
                    range_part = part.split('.')[0]
                    block_start, block_end = range_part.split('-')
                    return int(block_start), int(block_end)
        
        # Handle format: optimism_tx_101157267_101177266.parquet
        name_parts = file_name.split('_')
        if len(name_parts) >= 3:
            block_start = int(name_parts[-2])
            block_end = int(name_parts[-1].split('.')[0])
            return block_start, block_end
            
    except (ValueError, IndexError) as e:
        print(f"Warning: Could not extract block info from '{file_name}': {e}")
        return None, None

def load_tracking_file(chain, folder_prefix):
    """Load the tracking file from the transfer script if it exists."""
    tracking_file = f"{chain}_{folder_prefix.replace('/', '_')}_transferred.json"
    
    if os.path.exists(tracking_file):
        try:
            with open(tracking_file, 'r') as f:
                processed_files = set(json.load(f))
            print(f"Loaded tracking file: {tracking_file} ({len(processed_files)} processed files)")
            return processed_files
        except Exception as e:
            print(f"Error loading tracking file {tracking_file}: {e}")
    else:
        print(f"No tracking file found: {tracking_file}")
    
    return set()

# Cache for RPC calls to avoid repeated requests
class BlockTimestampCache:
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()
    
    def get_block_timestamp(self, w3, block_number):
        """Get block timestamp with caching."""
        with self.lock:
            if block_number in self.cache:
                return self.cache[block_number]
        
        try:
            block = w3.eth.get_block(block_number)
            timestamp = block['timestamp']
            
            with self.lock:
                self.cache[block_number] = timestamp
            
            return timestamp
        except Exception as e:
            print(f"Error fetching block {block_number}: {e}")
            return None
    
    def get_cache_size(self):
        return len(self.cache)

# Global cache instance
block_cache = BlockTimestampCache()

def get_date_from_parquet(s3_client, bucket_name, file_key):
    """Get the actual date from a parquet file's block_timestamp column."""
    try:
        # Download file to memory
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read()
        buffer = io.BytesIO(content)
        
        # Read parquet file
        df = pd.read_parquet(buffer)
        
        if df.empty:
            print(f"Warning: Empty file {file_key}")
            return None
            
        # Find timestamp column
        timestamp_col = None
        for col in df.columns:
            if col.lower() == 'block_timestamp':
                timestamp_col = col
                break
                
        if not timestamp_col:
            print(f"Warning: No block_timestamp column found in {file_key}")
            return None
            
        # Get the first timestamp
        block_timestamp = df[timestamp_col].iloc[0]
        
        # Parse timestamp
        if isinstance(block_timestamp, str):
            try:
                # Handle nanosecond precision timestamps by truncating to microseconds
                if '+' in block_timestamp or block_timestamp.endswith('Z'):
                    # Split on timezone marker and handle timezone separately
                    if '+' in block_timestamp:
                        ts_parts = block_timestamp.split('+')
                        tz_part = '+' + ts_parts[1]
                    else:  # ends with Z
                        ts_parts = [block_timestamp[:-1]]  # remove Z
                        tz_part = '+00:00'
                    
                    # Handle microseconds part carefully
                    main_ts = ts_parts[0].split('.')[0]
                    if '.' in ts_parts[0]:
                        decimal_part = ts_parts[0].split('.')[1]
                        # Pad with zeros if less than 6 digits
                        if len(decimal_part) < 6:
                            micros = decimal_part.ljust(6, '0')
                        elif len(decimal_part) > 6:
                            # If more than 6 digits (nanoseconds), truncate to microseconds
                            micros = decimal_part[:6]
                        else:
                            # If exactly 6 digits, use as is
                            micros = decimal_part
                        main_ts = f"{main_ts}.{micros}"
                    
                    # Reconstruct with timezone
                    block_timestamp = f"{main_ts}{tz_part}"
                
                # Try parsing with timezone
                dt = datetime.fromisoformat(block_timestamp)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    # Try format with microseconds and UTC: '2023-05-23 12:06:15.000000 UTC'
                    dt = datetime.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S.%f UTC")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    try:
                        # Try basic format
                        dt = datetime.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S")
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        print(f"Warning: Could not parse timestamp {block_timestamp} in {file_key}")
                        return None
        elif isinstance(block_timestamp, (int, float, np.int64, np.float64)):
            # Unix timestamp
            dt = datetime.fromtimestamp(float(block_timestamp))
            return dt.strftime("%Y-%m-%d")
        elif isinstance(block_timestamp, pd.Timestamp):
            # Pandas timestamp
            return block_timestamp.strftime("%Y-%m-%d")
        else:
            print(f"Warning: Unexpected timestamp type {type(block_timestamp)} in {file_key}")
            return None
            
    except Exception as e:
        print(f"Error processing {file_key}: {e}")
        return None

def check_single_file(args):
    """Check a single S3 file for its GCS counterpart using parquet timestamp."""
    file_info, s3_bucket, gcs_bucket, chain, processed_files = args
    
    s3_file = file_info['key']
    s3_size = file_info['size']
    
    result = {
        's3_file': s3_file,
        'status': 'error',
        'gcs_file': None,
        's3_size': s3_size,
        'gcs_size': 0,
        'size_match': False,
        'error': None,
        'parquet_date': None
    }
    
    try:
        # Extract block info
        block_start, block_end = extract_block_info(s3_file)
        if block_start is None or block_end is None:
            result['error'] = 'Could not extract block info'
            return result
        
        # Get date from parquet file
        s3_client = get_s3_client()
        date_str = get_date_from_parquet(s3_client, s3_bucket, s3_file)
        result['parquet_date'] = date_str
        
        if not date_str:
            result['error'] = 'Could not get timestamp from parquet file'
            return result
        
        # Check if GCS file exists
        exists, gcs_path, gcs_size = check_gcs_file_exists(gcs_bucket, chain, date_str, block_start, block_end)
        
        result['gcs_file'] = gcs_path
        result['gcs_size'] = gcs_size
        
        if exists:
            result['status'] = 'migrated'
            # Check if sizes match (allowing for differences due to compression)
            size_diff = abs(result['s3_size'] - result['gcs_size'])
            size_threshold = max(1024, result['s3_size'] * 0.1)  # 10% tolerance
            result['size_match'] = size_diff <= size_threshold
        else:
            result['status'] = 'missing'
            
        # If we have tracking file info, add it to the result
        if processed_files:
            result['in_tracking_file'] = s3_file in processed_files
        else:
            result['in_tracking_file'] = None  # No tracking file available
            
    except Exception as e:
        result['error'] = str(e)
    
    return result

def check_gcs_file_exists(gcs_bucket, chain, date_str, block_start, block_end):
    """Checks if corresponding GCS file exists."""
    try:
        gcs = get_gcs_client()
        bucket = gcs.bucket(gcs_bucket)
        
        filename = f"{chain}_tx_{block_start}_{block_end}.parquet"
        file_path = f"{chain}/{date_str}/{filename}"
        
        blob = bucket.blob(file_path)
        exists = blob.exists()
        
        if exists:
            blob.reload()
            size = blob.size
            return True, file_path, size
        else:
            return False, file_path, 0
            
    except Exception as e:
        print(f"Error checking GCS file: {e}")
        return False, None, 0

def check_migration(s3_bucket, gcs_bucket, folder_prefix, chain, max_sample_files=None, skip_tracking=False, delete_migrated=False, dry_run_delete=False, max_workers=10):
    """
    Checks migration status by reading timestamps directly from parquet files.
    
    Args:
        s3_bucket (str): S3 bucket name
        gcs_bucket (str): GCS bucket name
        folder_prefix (str): S3 folder to check
        chain (str): Chain name
        max_sample_files (int): Optional limit for testing
        skip_tracking (bool): Skip loading tracking file if True
        delete_migrated (bool): Delete S3 files after successful migration verification
        dry_run_delete (bool): Only simulate deletion (don't actually delete)
        max_workers (int): Maximum number of parallel workers for file checking
    """
    print(f"\n{'='*60}")
    print(f"PARQUET-BASED MIGRATION CHECKER (Direct File Processing + Parallel)")
    print(f"S3 Bucket: {s3_bucket}")
    print(f"GCS Bucket: {gcs_bucket}")
    print(f"S3 Folder: {folder_prefix}")
    print(f"Chain: {chain}")
    print(f"Max Workers: {max_workers}")
    if delete_migrated:
        print(f"Delete Mode: {'DRY RUN' if dry_run_delete else 'ENABLED (BATCH)'}")
    print(f"{'='*60}")
    
    # Load tracking file from transfer script (optional)
    if skip_tracking:
        print("⚠️  Skipping tracking file (--no-tracking specified)")
        processed_files = set()
    else:
        processed_files = load_tracking_file(chain, folder_prefix)
        if not processed_files:
            print("⚠️  No tracking file found - will check all files for existence in GCS")
    
    # Get S3 files (metadata only, no downloads)
    s3_files_info = list_s3_files(s3_bucket, folder_prefix)
    if not s3_files_info:
        print("No S3 files found!")
        return
    
    # Limit files for testing if specified
    if max_sample_files and max_sample_files < len(s3_files_info):
        print(f"Sampling {max_sample_files} files out of {len(s3_files_info)} for testing")
        s3_files_info = s3_files_info[:max_sample_files]
    
    print(f"Checking {len(s3_files_info)} files with {max_workers} parallel workers...")
    
    # Statistics
    stats = {
        'total': len(s3_files_info),
        'migrated': 0,
        'missing': 0,
        'errors': 0,
        'size_mismatches': 0,
        'total_s3_size': 0,
        'total_gcs_size': 0,
        'tracking_file_available': len(processed_files) > 0,
        'parallel_workers': max_workers
    }
    
    results = []
    start_time = time.time()
    
    # Parallel file checking with progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(check_single_file, (file_info, s3_bucket, gcs_bucket, chain, processed_files)): file_info
            for file_info in s3_files_info
        }
        
        # Process results with progress bar
        with tqdm(total=len(s3_files_info), desc="Checking files", unit="file") as pbar:
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Update statistics
                    stats['total_s3_size'] += result['s3_size']
                    stats['total_gcs_size'] += result['gcs_size']
                    
                    if result['status'] == 'migrated':
                        stats['migrated'] += 1
                        if not result['size_match']:
                            stats['size_mismatches'] += 1
                    elif result['status'] == 'missing':
                        stats['missing'] += 1
                    else:
                        stats['errors'] += 1
                    
                    # Update progress description
                    pbar.set_description(f"Checking (✓{stats['migrated']} ✗{stats['missing']} ⚠{stats['errors']})")
                    pbar.update(1)
                    
                except Exception as e:
                    print(f"Error processing file: {e}")
                    pbar.update(1)
    
    check_time = time.time() - start_time
    
    # Print detailed results
    print(f"\n{'='*60}")
    print(f"PARQUET-BASED MIGRATION CHECK RESULTS")
    print(f"{'='*60}")
    print(f"Total files checked: {stats['total']}")
    print(f"Successfully migrated: {stats['migrated']} ({stats['migrated']/stats['total']*100:.1f}%)")
    print(f"Missing in GCS: {stats['missing']} ({stats['missing']/stats['total']*100:.1f}%)")
    print(f"Errors: {stats['errors']} ({stats['errors']/stats['total']*100:.1f}%)")
    print(f"Size mismatches: {stats['size_mismatches']}")
    print(f"Total S3 size: {stats['total_s3_size']/1024/1024:.2f} MB")
    print(f"Total GCS size: {stats['total_gcs_size']/1024/1024:.2f} MB")
    print(f"Check time: {check_time:.2f} seconds ({stats['total']/check_time:.1f} files/second)")
    print(f"Parallel workers: {max_workers}")
    
    if stats['tracking_file_available']:
        # Show tracking file analysis
        tracked_files = [r for r in results if r.get('in_tracking_file') is True]
        untracked_files = [r for r in results if r.get('in_tracking_file') is False]
        print(f"Tracking file analysis:")
        print(f"  - Files in tracking file: {len(tracked_files)}")
        print(f"  - Files NOT in tracking file: {len(untracked_files)}")
        
        # Show status breakdown for tracked vs untracked files
        if tracked_files:
            tracked_migrated = len([r for r in tracked_files if r['status'] == 'migrated'])
            print(f"  - Tracked files migrated: {tracked_migrated}/{len(tracked_files)} ({tracked_migrated/len(tracked_files)*100:.1f}%)")
        if untracked_files:
            untracked_migrated = len([r for r in untracked_files if r['status'] == 'migrated'])
            print(f"  - Untracked files migrated: {untracked_migrated}/{len(untracked_files)} ({untracked_migrated/len(untracked_files)*100:.1f}%)")
    else:
        print("Tracking file: Not available (checking all files for existence)")
    
    # Show problematic files
    missing_files = [r for r in results if r['status'] == 'missing']
    error_files = [r for r in results if r['status'] == 'error']
    size_mismatch_files = [r for r in results if r['status'] == 'migrated' and not r['size_match']]
    
    if missing_files:
        print(f"\n❌ MISSING FILES ({len(missing_files)}):")
        for result in missing_files[:10]:
            tracking_info = ""
            if result.get('in_tracking_file') is not None:
                tracking_info = f" (In tracking: {'Yes' if result['in_tracking_file'] else 'No'})"
            print(f"  - {result['s3_file']} → {result['gcs_file']} (Parquet date: {result.get('parquet_date')}){tracking_info}")
        if len(missing_files) > 10:
            print(f"  ... and {len(missing_files) - 10} more")
    
    if error_files:
        print(f"\n⚠️  ERROR FILES ({len(error_files)}):")
        for result in error_files[:5]:
            print(f"  - {result['s3_file']}: {result['error']}")
        if len(error_files) > 5:
            print(f"  ... and {len(error_files) - 5} more")
    
    if size_mismatch_files:
        print(f"\n🔍 SIZE MISMATCHES ({len(size_mismatch_files)}):")
        for result in size_mismatch_files[:5]:
            s3_mb = result['s3_size'] / 1024 / 1024
            gcs_mb = result['gcs_size'] / 1024 / 1024
            print(f"  - {result['s3_file']}: S3={s3_mb:.2f}MB vs GCS={gcs_mb:.2f}MB")
        if len(size_mismatch_files) > 5:
            print(f"  ... and {len(size_mismatch_files) - 5} more")
    
    # Migration status
    migration_complete = (stats['missing'] == 0 and stats['errors'] == 0)
    
    if migration_complete:
        print(f"\n✅ MIGRATION COMPLETE: All files successfully migrated!")
    elif stats['missing'] > 0:
        print(f"\n❌ MIGRATION INCOMPLETE: {stats['missing']} files missing in GCS")
    
    if stats['errors'] > 0:
        print(f"\n⚠️  CHECK REQUIRED: {stats['errors']} files had errors during verification")
    
    print(f"{'='*60}")
    
    # Handle deletion if requested
    if delete_migrated:
        if not migration_complete:
            print(f"\n⚠️  DELETION SKIPPED: Migration not complete (missing: {stats['missing']}, errors: {stats['errors']})")
            print("Only deleting files when ALL files are successfully migrated to ensure data safety.")
        else:
            delete_migrated_files(s3_bucket, results, dry_run_delete)
    
    # Save detailed report
    report_file = f"migration_check_{chain}_{folder_prefix.replace('/', '_')}.json"
    try:
        with open(report_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'method': 'parquet_timestamps_parallel',
                'config': {
                    's3_bucket': s3_bucket,
                    'gcs_bucket': gcs_bucket,
                    'folder_prefix': folder_prefix,
                    'chain': chain,
                    'delete_migrated': delete_migrated,
                    'dry_run_delete': dry_run_delete,
                    'max_workers': max_workers
                },
                'statistics': stats,
                'performance': {
                    'check_time_seconds': check_time,
                    'files_per_second': stats['total'] / check_time if check_time > 0 else 0
                },
                'tracking_file_loaded': stats['tracking_file_available'],
                'tracking_file_size': len(processed_files) if processed_files else 0,
                'migration_complete': migration_complete,
                'results': results
            }, f, indent=2)
        print(f"\nDetailed report saved to: {report_file}")
    except Exception as e:
        print(f"Warning: Could not save report file: {e}")

def debug_s3_file(bucket_name, file_key):
    """
    Perform detailed debugging of an S3 file to understand why it might be failing.
    
    Returns:
        dict: Detailed debug information about the file
    """
    debug_info = {
        'file_key': file_key,
        'exists': False,
        'size_bytes': 0,
        'readable': False,
        'valid_parquet': False,
        'error_details': None,
        'content_preview': None,
        'metadata': {}
    }
    
    try:
        s3_client = get_s3_client()
        
        # Check if file exists and get metadata
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
            debug_info['exists'] = True
            debug_info['size_bytes'] = response.get('ContentLength', 0)
            debug_info['metadata'] = {
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag'),
                'content_type': response.get('ContentType'),
                'storage_class': response.get('StorageClass')
            }
        except ClientError as e:
            debug_info['error_details'] = f"File doesn't exist or access denied: {e}"
            return debug_info
        
        # Check if file is empty
        if debug_info['size_bytes'] == 0:
            debug_info['error_details'] = "File is completely empty (0 bytes)"
            return debug_info
        
        # Try to read the file
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read()
            debug_info['readable'] = True
            
            # Store first few bytes for inspection
            debug_info['content_preview'] = content[:100].hex() if len(content) >= 100 else content.hex()
            
            # Verify it's actually the size we expect
            actual_size = len(content)
            if actual_size != debug_info['size_bytes']:
                debug_info['error_details'] = f"Size mismatch: metadata says {debug_info['size_bytes']} bytes, but read {actual_size} bytes"
                return debug_info
            
            # Try to parse as parquet
            try:
                buffer = io.BytesIO(content)
                df = pd.read_parquet(buffer)
                debug_info['valid_parquet'] = True
                debug_info['row_count'] = len(df)
                debug_info['column_count'] = len(df.columns)
                debug_info['columns'] = list(df.columns)
                
                # Check if dataframe is empty
                if df.empty:
                    debug_info['error_details'] = "Parquet file is valid but contains no data (empty DataFrame)"
                else:
                    debug_info['error_details'] = None  # No error - file is completely valid
                
            except Exception as parquet_error:
                debug_info['valid_parquet'] = False
                debug_info['error_details'] = f"Valid file but not readable as parquet: {str(parquet_error)}"
                
                # Check if it looks like a parquet file (magic bytes)
                if content.startswith(b'PAR1'):
                    debug_info['error_details'] += " (File has correct parquet magic bytes but is corrupted)"
                else:
                    debug_info['error_details'] += f" (File doesn't start with parquet magic bytes, starts with: {content[:20].hex()})"
        
        except ClientError as e:
            debug_info['error_details'] = f"Cannot read file from S3: {e}"
        except Exception as e:
            debug_info['error_details'] = f"Unexpected error reading file: {e}"
    
    except Exception as e:
        debug_info['error_details'] = f"Failed to debug file: {e}"
    
    return debug_info

def debug_problematic_files(s3_bucket, missing_files_report, max_debug_files=10):
    """
    Debug the files that are consistently failing with 'empty-or-read-error'.
    
    Args:
        s3_bucket (str): S3 bucket name
        missing_files_report (str): Path to the JSON report with missing files
        max_debug_files (int): Maximum number of files to debug in detail
    """
    print(f"\n{'='*60}")
    print(f"DEBUGGING PROBLEMATIC FILES")
    print(f"{'='*60}")
    
    # Load the missing files from the report
    try:
        with open(missing_files_report, 'r') as f:
            data = json.load(f)
        
        missing_files = [r["s3_file"] for r in data.get("results", []) if r.get("status") == "missing"]
        if not missing_files:
            print("No missing files found in report to debug.")
            return
            
        print(f"Found {len(missing_files)} missing files in report")
        
        # Limit the number of files to debug
        files_to_debug = missing_files[:max_debug_files]
        if len(missing_files) > max_debug_files:
            print(f"Debugging first {max_debug_files} files (out of {len(missing_files)} total)")
        
        debug_results = []
        
        print(f"\nAnalyzing {len(files_to_debug)} files...")
        for i, file_key in enumerate(files_to_debug, 1):
            print(f"\n[{i}/{len(files_to_debug)}] Debugging: {file_key}")
            
            debug_info = debug_s3_file(s3_bucket, file_key)
            debug_results.append(debug_info)
            
            # Print immediate results
            print(f"  📁 Exists: {debug_info['exists']}")
            print(f"  📏 Size: {debug_info['size_bytes']} bytes")
            print(f"  📖 Readable: {debug_info['readable']}")
            print(f"  📊 Valid Parquet: {debug_info['valid_parquet']}")
            
            if debug_info['error_details']:
                print(f"  ❌ Error: {debug_info['error_details']}")
            
            if debug_info['valid_parquet']:
                print(f"  📈 Rows: {debug_info.get('row_count', 'N/A')}")
                print(f"  📋 Columns: {debug_info.get('column_count', 'N/A')}")
            
            if debug_info['content_preview'] and len(debug_info['content_preview']) > 0:
                print(f"  🔍 First bytes: {debug_info['content_preview'][:40]}...")
        
        # Analyze patterns
        print(f"\n{'='*60}")
        print(f"DEBUG ANALYSIS SUMMARY")
        print(f"{'='*60}")
        
        # Count different types of issues
        issues = {
            'empty_files': 0,
            'non_existent': 0,
            'corrupted_parquet': 0,
            'read_errors': 0,
            'empty_dataframes': 0,
            'other_errors': 0
        }
        
        for debug_info in debug_results:
            if not debug_info['exists']:
                issues['non_existent'] += 1
            elif debug_info['size_bytes'] == 0:
                issues['empty_files'] += 1
            elif not debug_info['readable']:
                issues['read_errors'] += 1
            elif not debug_info['valid_parquet']:
                issues['corrupted_parquet'] += 1
            elif 'empty DataFrame' in str(debug_info.get('error_details', '')):
                issues['empty_dataframes'] += 1
            else:
                issues['other_errors'] += 1
        
        print(f"Files analyzed: {len(debug_results)}")
        print(f"Non-existent files: {issues['non_existent']}")
        print(f"Empty files (0 bytes): {issues['empty_files']}")
        print(f"Read errors: {issues['read_errors']}")
        print(f"Corrupted parquet: {issues['corrupted_parquet']}")
        print(f"Empty DataFrames: {issues['empty_dataframes']}")
        print(f"Other errors: {issues['other_errors']}")
        
        # Show common patterns
        error_patterns = {}
        for debug_info in debug_results:
            if debug_info['error_details']:
                # Group similar errors
                error_key = debug_info['error_details'].split(':')[0] if ':' in debug_info['error_details'] else debug_info['error_details']
                error_patterns[error_key] = error_patterns.get(error_key, 0) + 1
        
        if error_patterns:
            print(f"\nCommon error patterns:")
            for pattern, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True):
                print(f"  - {pattern}: {count} files")
        
        # Save detailed debug report
        debug_report_file = f"debug_problematic_files_{int(time.time())}.json"
        try:
            # Convert datetime objects to strings for JSON serialization
            serializable_results = []
            for result in debug_results:
                serializable_result = result.copy()
                if 'metadata' in serializable_result and serializable_result['metadata']:
                    # Convert datetime objects to ISO format strings
                    metadata = serializable_result['metadata'].copy()
                    if 'last_modified' in metadata and metadata['last_modified']:
                        metadata['last_modified'] = metadata['last_modified'].isoformat()
                    serializable_result['metadata'] = metadata
                serializable_results.append(serializable_result)
            
            with open(debug_report_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'bucket': s3_bucket,
                    'report_analyzed': missing_files_report,
                    'files_debugged': len(debug_results),
                    'total_missing_files': len(missing_files),
                    'issue_summary': issues,
                    'error_patterns': error_patterns,
                    'detailed_results': serializable_results
                }, f, indent=2)
            print(f"\nDetailed debug report saved to: {debug_report_file}")
        except Exception as e:
            print(f"Warning: Could not save debug report: {e}")
            
    except Exception as e:
        print(f"Error loading missing files report: {e}")

def batch_delete_s3_files(bucket_name: str, file_keys: List[str], dry_run: bool = False) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Delete multiple S3 files in batches using delete_objects API.
    
    Args:
        bucket_name: S3 bucket name
        file_keys: List of S3 object keys to delete
        dry_run: If True, simulate deletion without actually deleting
        
    Returns:
        Tuple of (successful_deletions, failed_deletions)
    """
    if dry_run:
        print(f"DRY RUN: Would delete {len(file_keys)} files from s3://{bucket_name}/")
        return file_keys, []
    
    successful_deletions = []
    failed_deletions = []
    
    # S3 delete_objects API can handle up to 1000 objects per request
    batch_size = 1000
    
    try:
        s3_client = get_s3_client()
        
        # Process files in batches
        for i in range(0, len(file_keys), batch_size):
            batch = file_keys[i:i + batch_size]
            
            # Prepare delete request
            delete_request = {
                'Objects': [{'Key': key} for key in batch],
                'Quiet': False  # Return info about deleted and failed objects
            }
            
            try:
                response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete=delete_request
                )
                
                # Process successful deletions
                if 'Deleted' in response:
                    for deleted in response['Deleted']:
                        successful_deletions.append(deleted['Key'])
                
                # Process failed deletions
                if 'Errors' in response:
                    for error in response['Errors']:
                        failed_deletions.append({
                            'file': error['Key'],
                            'error': f"{error['Code']}: {error['Message']}"
                        })
                        
            except ClientError as e:
                # If batch fails entirely, mark all files in batch as failed
                error_message = str(e)
                for key in batch:
                    failed_deletions.append({
                        'file': key,
                        'error': f"Batch deletion failed: {error_message}"
                    })
                    
    except Exception as e:
        # If everything fails, mark all files as failed
        error_message = str(e)
        for key in file_keys:
            failed_deletions.append({
                'file': key,
                'error': f"Deletion setup failed: {error_message}"
            })
    
    return successful_deletions, failed_deletions

def delete_migrated_files(s3_bucket, results, dry_run=False):
    """Delete S3 files that have been successfully migrated to GCS using batch deletion."""
    
    # Filter for files that are successfully migrated with matching sizes
    files_to_delete = [
        r for r in results 
        if r['status'] == 'migrated' and r['size_match'] and r['error'] is None
    ]
    
    if not files_to_delete:
        print("No files to delete (no successfully migrated files with matching sizes)")
        return
    
    print(f"\n{'='*60}")
    print(f"S3 BATCH FILE DELETION {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"Files eligible for deletion: {len(files_to_delete)}")
    
    # Calculate total size
    total_size = sum(f['s3_size'] for f in files_to_delete)
    total_size_mb = total_size / 1024 / 1024
    total_size_gb = total_size_mb / 1024
    
    print(f"Total size to delete: {total_size_mb:.2f} MB ({total_size_gb:.2f} GB)")
    print(f"Batch deletion: Up to 1000 files per API call")
    
    if not dry_run:
        print(f"\n✅ VERIFIED MIGRATION: Proceeding with deletion of {len(files_to_delete)} successfully migrated files")
        print(f"Bucket: {s3_bucket}")
        print(f"Total size: {total_size_gb:.2f} GB")
        
        # Show first few files as examples
        print(f"\nFirst 5 files to be deleted:")
        for i, result in enumerate(files_to_delete[:5]):
            size_mb = result['s3_size'] / 1024 / 1024
            print(f"  {i+1}. {result['s3_file']} ({size_mb:.2f} MB)")
        
        if len(files_to_delete) > 5:
            print(f"  ... and {len(files_to_delete) - 5} more files")
        
        print(f"\n🚀 All files verified as successfully migrated to GCS - proceeding with safe deletion...")
    
    # Perform batch deletion
    file_keys = [result['s3_file'] for result in files_to_delete]
    
    print(f"\nStarting batch deletion...")
    start_time = time.time()
    
    successful_deletions, failed_deletions = batch_delete_s3_files(s3_bucket, file_keys, dry_run)
    
    deletion_time = time.time() - start_time
    
    # Calculate deletion statistics
    deletion_stats = {
        'attempted': len(file_keys),
        'successful': len(successful_deletions),
        'failed': len(failed_deletions),
        'total_size_deleted': 0,
        'deletion_time_seconds': deletion_time,
        'files_per_second': len(successful_deletions) / deletion_time if deletion_time > 0 else 0
    }
    
    # Calculate size of successfully deleted files
    successful_files_map = {r['s3_file']: r for r in files_to_delete}
    for deleted_key in successful_deletions:
        if deleted_key in successful_files_map:
            deletion_stats['total_size_deleted'] += successful_files_map[deleted_key]['s3_size']
    
    # Print deletion results
    print(f"\n{'='*60}")
    print(f"BATCH DELETION RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"Files attempted: {deletion_stats['attempted']}")
    print(f"Successfully deleted: {deletion_stats['successful']}")
    print(f"Failed deletions: {deletion_stats['failed']}")
    print(f"Deletion time: {deletion_time:.2f} seconds")
    print(f"Deletion rate: {deletion_stats['files_per_second']:.1f} files/second")
    
    if deletion_stats['successful'] > 0:
        deleted_size_mb = deletion_stats['total_size_deleted'] / 1024 / 1024
        deleted_size_gb = deleted_size_mb / 1024
        print(f"Total size deleted: {deleted_size_mb:.2f} MB ({deleted_size_gb:.2f} GB)")
        
        if deletion_time > 0:
            mbps = deleted_size_mb / deletion_time
            print(f"Deletion throughput: {mbps:.2f} MB/second")
    
    if failed_deletions:
        print(f"\n❌ FAILED DELETIONS ({len(failed_deletions)}):")
        for failure in failed_deletions[:10]:
            print(f"  - {failure['file']}: {failure['error']}")
        if len(failed_deletions) > 10:
            print(f"  ... and {len(failed_deletions) - 10} more")
    
    if deletion_stats['successful'] == len(files_to_delete):
        if dry_run:
            print(f"\n✅ DRY RUN COMPLETE: All {deletion_stats['successful']} files would be successfully deleted")
        else:
            print(f"\n✅ BATCH DELETION COMPLETE: All {deletion_stats['successful']} files successfully deleted from S3")
            improvement = len(files_to_delete) / deletion_time if deletion_time > 0 else float('inf')
            print(f"🚀 Performance: {improvement:.1f}x faster than single-file deletion (estimated)")
    elif deletion_stats['failed'] > 0:
        print(f"\n⚠️  PARTIAL SUCCESS: {deletion_stats['successful']}/{len(files_to_delete)} files deleted")
    
    # Save deletion report
    if not dry_run:
        report_file = f"batch_deletion_report_{int(time.time())}.json"
        try:
            with open(report_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'bucket': s3_bucket,
                    'method': 'batch_deletion',
                    'statistics': deletion_stats,
                    'successful_deletions': successful_deletions,
                    'failed_deletions': failed_deletions
                }, f, indent=2)
            print(f"Deletion report saved to: {report_file}")
        except Exception as e:
            print(f"Warning: Could not save deletion report: {e}")

def main():
    # Load environment variables
    s3_bucket = os.getenv("S3_LONG_TERM_BUCKET")
    gcs_bucket = os.getenv("GCS_BUCKET_NAME")
    
    if not s3_bucket or not gcs_bucket:
        print("Error: S3_LONG_TERM_BUCKET or GCS_BUCKET_NAME environment variables not set")
        sys.exit(1)
    
    # Check arguments
    if len(sys.argv) < 2:
        print("Usage: python s3_to_gcs_migration_checker_no_rpc.py <command> [options]")
        print("\nCommands:")
        print("  check <s3_folder_prefix> <chain_name> [options]  - Check migration status")
        print("  debug <report_json> [max_files]                   - Debug problematic files")
        print("\nCheck Options:")
        print("  [max_sample_files]  - Limit number of files to check (for testing)")
        print("  --no-tracking       - Skip loading tracking file")
        print("  --delete            - Delete S3 files after successful migration verification")
        print("  --dry-run-delete    - Simulate deletion without actually deleting files")
        print("  --workers N         - Number of parallel workers (default: 10)")
        print("\nDebug Options:")
        print("  <report_json>       - JSON report file from migration checker")
        print("  [max_files]         - Maximum number of files to debug (default: 10)")
        print("\nExamples:")
        print("  python s3_to_gcs_migration_checker_no_rpc.py check unichain/ unichain")
        print("  python s3_to_gcs_migration_checker_no_rpc.py check unichain/ unichain --workers 20")
        print("  python s3_to_gcs_migration_checker_no_rpc.py debug migration_check_unichain_unichain_.json")
        print("  python s3_to_gcs_migration_checker_no_rpc.py debug migration_check_unichain_unichain_.json 20")
        print("\n🚀 PERFORMANCE IMPROVEMENTS:")
        print("  - Batch deletion: Up to 1000 files per S3 API call (vs 1 file per call)")
        print("  - Parallel processing: Concurrent file checking with configurable workers")
        print("  - Direct parquet reading: Extract timestamps directly from files")
        print("\n🔍 DEBUG MODE: Analyze problematic files to understand why they're failing!")
        print("⚠️  DELETION SAFETY: Files are only deleted when ALL files in the folder are successfully migrated!")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "debug":
        if len(sys.argv) < 3:
            print("Error: debug command requires a report JSON file")
            print("Usage: python s3_to_gcs_migration_checker_no_rpc.py debug <report_json> [max_files]")
            sys.exit(1)
        
        report_json = sys.argv[2]
        max_debug_files = int(sys.argv[3]) if len(sys.argv) >= 4 else 10
        
        print(f"🔍 DEBUG MODE: Analyzing problematic files from {report_json}")
        debug_problematic_files(s3_bucket, report_json, max_debug_files)
        return
    
    elif command == "check":
        if len(sys.argv) < 4:
            print("Error: check command requires s3_folder_prefix and chain_name")
            print("Usage: python s3_to_gcs_migration_checker_no_rpc.py check <s3_folder_prefix> <chain_name> [options]")
            sys.exit(1)
        
        folder_prefix = sys.argv[2]
        chain = sys.argv[3]
        
        # Parse optional arguments
        max_sample_files = None
        skip_tracking = False
        delete_migrated = False
        dry_run_delete = False
        max_workers = 10  # Default number of workers
        
        i = 4
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == '--no-tracking':
                skip_tracking = True
            elif arg == '--delete':
                delete_migrated = True
            elif arg == '--dry-run-delete':
                delete_migrated = True
                dry_run_delete = True
            elif arg == '--workers':
                if i + 1 < len(sys.argv):
                    try:
                        max_workers = int(sys.argv[i + 1])
                        i += 1  # Skip the next argument since we consumed it
                    except ValueError:
                        print(f"Invalid worker count: {sys.argv[i + 1]}")
                        sys.exit(1)
                else:
                    print("--workers requires a number")
                    sys.exit(1)
            else:
                try:
                    max_sample_files = int(arg)
                except ValueError:
                    print(f"Invalid argument: {arg}")
                    sys.exit(1)
            i += 1
        
        print(f"Starting ENHANCED migration check (no RPC required)...")
        if delete_migrated:
            print(f"🗑️  Deletion mode: {'DRY RUN' if dry_run_delete else 'ENABLED (BATCH)'}")
        print(f"🚀 Performance: {max_workers} parallel workers + batch deletion")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        start_time = time.time()
        check_migration(s3_bucket, gcs_bucket, folder_prefix, chain, max_sample_files, skip_tracking, delete_migrated, dry_run_delete, max_workers)
        
        total_time = time.time() - start_time
        print(f"\nCheck completed in {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print(f"🎯 Timestamps extracted directly from parquet files")
        print(f"🚀 Enhanced performance with parallel processing and batch operations")
    
    else:
        print(f"Unknown command: {command}")
        print("Use 'check' or 'debug' commands. See --help for usage.")
        sys.exit(1)

if __name__ == "__main__":
    main()

# USAGE EXAMPLES:
# Check migration status:
# python s3_to_gcs_migration_checker_no_rpc.py check unichain/ unichain --no-tracking
# python s3_to_gcs_migration_checker_no_rpc.py check unichain/ unichain --delete --workers 12
# python s3_to_gcs_migration_checker_no_rpc.py check unichain/ unichain --delete
# python s3_to_gcs_migration_checker_no_rpc.py check unichain/ unichain --dry-run-delete --workers 15

# Debug problematic files:
# python s3_to_gcs_migration_checker_no_rpc.py debug migration_check_unichain_unichain_.json
# python s3_to_gcs_migration_checker_no_rpc.py debug migration_check_unichain_unichain_.json 20
