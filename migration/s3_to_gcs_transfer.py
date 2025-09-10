#!/usr/bin/env python3

import os
import sys
import time
import json
import io
import pandas as pd
import numpy as np
import re
import logging
import signal
import gc
import psutil
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import concurrent.futures
from tqdm import tqdm
import threading
import hashlib

# Import GCS utilities
from backend.src.adapters.rpc_funcs.gcs_utils import connect_to_gcs, save_data_for_range

load_dotenv()

# Setup logging to file with rotation
def setup_logging(chain, folder_prefix):
    """Setup comprehensive logging to file with rotation"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log filename based on chain and folder
    log_filename = f"{chain}_{folder_prefix.replace('/', '_')}_transfer.log"
    log_path = os.path.join(log_dir, log_filename)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting S3 to GCS transfer - Chain: {chain}, Folder: {folder_prefix}")
    logger.info(f"Log file: {log_path}")
    return logger

# Global logger (will be set in main)
logger = None

# Memory monitoring and cleanup
class MemoryMonitor:
    def __init__(self, warning_threshold_mb=2048, critical_threshold_mb=4096):
        self.warning_threshold = warning_threshold_mb * 1024 * 1024  # Convert to bytes
        self.critical_threshold = critical_threshold_mb * 1024 * 1024
        self.process = psutil.Process()
        self.lock = threading.Lock()
        self.last_cleanup = time.time()
        self.cleanup_interval = 60  # seconds - reduced frequency
        
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        memory_info = self.process.memory_info()
        return {
            'rss_mb': memory_info.rss / (1024 * 1024),  # Resident Set Size
            'vms_mb': memory_info.vms / (1024 * 1024),  # Virtual Memory Size
            'percent': self.process.memory_percent()
        }
    
    def check_memory_and_cleanup(self, force_cleanup=False):
        """Check memory usage and perform cleanup if necessary"""
        with self.lock:
            memory = self.get_memory_usage()
            current_time = time.time()
            
            # Log memory usage periodically
            if current_time - self.last_cleanup > self.cleanup_interval or force_cleanup:
                if logger:
                    logger.info(f"Memory usage: {memory['rss_mb']:.1f} MB RSS, {memory['vms_mb']:.1f} MB VMS, {memory['percent']:.1f}%")
                
                # Force garbage collection
                collected = gc.collect()
                if logger:
                    logger.info(f"Garbage collection freed {collected} objects")
                
                self.last_cleanup = current_time
            
            # Check thresholds
            if memory['rss_mb'] * 1024 * 1024 > self.critical_threshold:
                error_msg = f"CRITICAL: Memory usage {memory['rss_mb']:.1f} MB exceeds critical threshold {self.critical_threshold/(1024*1024):.1f} MB"
                if logger:
                    logger.critical(error_msg)
                print(f"🚨 {error_msg}")
                return 'critical'
            elif memory['rss_mb'] * 1024 * 1024 > self.warning_threshold:
                warning_msg = f"WARNING: Memory usage {memory['rss_mb']:.1f} MB exceeds warning threshold {self.warning_threshold/(1024*1024):.1f} MB"
                if logger:
                    logger.warning(warning_msg)
                print(f"⚠️  {warning_msg}")
                return 'warning'
            
            return 'normal'

# Global memory monitor
memory_monitor = None

# Graceful shutdown handling
class GracefulShutdown:
    def __init__(self):
        self.shutdown_requested = False
        self.tracking_manager = None
        self.executor = None
        self.progress_monitor = None
        self.shutdown_in_progress = False
        self.lock = threading.Lock()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        with self.lock:
            # Prevent recursive signal handling
            if self.shutdown_in_progress:
                print("\n🔄 Shutdown already in progress...")
                return
            
            self.shutdown_in_progress = True
            signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
            print(f"\n🛑 Received {signal_name}. Initiating graceful shutdown...")
            if logger:
                logger.info(f"Received {signal_name}. Initiating graceful shutdown...")
            
            self.shutdown_requested = True
        
        # Start shutdown in a separate thread to avoid blocking the signal handler
        shutdown_thread = threading.Thread(target=self._perform_shutdown, daemon=False)
        shutdown_thread.start()
        
        # Wait for shutdown thread with timeout
        shutdown_thread.join(timeout=30)  # 30 second timeout
        
        if shutdown_thread.is_alive():
            print("⚠️  Graceful shutdown timed out, forcing exit...")
            if logger:
                logger.warning("Graceful shutdown timed out, forcing exit")
            os._exit(1)  # Force exit if shutdown hangs
        
    def _perform_shutdown(self):
        """Perform the actual shutdown operations"""
        try:
            # Stop progress monitor first
            if self.progress_monitor:
                print("🔄 Stopping progress monitor...")
                self.progress_monitor.stop()
                self.progress_monitor.join(timeout=5)
            
            # Save tracking data
            if self.tracking_manager:
                print("💾 Saving progress...")
                if logger:
                    logger.info("Saving tracking data before shutdown")
                self.tracking_manager.final_save()
            
            # Shutdown executor with timeout
            if self.executor:
                print("⏹️  Shutting down thread pool...")
                if logger:
                    logger.info("Shutting down thread pool executor")
                
                # Cancel all pending futures
                try:
                    if hasattr(self, 'futures') and self.futures:
                        for future in self.futures:
                            if not future.done():
                                future.cancel()
                        print(f"📋 Cancelled {len([f for f in self.futures if f.cancelled()])} pending tasks")
                except:
                    pass
                
                # Shutdown the executor
                try:
                    # First try graceful shutdown with short timeout
                    self.executor.shutdown(wait=False)
                    
                    # Wait a reasonable time for tasks to complete
                    import time
                    start_time = time.time()
                    timeout = 10  # 10 second timeout
                    
                    while time.time() - start_time < timeout:
                        if hasattr(self.executor, '_shutdown') and self.executor._shutdown:
                            break
                        if hasattr(self.executor, '_threads') and not self.executor._threads:
                            break
                        time.sleep(0.1)
                    
                    # Check if shutdown completed
                    if hasattr(self.executor, '_shutdown') and not self.executor._shutdown:
                        print("⚠️  Executor shutdown timed out, forcing termination")
                        if logger:
                            logger.warning("Executor shutdown timed out, forcing termination")
                        
                        # Force shutdown by setting shutdown flag if possible
                        try:
                            if hasattr(self.executor, '_shutdown'):
                                self.executor._shutdown = True
                        except:
                            pass
                    else:
                        print("✅ Thread pool shutdown completed")
                        
                except Exception as e:
                    print(f"⚠️  Error during executor shutdown: {e}")
                    if logger:
                        logger.warning(f"Error during executor shutdown: {e}")
            
            print("✅ Graceful shutdown complete.")
            if logger:
                logger.info("Graceful shutdown complete")
                
        except Exception as e:
            print(f"❌ Error during shutdown: {e}")
            if logger:
                logger.error(f"Error during shutdown: {e}")
        finally:
            # Force exit
            os._exit(0)
    
    def set_tracking_manager(self, tracking_manager):
        self.tracking_manager = tracking_manager
        
    def set_executor(self, executor):
        self.executor = executor
        
    def set_futures(self, futures):
        self.futures = futures
        
    def set_progress_monitor(self, progress_monitor):
        self.progress_monitor = progress_monitor

# Global shutdown handler
shutdown_handler = GracefulShutdown()

# Thread-local storage for GCS client
thread_local = threading.local()

# Global lock manager for preventing race conditions on specific files
class FileLockManager:
    def __init__(self):
        self.locks = {}
        self.master_lock = threading.Lock()
    
    def get_file_lock(self, file_identifier):
        """Get or create a lock for a specific file identifier"""
        with self.master_lock:
            if file_identifier not in self.locks:
                self.locks[file_identifier] = threading.Lock()
            return self.locks[file_identifier]
    
    def cleanup_lock(self, file_identifier):
        """Remove a lock after processing is complete"""
        with self.master_lock:
            if file_identifier in self.locks:
                del self.locks[file_identifier]

# Global file lock manager
file_lock_manager = FileLockManager()

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

def read_parquet_from_s3(bucket_name, file_key):
    """
    Reads a parquet file directly from S3 into memory with improved memory management
    
    Args:
        bucket_name (str): S3 bucket name
        file_key (str): S3 object key
    
    Returns:
        DataFrame: Pandas DataFrame containing the parquet data
    """
    start_time = time.time()
    buffer = None
    response = None
    
    try:
        # Check memory before processing
        if memory_monitor:
            memory_status = memory_monitor.check_memory_and_cleanup()
            if memory_status == 'critical':
                if logger:
                    logger.error(f"Skipping file {file_key} due to critical memory usage")
                return None, 0, 0
        
        # Get thread-local S3 client
        s3_client = get_s3_client()
        
        # Get object directly to memory with proper resource management
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read all data first, then close the response
        data = response['Body'].read()
        response['Body'].close()
        
        # Create buffer from data
        buffer = io.BytesIO(data)
        
        # Read parquet from the buffer
        df = pd.read_parquet(buffer)
        buffer.close()  # Close buffer immediately after reading
        
        download_time = time.time() - start_time
        file_size_mb = len(data) / (1024 * 1024)
        
        # Log memory usage for large files
        if file_size_mb > 50 and logger:
            logger.info(f"Loaded large file {file_key}: {file_size_mb:.1f} MB")
        
        return df, file_size_mb, download_time
        
    except Exception as e:
        error_msg = f"Error reading parquet from S3 {file_key}: {str(e)}"
        if logger:
            logger.error(error_msg)
        return None, 0, 0
        
    finally:
        # Clean up resources - more conservative cleanup
        try:
            if 'buffer' in locals() and buffer:
                buffer.close()
        except:
            pass
            
        try:
            if 'response' in locals() and response and hasattr(response, 'Body'):
                response['Body'].close()
        except:
            pass  # Already closed
        
        # Force garbage collection for large files
        try:
            if 'data' in locals() and len(data) > 50 * 1024 * 1024:  # 50MB threshold
                gc.collect()
        except:
            pass

def list_files_in_folder(bucket_name, folder_prefix, use_cache=True):
    """
    Lists all files in a specific folder within the S3 bucket with caching support
    
    Args:
        bucket_name (str): S3 bucket name
        folder_prefix (str): Folder prefix to list
        use_cache (bool): Whether to use cached file listing
    
    Returns:
        list: List of file keys
    """
    # Create cache filename
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_filename = f"{bucket_name}_{folder_prefix.replace('/', '_')}_files.json"
    cache_path = os.path.join(cache_dir, cache_filename)
    
    # Try to load from cache first
    if use_cache and os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)
                files = cache_data.get('files', [])
                cache_time = cache_data.get('timestamp', 0)
                
            if logger:
                logger.info(f"Loaded {len(files)} files from cache (created: {datetime.fromtimestamp(cache_time)})")
            print(f"Loaded {len(files)} files from cache (created: {datetime.fromtimestamp(cache_time)})")
            return files
        except Exception as e:
            if logger:
                logger.warning(f"Failed to load cache file: {e}. Will re-list from S3.")
            print(f"Failed to load cache file: {e}. Will re-list from S3.")
    
    print(f"Listing files in folder '{folder_prefix}' from S3...")
    if logger:
        logger.info(f"Listing files in folder '{folder_prefix}' from S3...")
    
    start_time = time.time()
    try:
        # Create S3 client
        s3_client = get_s3_client()
        
        # Ensure folder_prefix ends with a slash
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        # Get all objects with the specified prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        result = paginator.paginate(
            Bucket=bucket_name,
            Prefix=folder_prefix
        )
        
        files = []
        page_count = 0
        for page in result:
            page_count += 1
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Skip the folder itself (which appears as a key)
                    if obj["Key"] != folder_prefix:
                        files.append(obj["Key"])
            
            # Force garbage collection every 100 pages to prevent memory buildup
            if page_count % 100 == 0:
                gc.collect()
                if logger:
                    logger.info(f"Processed {page_count} pages, found {len(files)} files so far")
        
        # Final garbage collection after listing
        gc.collect()
        
        list_time = time.time() - start_time
        print(f"Found {len(files)} files in {list_time:.2f} seconds")
        if logger:
            logger.info(f"Found {len(files)} files in {list_time:.2f} seconds")
        
        # Cache the results
        try:
            cache_data = {
                'files': files,
                'timestamp': time.time(),
                'bucket': bucket_name,
                'folder_prefix': folder_prefix,
                'count': len(files)
            }
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)
            if logger:
                logger.info(f"Cached file listing to {cache_path}")
            print(f"Cached file listing to {cache_path}")
        except Exception as e:
            if logger:
                logger.warning(f"Failed to cache file listing: {e}")
            print(f"Warning: Failed to cache file listing: {e}")
        
        return files
        
    except ClientError as e:
        error_msg = f"Error listing files in S3: {e}"
        print(error_msg)
        if logger:
            logger.error(error_msg)
        return []

def extract_block_info(file_key):
    """
    Extracts block range from filename
    
    Args:
        file_key (str): S3 object key
    
    Returns:
        tuple: (block_start, block_end)
    """
    file_name = os.path.basename(file_key)
    
    try:
        # Handle format: optimism_tx_101157267-101177266.parquet
        if '-' in file_name:
            # Split by underscore and find the part with hyphen
            name_parts = file_name.split('_')
            for part in name_parts:
                if '-' in part:
                    # Remove .parquet extension and split by hyphen
                    range_part = part.split('.')[0]
                    block_start, block_end = range_part.split('-')
                    return int(block_start), int(block_end)
        
        # Fallback: Handle format with underscores: optimism_tx_101157267_101177266.parquet
        name_parts = file_name.split('_')
        if len(name_parts) >= 3:
            block_start = int(name_parts[-2])
            block_end = int(name_parts[-1].split('.')[0])
            return block_start, block_end
            
    except (ValueError, IndexError) as e:
        print(f"Warning: Could not extract block info from filename '{file_name}': {e}")

def generate_gcs_file_identifier(df, block_start, block_end, chain):
    """
    Generate a unique identifier for the GCS file that would be created
    This helps us create file-specific locks to prevent race conditions
    """
    # Generate the filename
    filename = f"{chain}_tx_{block_start}_{block_end}.parquet"
    
    # Determine which timestamp column to use for date
    timestamp_col = None
    
    # List of potential timestamp column names (in order of preference)
    timestamp_candidates = [
        'block_timestamp',
        'block_time', 
        'timestamp',
    ]
    
    # Check for timestamp column (case-insensitive)
    for candidate in timestamp_candidates:
        for col in df.columns:
            if col.lower() == candidate.lower():
                timestamp_col = col
                break
        if timestamp_col:
            break
    
    # Get date_str based on available timestamp column
    if timestamp_col and not df.empty:
        # Use the timestamp from the first row
        block_timestamp = df[timestamp_col].iloc[0]
        
        # Convert timestamp to date format YYYY-MM-DD
        try:
            # Handle different timestamp formats
            if isinstance(block_timestamp, (int, float, np.int64, np.float64)):
                # Unix timestamp (seconds since epoch)
                timestamp_value = float(block_timestamp)
                date_str = datetime.fromtimestamp(timestamp_value).strftime("%Y-%m-%d")
            else:
                # String timestamp or datetime object
                if isinstance(block_timestamp, str):
                    # Try to parse the string timestamp
                    try:
                        date_str = datetime.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                    except ValueError:
                        try:
                            # Try format with microseconds and UTC: '2023-05-23 12:06:15.000000 UTC'
                            date_str = datetime.strptime(block_timestamp, "%Y-%m-%d %H:%M:%S.%f UTC").strftime("%Y-%m-%d")
                        except ValueError:
                            # Try ISO format - normalize fractional seconds to microseconds
                            timestamp_str = block_timestamp.replace('Z', '+00:00')
                            
                            # Handle fractional seconds of varying precision
                            # Pattern: YYYY-MM-DDTHH:MM:SS.fractional+TZ
                            fractional_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)(\+\d{2}:\d{2})'
                            match = re.match(fractional_pattern, timestamp_str)
                            
                            if match:
                                date_part = match.group(1)
                                fractional_part = match.group(2)
                                timezone_part = match.group(3)
                                
                                # Normalize fractional seconds to exactly 6 digits (microseconds)
                                if len(fractional_part) > 6:
                                    # Truncate to 6 digits (nanoseconds -> microseconds)
                                    fractional_part = fractional_part[:6]
                                elif len(fractional_part) < 6:
                                    # Pad with zeros to reach 6 digits
                                    fractional_part = fractional_part.ljust(6, '0')
                                
                                timestamp_str = f"{date_part}.{fractional_part}{timezone_part}"
                            
                            date_str = datetime.fromisoformat(timestamp_str).strftime("%Y-%m-%d")
                else:
                    # Assume it's already a datetime object
                    date_str = block_timestamp.strftime("%Y-%m-%d")
        except Exception:
            # Fallback to current date if there's an error parsing the timestamp
            date_str = time.strftime("%Y-%m-%d")
    else:
        # Fallback to current date if timestamp column doesn't exist
        date_str = time.strftime("%Y-%m-%d")
    
    # Expected file path in GCS
    file_path = f"{chain}/{date_str}/{filename}"
    return file_path

# Optimized tracking system using in-memory cache and batch writes
class TrackingManager:
    def __init__(self, tracking_file):
        self.tracking_file = tracking_file
        self.processed_files = set()
        self.pending_files = set()
        self.lock = threading.Lock()
        self.last_save_time = time.time()
        self.save_interval = 10  # Save every 10 seconds
        self.save_batch_size = 50  # Or when 50 new files are processed
        self.shutdown_event = threading.Event()
        
        # Load existing processed files once at startup
        self._load_processed_files()
        
        # Start background saver thread
        self.saver_thread = threading.Thread(target=self._background_save, daemon=False)
        self.saver_thread.start()
    
    def _load_processed_files(self):
        """Load processed files from disk once at startup"""
        try:
            if os.path.exists(self.tracking_file):
                print(f"Loading tracking file: {self.tracking_file}")
                start_time = time.time()
                with open(self.tracking_file, 'r') as f:
                    file_list = json.load(f)
                    self.processed_files = set(file_list)
                load_time = time.time() - start_time
                print(f"Loaded {len(self.processed_files)} processed files in {load_time:.2f} seconds")
            else:
                print(f"No existing tracking file found. Starting fresh.")
                self.processed_files = set()
        except Exception as e:
            print(f"Error loading tracking file: {e}. Starting fresh.")
            self.processed_files = set()
    
    def is_processed(self, file_key):
        """Check if a file has been processed (fast in-memory lookup)"""
        with self.lock:
            return file_key in self.processed_files
    
    def mark_processed(self, file_key):
        """Mark a file as processed (fast in-memory operation)"""
        with self.lock:
            self.processed_files.add(file_key)
            self.pending_files.add(file_key)
            
            # Save if we have enough pending files or enough time has passed
            if (len(self.pending_files) >= self.save_batch_size or 
                time.time() - self.last_save_time >= self.save_interval):
                self._save_now()
    
    def _save_now(self):
        """Save pending files to disk (called with lock held)"""
        if self.pending_files:
            try:
                with open(self.tracking_file, 'w') as f:
                    json.dump(list(self.processed_files), f)
                self.pending_files.clear()
                self.last_save_time = time.time()
            except Exception as e:
                print(f"Error saving tracking file: {e}")
    
    def _background_save(self):
        """Background thread to periodically save changes"""
        while not self.shutdown_event.is_set():
            # Use wait() instead of sleep to allow for early termination
            if self.shutdown_event.wait(self.save_interval):
                break  # Shutdown was requested
            
            with self.lock:
                if self.pending_files:
                    self._save_now()
    
    def final_save(self):
        """Final save when shutting down"""
        # Signal background thread to stop
        self.shutdown_event.set()
        
        # Wait for background thread to finish
        if self.saver_thread.is_alive():
            self.saver_thread.join(timeout=5)
        
        # Final save
        with self.lock:
            self._save_now()
    
    def get_processed_count(self):
        """Get count of processed files"""
        with self.lock:
            return len(self.processed_files)

def check_file_exists_in_gcs(df, block_start, block_end, chain, bucket_name):
    """
    Checks if a file with the same data already exists in GCS.
    Attempts to determine the expected GCS path based on the same logic used by save_data_for_range.
    
    Args:
        df (pd.DataFrame): The DataFrame to check for existing data
        block_start (int): Start block number
        block_end (int): End block number
        chain (str): Chain name
        bucket_name (str): GCS bucket name
        
    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        # Get thread-local GCS client
        gcs = get_gcs_client()
        bucket = gcs.bucket(bucket_name)
        
        # Get the expected file path
        file_path = generate_gcs_file_identifier(df, block_start, block_end, chain)
        
        # Check if file exists
        blob = bucket.blob(file_path)
        exists = blob.exists()
        
        return exists, file_path
    except Exception as e:
        # Default to False so we attempt to process the file
        return False, None

def safe_save_data_for_range(df, block_start, block_end, chain, bucket_name, max_retries=3):
    """
    Thread-safe version of save_data_for_range with retry logic and race condition prevention
    
    Args:
        df (pd.DataFrame): The DataFrame containing transaction data
        block_start (int): Start block number
        block_end (int): End block number
        chain (str): Chain name
        bucket_name (str): GCS bucket name
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Generate unique identifier for this file
    file_identifier = generate_gcs_file_identifier(df, block_start, block_end, chain)
    
    # Get file-specific lock to prevent race conditions
    file_lock = file_lock_manager.get_file_lock(file_identifier)
    
    with file_lock:
        try:
            # Double-check if file exists before proceeding (within the lock)
            exists, _ = check_file_exists_in_gcs(df, block_start, block_end, chain, bucket_name)
            if exists:
                # File was created by another thread while we were waiting
                return True
            
            # Attempt to save with retries
            for attempt in range(max_retries):
                try:
                    save_data_for_range(df, block_start, block_end, chain, bucket_name)
                    return True
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Check if it's a 409 conflict error (race condition)
                    if "409" in error_msg and "conflict" in error_msg.lower():
                        print(f"Conflict detected on attempt {attempt + 1}, checking if file now exists...")
                        # Wait a bit and check if the file exists now
                        time.sleep(0.5)
                        exists, _ = check_file_exists_in_gcs(df, block_start, block_end, chain, bucket_name)
                        if exists:
                            print(f"File was successfully created by another process")
                            return True
                    
                    # For other errors or if file still doesn't exist, retry
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        print(f"Attempt {attempt + 1} failed, retrying in {wait_time}s: {error_msg}")
                        time.sleep(wait_time)
                    else:
                        print(f"All {max_retries} attempts failed for {file_identifier}: {error_msg}")
                        raise e
            
            return False
            
        finally:
            # Clean up the lock after processing
            file_lock_manager.cleanup_lock(file_identifier)

# Global counters for statistics with thread-safe access
class Statistics:
    def __init__(self):
        self.lock = threading.Lock()
        self.processed_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.total_size_mb = 0
        self.total_time_spent = 0
        self.start_time = time.time()
        self.last_progress_time = time.time()
        self.last_processed = 0
        self.times = []  # List to store processing times
        
    def increment_processed(self, file_time=0):
        with self.lock:
            self.processed_count += 1
            self.total_time_spent += file_time
            self.times.append(file_time)
            
    def increment_error(self):
        with self.lock:
            self.error_count += 1
            
    def increment_skipped(self):
        with self.lock:
            self.skipped_count += 1
            
    def add_size(self, size_mb):
        with self.lock:
            self.total_size_mb += size_mb
            
    def get_progress_stats(self, total_files):
        """Gets stats about progress and estimates remaining time"""
        with self.lock:
            elapsed = time.time() - self.start_time
            completed = self.processed_count + self.skipped_count
            remaining = total_files - completed - self.error_count
            
            # Calculate processing rate
            if self.processed_count > 0:
                # Calculate average time per file
                avg_time_per_file = self.total_time_spent / self.processed_count
                
                # Calculate processing rate (files per second)
                rate = self.processed_count / elapsed if elapsed > 0 else 0
                
                # Get moving average of last 10 files or available files
                recent_times = self.times[-10:] if len(self.times) > 10 else self.times
                recent_avg_time = sum(recent_times) / len(recent_times) if recent_times else avg_time_per_file
                
                # Estimate time remaining
                eta_seconds = remaining * recent_avg_time if rate > 0 else float('inf')
                eta_str = str(timedelta(seconds=int(eta_seconds))) if eta_seconds != float('inf') else "unknown"
                
                # Calculate effective parallel speedup
                sequential_est = self.total_time_spent
                parallel_speedup = sequential_est / elapsed if elapsed > 0 else 0
                
                # Calculate progress since last check
                time_since_last = time.time() - self.last_progress_time
                files_since_last = self.processed_count - self.last_processed
                current_rate = files_since_last / time_since_last if time_since_last > 0 else 0
                
                # Update last check values
                self.last_progress_time = time.time()
                self.last_processed = self.processed_count
                
                return {
                    'elapsed': elapsed,
                    'completed': completed,
                    'remaining': remaining,
                    'avg_time_per_file': avg_time_per_file,
                    'recent_avg_time': recent_avg_time,
                    'processing_rate': rate,
                    'current_rate': current_rate,
                    'eta_seconds': eta_seconds,
                    'eta_str': eta_str,
                    'parallel_speedup': parallel_speedup,
                    'success_rate': (self.processed_count / total_files) * 100 if total_files > 0 else 0
                }
            else:
                return {
                    'elapsed': elapsed,
                    'completed': completed,
                    'remaining': remaining,
                    'avg_time_per_file': 0,
                    'recent_avg_time': 0,
                    'processing_rate': 0,
                    'current_rate': 0,
                    'eta_seconds': float('inf'),
                    'eta_str': 'unknown',
                    'parallel_speedup': 0,
                    'success_rate': 0
                }

def process_file(args):
    """
    Process a single file - designed for concurrent execution with improved memory management
    
    Args:
        args: Tuple containing (file_key, block_info, s3_bucket, gcs_bucket, chain, tracking_manager, stats)
        
    Returns:
        dict: Statistics about the processing
    """
    file_key, (block_start, block_end), s3_bucket, gcs_bucket, chain, tracking_manager, stats = args
    
    result = {
        'file_key': file_key,
        'status': 'error',
        'time': 0,
        'size_mb': 0
    }
    
    file_start_time = time.time()
    df = None
    
    try:
        # Check for shutdown signal
        if shutdown_handler.shutdown_requested:
            result['status'] = 'cancelled'
            return result
        
        # Check memory before processing
        if memory_monitor:
            memory_status = memory_monitor.check_memory_and_cleanup()
            if memory_status == 'critical':
                result['status'] = 'memory_critical'
                result['error'] = 'Skipped due to critical memory usage'
                if logger:
                    logger.error(f"Skipping {file_key} due to critical memory usage")
                return result
        
        # Check for shutdown signal before expensive operations
        if shutdown_handler.shutdown_requested:
            result['status'] = 'cancelled'
            return result
            
        # Read file directly from S3 to memory
        df, file_size_mb, download_time = read_parquet_from_s3(s3_bucket, file_key)
        
        if df is None:
            stats.increment_error()
            result['status'] = 'error'
            result['error'] = 'Failed to read from S3'
            if logger:
                logger.error(f"Failed to read {file_key} from S3")
            return result
            
        result['size_mb'] = file_size_mb
        stats.add_size(file_size_mb)
        
        if df.empty:
            # Mark as processed so we don't retry
            tracking_manager.mark_processed(file_key)
            result['status'] = 'empty'
            if logger:
                logger.info(f"File {file_key} is empty, marking as processed")
            return result
        
        # Check if the file already exists in GCS
        exists, file_path = check_file_exists_in_gcs(df, block_start, block_end, chain, gcs_bucket)
        if exists:
            tracking_manager.mark_processed(file_key)
            stats.increment_skipped()
            result['status'] = 'skipped'
            result['gcs_path'] = file_path
            if logger:
                logger.info(f"File {file_key} already exists at {file_path}, skipping")
            return result
        
        # Check for shutdown signal before expensive GCS operation
        if shutdown_handler.shutdown_requested:
            result['status'] = 'cancelled'
            return result
            
        # Save data to GCS using the thread-safe version
        save_start = time.time()
        success = safe_save_data_for_range(df, block_start, block_end, chain, gcs_bucket)
        save_time = time.time() - save_start
        
        if not success:
            stats.increment_error()
            result['status'] = 'error'
            result['error'] = 'Failed to save to GCS after retries'
            if logger:
                logger.error(f"Failed to save {file_key} to GCS after retries")
            return result
        
        # Mark as processed
        tracking_manager.mark_processed(file_key)
        
        # Calculate processing time
        file_time = time.time() - file_start_time
        
        # Update stats
        stats.increment_processed(file_time)
        
        result['time'] = file_time
        result['status'] = 'processed'
        result['download_time'] = download_time
        result['save_time'] = save_time
        
        # Only log large files or errors to reduce log volume
        if file_size_mb > 1.0 and logger:  # Only log files larger than 1MB
            logger.info(f"Successfully processed {file_key} in {file_time:.2f}s ({file_size_mb:.2f} MB)")
        
        return result
        
    except Exception as e:
        stats.increment_error()
        result['error'] = str(e)
        error_msg = f"Error processing {file_key}: {str(e)}"
        if logger:
            logger.error(error_msg)
        return result
        
    finally:
        # Clean up DataFrame to free memory
        if df is not None:
            del df
        
        # Force garbage collection for memory cleanup
        if result.get('size_mb', 0) > 50:  # For files larger than 50MB
            gc.collect()

# Thread for printing progress information periodically
class ProgressMonitor(threading.Thread):
    def __init__(self, stats, total_files, interval=10):
        threading.Thread.__init__(self)
        self.stats = stats
        self.total_files = total_files
        self.interval = interval
        self.daemon = True  # Daemon thread will exit when main thread exits
        self.stop_event = threading.Event()
        
    def run(self):
        while not self.stop_event.is_set():
            # Check shutdown signal first
            if shutdown_handler.shutdown_requested:
                break
                
            progress = self.stats.get_progress_stats(self.total_files)
            
            if progress['processing_rate'] > 0:
                # Print progress information
                eta_completion = datetime.now() + timedelta(seconds=progress['eta_seconds'])
                eta_time_str = eta_completion.strftime('%Y-%m-%d %H:%M:%S')
                
                print("\n" + "="*30 + " PROGRESS REPORT " + "="*30)
                print(f"Time elapsed: {timedelta(seconds=int(progress['elapsed']))}")
                print(f"Files processed: {self.stats.processed_count}/{self.total_files} " + 
                      f"({progress['success_rate']:.1f}%)")
                print(f"Files skipped: {self.stats.skipped_count}, Errors: {self.stats.error_count}")
                print(f"Current processing rate: {progress['current_rate']:.2f} files/sec " +
                      f"(avg: {progress['processing_rate']:.2f} files/sec)")
                print(f"Remaining files: {progress['remaining']}")
                print(f"Estimated time to completion: {progress['eta_str']}")
                print(f"Estimated completion at: {eta_time_str}")
                print(f"Parallel speedup: {progress['parallel_speedup']:.2f}x")
                print(f"Total data transferred: {self.stats.total_size_mb:.2f} MB")
                print("="*80)
            
            # Sleep for the specified interval
            self.stop_event.wait(self.interval)
            
    def stop(self):
        self.stop_event.set()

def process_s3_files_to_gcs(s3_bucket, gcs_bucket, folder_prefix, chain, max_workers=10, progress_interval=30, use_cache=True, batch_size=10000):
    """
    Processes files from S3 folder in parallel with improved memory management and caching:
    1. Reads files directly into memory
    2. Uploads to GCS with proper date structure
    3. Tracks processed files for resumable transfers
    4. Caches file listings to avoid re-listing on restart
    5. Monitors memory usage and handles cleanup
    6. Processes files in batches to prevent memory overload
    
    Args:
        s3_bucket (str): S3 bucket name
        gcs_bucket (str): GCS bucket name
        folder_prefix (str): Folder in S3 bucket to process
        chain (str): Blockchain chain name for GCS path
        max_workers (int): Maximum number of parallel workers
        progress_interval (int): How often to print progress updates in seconds
        use_cache (bool): Whether to use cached file listings
        batch_size (int): Number of files to process in each batch
    """
    global memory_monitor
    overall_start_time = time.time()
    
    # Initialize memory monitor with realistic thresholds for large datasets
    memory_monitor = MemoryMonitor(warning_threshold_mb=2048, critical_threshold_mb=3072)
    if logger:
        logger.info("Memory monitor initialized with 2GB warning, 3GB critical thresholds")
    
    # Get files in folder (with caching)
    files = list_files_in_folder(s3_bucket, folder_prefix, use_cache=use_cache)
    
    if not files:
        print(f"No files found in {folder_prefix}")
        return
    
    # CRITICAL: Clear the files list from memory immediately after getting count
    total_files_count = len(files)
    if logger:
        logger.info(f"Total files found: {total_files_count}. Processing in batches of {batch_size}")
    
    # Process files in batches to avoid memory overload
    return process_files_in_batches(s3_bucket, gcs_bucket, folder_prefix, chain, files, 
                                   max_workers, progress_interval, batch_size, overall_start_time)

def process_files_in_batches(s3_bucket, gcs_bucket, folder_prefix, chain, all_files, 
                           max_workers, progress_interval, batch_size, overall_start_time):
    """
    Process files in batches to manage memory usage
    """
    total_files = len(all_files)
    
    # Initialize optimized tracking manager
    tracking_file = f"{chain}_{folder_prefix.replace('/', '_')}_transferred.json"
    print(f"Using tracking file: {tracking_file}")
    tracking_manager = TrackingManager(tracking_file)
    
    # Statistics tracking
    stats = Statistics()
    
    # Setup shutdown handler
    shutdown_handler.set_tracking_manager(tracking_manager)
    
    processed_total = 0
    
    # Initialize overall progress bar
    already_processed = tracking_manager.get_processed_count()
    overall_pbar = tqdm(
        total=total_files,
        initial=already_processed,
        desc="Overall Progress",
        unit="file",
        dynamic_ncols=True,
        position=0,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {desc}"
    )
    
    # Process in batches
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch_files = all_files[batch_start:batch_end]
        
        batch_num = (batch_start // batch_size) + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        print(f"\n{'='*50}")
        print(f"PROCESSING BATCH {batch_num}/{total_batches}")
        print(f"Files {batch_start + 1} to {batch_end} of {total_files}")
        print(f"Batch size: {len(batch_files)} files")
        print(f"{'='*50}")
        
        if logger:
            logger.info(f"Starting batch {batch_num}/{total_batches}: files {batch_start + 1}-{batch_end}")
        
        # Check memory before processing batch
        if memory_monitor:
            memory_status = memory_monitor.check_memory_and_cleanup(force_cleanup=True)
            if memory_status == 'critical':
                print(f"🚨 Critical memory usage detected before batch {batch_num}. Attempting cleanup...")
                if logger:
                    logger.critical(f"Critical memory usage before batch {batch_num}")
                
                # Force aggressive cleanup
                gc.collect()
                time.sleep(1)  # Give system time to free memory
                
                # Check again
                memory_status = memory_monitor.check_memory_and_cleanup(force_cleanup=True)
                if memory_status == 'critical':
                    print(f"💥 Unable to free enough memory. Stopping at batch {batch_num}")
                    if logger:
                        logger.critical(f"Unable to free memory, stopping at batch {batch_num}")
                    break
        
        # Process this batch
        batch_processed = process_single_batch(s3_bucket, gcs_bucket, chain, batch_files, 
                                             tracking_manager, stats, max_workers, 
                                             progress_interval, batch_num, processed_total, total_files,
                                             overall_pbar)
        
        processed_total += batch_processed
        
        # Update overall progress bar
        overall_pbar.set_description(f"Overall Progress - Batch {batch_num}/{(total_files + batch_size - 1) // batch_size} completed")
        
        # Force cleanup after each batch
        if memory_monitor:
            memory_monitor.check_memory_and_cleanup(force_cleanup=True)
        
        # Clear batch from memory
        del batch_files
        gc.collect()
        
        if logger:
            logger.info(f"Completed batch {batch_num}/{total_batches}. Total processed: {processed_total}")
        
        # Check for shutdown
        if shutdown_handler.shutdown_requested:
            print("🛑 Shutdown requested, stopping batch processing")
            break
    
    # Clear the full file list from memory
    del all_files
    gc.collect()
    
    # Close overall progress bar
    overall_pbar.close()
    
    # Final save of tracking data
    tracking_manager.final_save()
    
    # Calculate and print overall statistics
    overall_time = time.time() - overall_start_time
    success_rate = (stats.processed_count / total_files) * 100 if total_files else 0
    
    print("\n" + "="*50)
    print(f"TRANSFER COMPLETE")
    print(f"Total files processed: {stats.processed_count}/{total_files} ({success_rate:.1f}%)")
    print(f"Files skipped (already exist): {stats.skipped_count}")
    print(f"Files with errors: {stats.error_count}")
    print(f"Total data transferred: {stats.total_size_mb:.2f} MB")
    print(f"Total time: {overall_time:.2f} seconds ({overall_time/60:.2f} minutes)")
    print("="*50)
    
    if logger:
        logger.info(f"Transfer complete: {stats.processed_count}/{total_files} processed, "
                   f"{stats.skipped_count} skipped, {stats.error_count} errors")

def process_single_batch(s3_bucket, gcs_bucket, chain, batch_files, tracking_manager, 
                        stats, max_workers, progress_interval, batch_num, processed_so_far, total_files, overall_pbar):
    """
    Process a single batch of files
    """
    # Filter out already processed files (fast in-memory lookup)
    files_to_process = [f for f in batch_files if not tracking_manager.is_processed(f)]
    
    if not files_to_process:
        print(f"All files in batch {batch_num} have already been processed.")
        return 0
    
    # Sort files by block number for this batch
    files_with_blocks = [(f, extract_block_info(f)) for f in files_to_process]
    sorted_files = sorted(files_with_blocks, key=lambda x: x[1][0] if x[1] else (0, 0))
    
    batch_size = len(sorted_files)
    print(f"Processing {batch_size} new files in batch {batch_num}")
    
    processed_in_batch = 0
    
    # Process files in parallel with reduced worker count for memory management
    effective_workers = min(max_workers, 3)  # Limit workers to prevent memory issues
    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
        # Set executor in shutdown handler
        shutdown_handler.set_executor(executor)
        
        # Prepare arguments for each file
        args_list = [(file_key, block_info, s3_bucket, gcs_bucket, chain, tracking_manager, stats) 
                     for file_key, block_info in sorted_files]
        
        # Submit all tasks
        futures = {executor.submit(process_file, args): args[0] for args in args_list}
        
        # Set futures in shutdown handler for cancellation
        shutdown_handler.set_futures(list(futures.keys()))
        
        # Process completed futures
        for future in concurrent.futures.as_completed(futures):
            # Check for shutdown signal
            if shutdown_handler.shutdown_requested:
                if logger:
                    logger.info("Shutdown requested, stopping batch processing")
                # Cancel remaining futures
                for remaining_future in futures:
                    if not remaining_future.done():
                        remaining_future.cancel()
                break
                
            file_key = futures[future]
            try:
                result = future.result()
                
                # Update counters
                processed_in_batch += 1
                
                # Update overall progress bar
                overall_pbar.update(1)
                
                # Calculate overall progress info
                total_processed = processed_so_far + processed_in_batch
                remaining_total = total_files - total_processed
                
                # Update overall progress description with current file info
                status = result['status']
                filename = file_key.split('/')[-1]  # Get just the filename
                if status == 'processed':
                    overall_pbar.set_description(f"Overall Progress - Processing: {filename} ({result['size_mb']:.2f} MB)")
                elif status == 'skipped':
                    overall_pbar.set_description(f"Overall Progress - Skipped: {filename}")
                elif status == 'memory_critical':
                    overall_pbar.set_description(f"Overall Progress - Memory Critical - Remaining: {remaining_total}")
                elif status == 'error':
                    overall_pbar.set_description(f"Overall Progress - Error: {filename}")
                
                # Only print significant events to reduce clutter
                if status == 'memory_critical':
                    tqdm.write(f"🚨 Skipped {file_key} due to critical memory usage")
                elif status == 'error':
                    tqdm.write(f"✗ Error processing {file_key}: {result.get('error', 'unknown error')}")
                
                # Check memory less frequently to reduce log clutter
                if processed_in_batch % 50 == 0 and memory_monitor:
                    memory_monitor.check_memory_and_cleanup()
                
            except Exception as e:
                error_msg = f"✗ Error with {file_key}: {str(e)}"
                tqdm.write(error_msg)
                if logger:
                    logger.error(error_msg)
                processed_in_batch += 1
                overall_pbar.update(1)
    
    return processed_in_batch

def main():
    global logger
    start_time = time.time()
    
    # Load environment variables
    s3_bucket = os.getenv("S3_LONG_TERM_BUCKET")
    gcs_bucket = os.getenv("GCS_BUCKET_NAME")
    
    if not s3_bucket or not gcs_bucket:
        print("Error: S3_LONG_TERM_BUCKET or GCS_BUCKET_NAME environment variables not set")
        sys.exit(1)
    
    # Check arguments
    if len(sys.argv) < 3:
        print("Usage: python s3_to_gcs_transfer.py <s3_folder_prefix> <chain_name> [max_workers] [progress_interval] [batch_size] [--no-cache]")
        print("  --no-cache: Skip using cached file listings and force re-listing from S3")
        print("  batch_size: Number of files to process in each batch (default: 5000)")
        sys.exit(1)
    
    folder_prefix = sys.argv[1]
    chain = sys.argv[2]
    
    # Check for --no-cache flag
    use_cache = True
    if "--no-cache" in sys.argv:
        use_cache = False
        sys.argv.remove("--no-cache")
        print("Cache disabled: Will re-list files from S3")
    
    # Setup logging first
    logger = setup_logging(chain, folder_prefix)
    
    # Optional parallel workers argument
    max_workers = 10  # Default value
    if len(sys.argv) >= 4:
        try:
            max_workers = int(sys.argv[3])
        except ValueError:
            print(f"Invalid value for max_workers: {sys.argv[3]}. Using default of 10.")
            if logger:
                logger.warning(f"Invalid max_workers value: {sys.argv[3]}, using default of 10")
    
    # Optional progress interval
    progress_interval = 30  # Default value (seconds)
    if len(sys.argv) >= 5:
        try:
            progress_interval = int(sys.argv[4])
        except ValueError:
            print(f"Invalid value for progress_interval: {sys.argv[4]}. Using default of 30 seconds.")
            if logger:
                logger.warning(f"Invalid progress_interval value: {sys.argv[4]}, using default of 30 seconds")
    
    # Optional batch size
    batch_size = 1000  # Default value - MUCH smaller batches for memory management
    if len(sys.argv) >= 6:
        try:
            batch_size = int(sys.argv[5])
        except ValueError:
            print(f"Invalid value for batch_size: {sys.argv[5]}. Using default of 1000.")
            if logger:
                logger.warning(f"Invalid batch_size value: {sys.argv[5]}, using default of 1000")
    
    print(f"Starting transfer from S3 bucket '{s3_bucket}' folder '{folder_prefix}' to GCS bucket '{gcs_bucket}'")
    print(f"Chain: {chain}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Progress updates will be shown every {progress_interval} seconds")
    print(f"Using cache: {use_cache}")
    print(f"Max workers: {max_workers}")
    print(f"Batch size: {batch_size} files per batch")
    print("-"*50)
    
    if logger:
        logger.info(f"Transfer configuration - S3: {s3_bucket}/{folder_prefix}, GCS: {gcs_bucket}, Chain: {chain}")
        logger.info(f"Workers: {max_workers}, Progress interval: {progress_interval}s, Use cache: {use_cache}, Batch size: {batch_size}")
    
    try:
        process_s3_files_to_gcs(s3_bucket, gcs_bucket, folder_prefix, chain, max_workers, progress_interval, use_cache, batch_size)
        
        total_time = time.time() - start_time
        completion_msg = f"Script execution completed in {total_time:.2f} seconds ({total_time/60:.2f} minutes)"
        print(f"\n{completion_msg}")
        if logger:
            logger.info(completion_msg)
            
    except Exception as e:
        error_msg = f"Fatal error during execution: {str(e)}"
        print(f"💥 {error_msg}")
        if logger:
            logger.critical(error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main() 

#python s3_to_gcs_transfer.py arbitrum/ arbitrum 10 15