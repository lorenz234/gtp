#!/usr/bin/env python3
"""
User Operations Backfill Script

This script backfills user operations for all previous transactions stored in GCS.
It processes chains one by one, going through their parquet files day by day to avoid
loading everything into memory at once.

Usage:
    python backfill_user_ops.py [--chain CHAIN_NAME] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--dry-run]

Examples:
    python backfill_user_ops.py                           # Process all chains
    python backfill_user_ops.py --chain ethereum          # Process only Ethereum
    python backfill_user_ops.py --chain ethereum --start-date 2024-01-01 --end-date 2024-01-31  # Date range
    python backfill_user_ops.py --dry-run                 # Preview what would be processed
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime
import io
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f"Loaded .env file from: {env_path}")

# Add the backend directory to the path so we can import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from adapters.rpc_funcs.gcs_utils import connect_to_gcs
from adapters.rpc_funcs.utils import (
    prep_dataframe_new,
    process_user_ops_for_transactions,
    connect_to_node,
    get_chain_config,
    load_4bytes_data,
    create_4byte_lookup
)
from db_connector import DbConnector
from main_config import get_main_config


class UserOpsBackfiller:
    """
    Backfills user operations for transaction data stored in GCS.
    Processes chains day by day to manage memory usage efficiently.
    """

    def __init__(self):
        """Initialize the backfiller with GCS and DB connections."""
        self.gcs, self.bucket_name = connect_to_gcs()
        self.bucket = self.gcs.bucket(self.bucket_name)
        self.db_connector = DbConnector()
        self.main_config = get_main_config()

        # Store the backend directory path for 4bytes.parquet
        self.backend_dir = os.path.join(os.path.dirname(__file__), '..', '..')
        self.backend_dir = os.path.abspath(self.backend_dir)

    def get_available_chains(self):
        """
        Get list of available chains from main config.

        Returns:
            list: List of chain origin keys
        """
        return [chain.origin_key for chain in self.main_config if hasattr(chain, 'origin_key')]

    def list_chain_dates(self, chain):
        """
        List all available dates for a given chain in GCS.

        Args:
            chain (str): Chain name

        Returns:
            list: Sorted list of date strings (YYYY-MM-DD format)
        """
        prefix = f"{chain}/"
        blobs = self.bucket.list_blobs(prefix=prefix, delimiter='/')

        # Extract date folders
        dates = set()

        # First iterate through blobs to consume them (even if we don't need them)
        for blob in blobs:
            pass  # Just consume the iterator

        # Then iterate through prefixes to get date folders
        for blob_prefix in blobs.prefixes:
            # blob_prefix is like "ethereum/2024-01-01/"
            date_part = blob_prefix.split('/')[-2]
            try:
                # Validate date format
                datetime.strptime(date_part, '%Y-%m-%d')
                dates.add(date_part)
            except ValueError:
                continue

        return sorted(list(dates))

    def list_parquet_files_for_date(self, chain, date_str):
        """
        List all parquet files for a specific chain and date.

        Args:
            chain (str): Chain name
            date_str (str): Date in YYYY-MM-DD format

        Returns:
            list: List of blob names for parquet files
        """
        prefix = f"{chain}/{date_str}/"
        blobs = self.bucket.list_blobs(prefix=prefix)

        parquet_files = []
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                parquet_files.append(blob.name)

        return parquet_files

    def load_parquet_from_gcs(self, blob_name):
        """
        Load a parquet file from GCS into a DataFrame.

        Args:
            blob_name (str): GCS blob name

        Returns:
            pd.DataFrame: Loaded DataFrame or None if error
        """
        try:
            blob = self.bucket.blob(blob_name)
            parquet_data = blob.download_as_bytes()
            df = pd.read_parquet(io.BytesIO(parquet_data))
            return df
        except Exception as e:
            print(f"Error loading {blob_name}: {e}")
            return None

    def setup_rpc_connection(self, chain):
        """
        Set up RPC connection for a given chain.

        Args:
            chain (str): Chain name

        Returns:
            Web3CC: Web3 connection object or None if failed
        """
        try:
            active_rpc_configs, _ = get_chain_config(self.db_connector, chain)

            if not active_rpc_configs:
                print(f"No RPC configurations found for chain: {chain}")
                return None

            # Use the first available RPC config
            rpc_config = active_rpc_configs[0]
            w3 = connect_to_node(rpc_config)
            print(f"Connected to RPC for {chain}: {rpc_config['url']}")
            return w3
        except Exception as e:
            print(f"Failed to setup RPC connection for {chain}: {e}")
            return None

    def process_transactions_for_user_ops(self, chain, df_raw, w3, four_byte_lookup):
        """
        Process transactions to extract user operations.

        Args:
            chain (str): Chain name
            df_raw (pd.DataFrame): Raw transaction data
            w3: Web3 connection

        Returns:
            pd.DataFrame: User operations DataFrame
        """
        try:
            # Prepare the data using chain-specific configuration
            df_prep = prep_dataframe_new(df_raw.copy(), chain)

            # Change to backend directory to ensure 4bytes.parquet can be found
            original_cwd = os.getcwd()
            os.chdir(self.backend_dir)

            try:
                # Extract user operations using both raw and prepared data
                user_ops_df = process_user_ops_for_transactions(w3, df_raw, df_prep, chain, four_byte_lookup)
                return user_ops_df
            finally:
                # Always change back to original directory
                os.chdir(original_cwd)

        except Exception as e:
            print(f"Error processing transactions for user ops: {e}")
            return pd.DataFrame()

    def save_user_ops_to_db(self, user_ops_df):
        """
        Save user operations to the database.

        Args:
            user_ops_df (pd.DataFrame): User operations DataFrame

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if user_ops_df.empty:
                return True

            # Create a copy to avoid modifying the original
            df_to_save = user_ops_df.copy()

            # Ensure required columns exist
            required_cols = ['origin_key', 'tx_hash', 'tree_index']
            missing_cols = [col for col in required_cols if col not in df_to_save.columns]
            if missing_cols:
                print(f"Error: Missing required columns: {missing_cols}")
                print(f"Available columns: {df_to_save.columns.tolist()}")
                return False

            # Drop duplicates before setting index
            initial_count = len(df_to_save)
            df_to_save.drop_duplicates(subset=required_cols, keep='first', inplace=True)
            duplicates_removed = initial_count - len(df_to_save)
            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicate records")

            # Reset index before setting new one to avoid issues
            df_to_save.reset_index(drop=True, inplace=True)

            # Set composite primary key
            df_to_save.set_index(required_cols, inplace=True)

            # Save to database
            self.db_connector.upsert_table('uops', df_to_save, if_exists='update')
            print(f"Saved {len(df_to_save)} user operations to database")
            return True
        except Exception as e:
            print(f"Error saving user ops to database: {e}")
            import traceback
            traceback.print_exc()
            return False

    def backfill_chain_date(self, chain, date_str, dry_run=False, batch_size=50):
        """
        Backfill user operations for a specific chain and date.

        Args:
            chain (str): Chain name
            date_str (str): Date in YYYY-MM-DD format
            dry_run (bool): If True, only preview what would be processed

        Returns:
            dict: Statistics about the processing
        """
        stats = {
            'files_processed': 0,
            'transactions_processed': 0,
            'user_ops_extracted': 0,
            'errors': 0
        }

        print(f"\n--- Processing {chain} for {date_str} ---")

        # List parquet files for this date
        parquet_files = self.list_parquet_files_for_date(chain, date_str)

        if not parquet_files:
            print(f"No parquet files found for {chain} on {date_str}")
            return stats

        print(f"Found {len(parquet_files)} parquet files")

        if dry_run:
            print("DRY RUN: Would process the following files:")
            for file in parquet_files:
                print(f"  - {file}")
            return stats

        # Setup RPC connection
        w3 = self.setup_rpc_connection(chain)
        if not w3:
            print(f"Skipping {chain} due to RPC connection failure")
            stats['errors'] += 1
            return stats
        
        # Load 4bytes data
        df_4bytes = load_4bytes_data()
        four_byte_lookup = create_4byte_lookup(df_4bytes)
        print(f"Loaded {len(df_4bytes)} 4byte entries into lookup dict for user ops processing")

        all_user_ops = []
        # Use the provided batch_size parameter

        # Process each parquet file
        for i, file_path in enumerate(parquet_files):
            try:
                print(f"Processing file {i+1}/{len(parquet_files)}: {file_path}")

                # Load the parquet file.-
                df_raw = self.load_parquet_from_gcs(file_path)
                if df_raw is None or df_raw.empty:
                    print(f"Skipping empty file: {file_path}")
                    continue

                stats['files_processed'] += 1
                stats['transactions_processed'] += len(df_raw)

                # Extract user operations
                user_ops_df = self.process_transactions_for_user_ops(chain, df_raw, w3, four_byte_lookup)

                if not user_ops_df.empty:
                    all_user_ops.append(user_ops_df)
                    stats['user_ops_extracted'] += len(user_ops_df)
                    print(f"Extracted {len(user_ops_df)} user operations from {len(df_raw)} transactions")
                else:
                    print(f"No user operations found in {file_path}")

                # Save batch when we reach batch_size files with user ops or at the end
                should_save_batch = False

                # Count files with user ops for batching
                files_with_user_ops = len(all_user_ops)

                # Save if we have reached batch size or we're at the last file
                if files_with_user_ops >= batch_size:
                    should_save_batch = True
                elif i == len(parquet_files) - 1 and files_with_user_ops > 0:
                    should_save_batch = True

                if should_save_batch:
                    combined_user_ops = pd.concat(all_user_ops, ignore_index=True)
                    print(f"Saving batch of {len(combined_user_ops)} user operations from {files_with_user_ops} files to database...")
                    success = self.save_user_ops_to_db(combined_user_ops)
                    if success:
                        print(f"Successfully saved batch to database")
                        all_user_ops = []  # Clear the batch
                    else:
                        print(f"Failed to save batch to database")
                        stats['errors'] += 1

            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                stats['errors'] += 1
                continue

        print(f"Completed {chain} for {date_str}: {stats['files_processed']} files, "
              f"{stats['transactions_processed']} transactions, {stats['user_ops_extracted']} user ops")

        return stats

    def backfill_chain(self, chain, start_date=None, end_date=None, dry_run=False, batch_size=50):
        """
        Backfill user operations for an entire chain.

        Args:
            chain (str): Chain name
            start_date (str): Start date in YYYY-MM-DD format (optional)
            end_date (str): End date in YYYY-MM-DD format (optional)
            dry_run (bool): If True, only preview what would be processed

        Returns:
            dict: Overall statistics
        """
        print(f"\n=== Starting backfill for chain: {chain} ===")

        # Get available dates for this chain
        available_dates = self.list_chain_dates(chain)

        if not available_dates:
            print(f"No data found for chain: {chain}")
            return {'chains_processed': 0, 'total_files': 0, 'total_transactions': 0, 'total_user_ops': 0, 'total_errors': 0}

        # Filter dates if start/end dates are specified
        if start_date:
            available_dates = [d for d in available_dates if d >= start_date]
        if end_date:
            available_dates = [d for d in available_dates if d <= end_date]

        print(f"Will process {len(available_dates)} dates: {available_dates[0]} to {available_dates[-1]}")

        if dry_run:
            print("DRY RUN: Available dates to process:")
            for date_str in available_dates:
                parquet_files = self.list_parquet_files_for_date(chain, date_str)
                print(f"  {date_str}: {len(parquet_files)} files")
            return {'chains_processed': 1, 'total_files': 0, 'total_transactions': 0, 'total_user_ops': 0, 'total_errors': 0}

        # Process each date
        total_stats = {'total_files': 0, 'total_transactions': 0, 'total_user_ops': 0, 'total_errors': 0}

        for date_str in available_dates:
            date_stats = self.backfill_chain_date(chain, date_str, dry_run, batch_size)

            total_stats['total_files'] += date_stats['files_processed']
            total_stats['total_transactions'] += date_stats['transactions_processed']
            total_stats['total_user_ops'] += date_stats['user_ops_extracted']
            total_stats['total_errors'] += date_stats['errors']

        total_stats['chains_processed'] = 1

        print(f"\n=== Completed backfill for chain: {chain} ===")
        print(f"Total files: {total_stats['total_files']}")
        print(f"Total transactions: {total_stats['total_transactions']}")
        print(f"Total user ops: {total_stats['total_user_ops']}")
        print(f"Total errors: {total_stats['total_errors']}")

        return total_stats

    def backfill_all_chains(self, start_date=None, end_date=None, dry_run=False, batch_size=50):
        """
        Backfill user operations for all available chains.

        Args:
            start_date (str): Start date in YYYY-MM-DD format (optional)
            end_date (str): End date in YYYY-MM-DD format (optional)
            dry_run (bool): If True, only preview what would be processed
            batch_size (int): Number of files to process before saving to database

        Returns:
            dict: Overall statistics
        """
        print("\n=== Starting backfill for all chains ===")

        available_chains = self.get_available_chains()
        print(f"Available chains: {available_chains}")

        overall_stats = {'chains_processed': 0, 'total_files': 0, 'total_transactions': 0, 'total_user_ops': 0, 'total_errors': 0}

        for chain in available_chains:
            try:
                chain_stats = self.backfill_chain(chain, start_date, end_date, dry_run, batch_size)

                overall_stats['chains_processed'] += chain_stats.get('chains_processed', 0)
                overall_stats['total_files'] += chain_stats.get('total_files', 0)
                overall_stats['total_transactions'] += chain_stats.get('total_transactions', 0)
                overall_stats['total_user_ops'] += chain_stats.get('total_user_ops', 0)
                overall_stats['total_errors'] += chain_stats.get('total_errors', 0)

            except Exception as e:
                print(f"Error processing chain {chain}: {e}")
                overall_stats['total_errors'] += 1
                continue

        print("\n=== FINAL SUMMARY ===")
        print(f"Chains processed: {overall_stats['chains_processed']}")
        print(f"Total files: {overall_stats['total_files']}")
        print(f"Total transactions: {overall_stats['total_transactions']}")
        print(f"Total user ops: {overall_stats['total_user_ops']}")
        print(f"Total errors: {overall_stats['total_errors']}")

        return overall_stats


def main():
    """Main entry point for the backfill script."""
    parser = argparse.ArgumentParser(
        description='Backfill user operations from transaction data stored in GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Process all chains
  %(prog)s --chain ethereum                   # Process only Ethereum
  %(prog)s --chain ethereum --start-date 2024-01-01 --end-date 2024-01-31
  %(prog)s --dry-run                          # Preview what would be processed
        """
    )

    parser.add_argument('--chain', type=str, help='Specific chain to process (e.g., ethereum, arbitrum)')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--batch-size', type=int, default=50, help='Number of files to process before saving to database (default: 50)')
    parser.add_argument('--dry-run', action='store_true', help='Preview what would be processed without actually doing it')

    args = parser.parse_args()

    # Validate date formats if provided
    if args.start_date:
        try:
            datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print("Error: start-date must be in YYYY-MM-DD format")
            sys.exit(1)

    if args.end_date:
        try:
            datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            print("Error: end-date must be in YYYY-MM-DD format")
            sys.exit(1)

    # Initialize backfiller
    try:
        backfiller = UserOpsBackfiller()
    except Exception as e:
        print(f"Failed to initialize backfiller: {e}")
        sys.exit(1)

    # Run backfill
    try:
        if args.chain:
            # Process specific chain
            backfiller.backfill_chain(args.chain, args.start_date, args.end_date, args.dry_run, args.batch_size)
        else:
            # Process all chains
            backfiller.backfill_all_chains(args.start_date, args.end_date, args.dry_run, args.batch_size)

        print(f"\nBackfill completed successfully!")

        if args.dry_run:
            print("This was a dry run. No actual processing was performed.")

    except KeyboardInterrupt:
        print("\nBackfill interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Backfill failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

#Usage Examples:

  # Process all chains
  #python backfill_user_ops.py

  # Process only Ethereum
  #python backfill_user_ops.py --chain ethereum

  # Process Ethereum for a specific date range
  #python backfill_user_ops.py --chain ethereum --start-date 2024-01-01 --end-date 2024-01-31

  # Process with custom batch size (save every 100 files instead of default 50)
  #python backfill_user_ops.py --chain ethereum --batch-size 100

  # Dry run to see what would be processed
  #python backfill_user_ops.py --dry-run