#!/usr/bin/env python3

import sys
import os
import pandas as pd
from datetime import datetime, date

# Add the backend src directory to the path
sys.path.append('/Users/nader/Documents/GitHub/gtp/backend/src')

from adapters.rpc_funcs.utils import process_authorization_list_data

def test_block_date_processing():
    """Test that block_date processing handles both date objects and timestamps correctly."""
    
    print("Testing block_date processing fix...")
    
    # Test case 1: DataFrame with date objects (current scenario causing the error)
    test_data_with_dates = {
        'tx_hash': ['0x123', '0x456'],
        'tx_index': [0, 1],
        'eoa_address': ['0xabc', '0xdef'],
        'contract_address': ['0x789', '0x012'],
        'origin_key': [None, None],
        'chain_ids': [[1], [1]],
        'block_number': [34152131, 34152132],
        'block_timestamp': [1723564800, 1723651200],  # timestamps
        'block_date': [date(2024, 8, 13), date(2024, 8, 14)]  # actual date objects
    }
    
    df_with_dates = pd.DataFrame(test_data_with_dates)
    
    print("Input DataFrame with date objects:")
    print(f"block_date dtype: {df_with_dates['block_date'].dtype}")
    print(f"block_date values: {df_with_dates['block_date'].tolist()}")
    print(f"block_date types: {[type(x) for x in df_with_dates['block_date']]}")
    
    try:
        processed_df = process_authorization_list_data(df_with_dates.copy(), 'ethereum')
        print("✅ SUCCESS: DataFrame with date objects processed without error")
        print(f"Processed block_date dtype: {processed_df['block_date'].dtype}")
        print(f"Processed block_date values: {processed_df['block_date'].tolist()}")
        print(f"Processed block_date types: {[type(x) for x in processed_df['block_date']]}")
    except Exception as e:
        print(f"❌ ERROR: Failed to process DataFrame with date objects: {e}")
        return False
    
    print("\n" + "="*60 + "\n")
    
    # Test case 2: DataFrame with timestamps (should still work)
    test_data_with_timestamps = {
        'tx_hash': ['0x123', '0x456'],
        'tx_index': [0, 1],
        'eoa_address': ['0xabc', '0xdef'],
        'contract_address': ['0x789', '0x012'],
        'origin_key': [None, None],
        'chain_ids': [[1], [1]],
        'block_number': [34152131, 34152132],
        'block_timestamp': [1723564800, 1723651200],  # timestamps
        'block_date': [1723564800, 1723651200]  # timestamps (old format)
    }
    
    df_with_timestamps = pd.DataFrame(test_data_with_timestamps)
    
    print("Input DataFrame with timestamps:")
    print(f"block_date dtype: {df_with_timestamps['block_date'].dtype}")
    print(f"block_date values: {df_with_timestamps['block_date'].tolist()}")
    print(f"block_date types: {[type(x) for x in df_with_timestamps['block_date']]}")
    
    try:
        processed_df = process_authorization_list_data(df_with_timestamps.copy(), 'ethereum')
        print("✅ SUCCESS: DataFrame with timestamps processed without error")
        print(f"Processed block_date dtype: {processed_df['block_date'].dtype}")
        print(f"Processed block_date values: {processed_df['block_date'].tolist()}")
        print(f"Processed block_date types: {[type(x) for x in processed_df['block_date']]}")
    except Exception as e:
        print(f"❌ ERROR: Failed to process DataFrame with timestamps: {e}")
        return False
    
    print("\n✅ All tests passed! The block_date processing fix is working correctly.")
    return True

if __name__ == "__main__":
    test_block_date_processing()