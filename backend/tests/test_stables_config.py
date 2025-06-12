#!/usr/bin/env python3
"""
Test script for stablecoin adapter configuration
Safely runs steps 1-4, then compares current vs new total supply calculation
"""

import sys
import os
import pandas as pd
sys.path.append(f"{os.getcwd()}/backend/")

from src.db_connector import DbConnector
from src.adapters.adapter_stables import AdapterStablecoinSupply

def get_current_total_supply(db_connector, days=3):
    """
    Calculate current total supply by summing values in fact_stables
    """
    print("\n📊 CURRENT TOTAL SUPPLY (from fact_stables)")
    print("="*50)
    
    # Get all supply data from fact_stables
    bridged_df = db_connector.get_data_from_table(
        "fact_stables",
        filters={"metric_key": "supply_bridged"},
        days=days
    )
    
    direct_df = db_connector.get_data_from_table(
        "fact_stables", 
        filters={"metric_key": "supply_direct"},
        days=days
    )
    
    locked_df = db_connector.get_data_from_table(
        "fact_stables",
        filters={"metric_key": "locked_supply"}, 
        days=days
    )
    
    bridged_exceptions_df = db_connector.get_data_from_table(
        "fact_stables",
        filters={"metric_key": "supply_bridged_exceptions"},
        days=days
    )
    
    # Combine all data
    all_data = []
    for df, name in [(bridged_df, "bridged"), (direct_df, "direct"), 
                     (locked_df, "locked"), (bridged_exceptions_df, "bridged_exceptions")]:
        if not df.empty:
            df_reset = df.reset_index()
            df_reset['supply_type'] = name
            all_data.append(df_reset)
            print(f"✓ Found {len(df)} {name} records")
    
    if not all_data:
        print("❌ No supply data found in fact_stables")
        return pd.DataFrame()
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Calculate total by chain (same logic as get_total_supply)
    bridged_all = combined_df[combined_df['supply_type'].isin(['bridged', 'bridged_exceptions'])]
    
    # Sum bridged supply for L2 chains (not Ethereum)
    bridged_l2s = bridged_all[bridged_all['origin_key'] != 'ethereum']
    bridged_l2_total = bridged_l2s.groupby('date')['value'].sum().reset_index()
    
    # Calculate totals per chain
    chain_totals = combined_df.groupby(['origin_key', 'date'])['value'].sum().reset_index()
    
    # For Ethereum, subtract the L2 bridged amounts
    ethereum_totals = chain_totals[chain_totals['origin_key'] == 'ethereum'].copy()
    other_totals = chain_totals[chain_totals['origin_key'] != 'ethereum']
    
    if not ethereum_totals.empty and not bridged_l2_total.empty:
        ethereum_totals = ethereum_totals.merge(bridged_l2_total, on='date', how='left', suffixes=('', '_l2'))
        ethereum_totals['value'] = ethereum_totals['value'] - ethereum_totals['value_l2'].fillna(0)
        ethereum_totals = ethereum_totals[['origin_key', 'date', 'value']]
        
        final_totals = pd.concat([ethereum_totals, other_totals], ignore_index=True)
    else:
        final_totals = chain_totals
    
    # Show latest totals by chain
    if not final_totals.empty:
        latest_date = final_totals['date'].max()
        latest_totals = final_totals[final_totals['date'] == latest_date]
        latest_totals = latest_totals.sort_values('value', ascending=False)
        
        print(f"\nCurrent totals by chain (latest date: {latest_date}):")
        for _, row in latest_totals.iterrows():
            print(f"  {row['origin_key']}: ${row['value']:,.2f}")
        
        grand_total = latest_totals['value'].sum()
        print(f"\nCurrent grand total: ${grand_total:,.2f}")
    
    return final_totals

def test_stables_config(chains_to_test=None):
    """
    Test stablecoin configuration safely and compare results
    """
    # Initialize DB Connector
    db_connector = DbConnector()
    days = 3

    # Create adapter params
    adapter_params = {}
    if chains_to_test:
        adapter_params['origin_keys'] = chains_to_test

    print("🧪 TESTING STABLECOIN CONFIGURATION")
    print("="*60)
    
    # Step 0: Get current total supply
    current_totals = get_current_total_supply(db_connector, days)

    # Initialize the Stablecoin Adapter
    print(f"\n🔧 Initializing adapter...")
    stablecoin_adapter = AdapterStablecoinSupply(adapter_params, db_connector)
    print(f"Testing with chains: {stablecoin_adapter.chains}")

    try:
        # Step 1: Get block data
        print(f"\n📊 Step 1: Block data...")
        block_params = {'days': days, 'load_type': 'block_data'}
        block_df = stablecoin_adapter.extract(block_params, update=True)
        print(f"✓ Generated {len(block_df)} block records")

        # Step 2: Get bridged supply
        print(f"\n🌉 Step 2: Bridged supply...")
        bridged_params = {'days': days, 'load_type': 'bridged_supply'}
        bridged_df = stablecoin_adapter.extract(bridged_params, update=True)
        print(f"✓ Generated {len(bridged_df)} bridged records")

        # Step 3: Get direct supply
        print(f"\n🏭 Step 3: Direct supply...")
        direct_params = {'days': days, 'load_type': 'direct_supply'}
        direct_df = stablecoin_adapter.extract(direct_params, update=True)
        print(f"✓ Generated {len(direct_df)} direct records")

        # Step 4: Get locked supply
        print(f"\n🔒 Step 4: Locked supply...")
        locked_params = {'days': days, 'load_type': 'locked_supply'}
        locked_df = stablecoin_adapter.extract(locked_params, update=True)
        print(f"✓ Generated {len(locked_df)} locked records")

        print("\n✅ Steps 1-4 completed and loaded to fact_stables")

        # Step 5: Generate new total supply (DON'T LOAD)
        print(f"\n🧮 Step 5: Generating NEW total supply (NOT LOADING)...")
        total_params = {'days': days, 'load_type': 'total_supply'}
        new_total_df = stablecoin_adapter.extract(total_params)
        print(f"✓ Generated {len(new_total_df)} total supply records")

        # Compare current vs new
        print(f"\n🔍 COMPARISON: Current vs New Total Supply")
        print("="*60)
        
        if not new_total_df.empty:
            new_df_reset = new_total_df.reset_index()
            latest_date = new_df_reset['date'].max()
            new_latest = new_df_reset[new_df_reset['date'] == latest_date]
            new_latest = new_latest.sort_values('value', ascending=False)
            
            print(f"NEW totals by chain (date: {latest_date}):")
            new_grand_total = 0
            for _, row in new_latest.iterrows():
                print(f"  {row['origin_key']}: ${row['value']:,.2f}")
                new_grand_total += row['value']
            
            print(f"\nNEW grand total: ${new_grand_total:,.2f}")
            
            # Compare with current if we have it
            if not current_totals.empty:
                current_latest = current_totals[current_totals['date'] == latest_date]
                current_grand_total = current_latest['value'].sum() if not current_latest.empty else 0
                
                difference = new_grand_total - current_grand_total
                print(f"Current grand total: ${current_grand_total:,.2f}")
                print(f"Difference: ${difference:,.2f}")
                
                if abs(difference) > 1000:  # Flag significant differences
                    print(f"⚠️  SIGNIFICANT DIFFERENCE DETECTED: ${difference:,.2f}")
                else:
                    print(f"✅ Difference is small: ${difference:,.2f}")
            
            print(f"\n" + "!"*60)
            print("⚠️  NEW TOTAL SUPPLY READY BUT NOT LOADED TO DATABASE!")
            print("Review the comparison above before loading.")
            print("To load to fact_kpis: stablecoin_adapter.load(new_total_df)")
            print("!"*60)
            
            return new_total_df
        else:
            print("❌ No new total supply data generated")
            return None

    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return None

def verify_chain_config(chain_name):
    """
    Verify that a chain is properly configured
    """
    from src.stables_config import stables_mapping
    
    print(f"\n🔍 Verifying configuration for {chain_name}...")
    
    if chain_name not in stables_mapping:
        print(f"❌ {chain_name} not found in stables_mapping")
        return False
    
    config = stables_mapping[chain_name]
    print(f"✓ {chain_name} found in stables_mapping")
    
    # Check bridged config
    if 'bridged' in config:
        print(f"✓ Bridged config found: {len(config['bridged'])} source chains")
        for source_chain, addresses in config['bridged'].items():
            print(f"  - {source_chain}: {len(addresses)} bridge addresses")
    
    # Check direct config
    if 'direct' in config:
        print(f"✓ Direct config found: {len(config['direct'])} tokens")
        for token_id, token_config in config['direct'].items():
            print(f"  - {token_id}: {token_config['token_address']}")
    
    # Check locked supply config
    if 'locked_supply' in config:
        print(f"✓ Locked supply config found: {len(config['locked_supply'])} tokens")
    
    if not any(key in config for key in ['bridged', 'direct', 'locked_supply']):
        print(f"⚠️  {chain_name} has no bridged, direct, or locked_supply configuration")
    
    return True

if __name__ == "__main__":
    # Test specific chains - modify this list as needed
    chains_to_test = ['redstone']
    
    print("🧪 Testing Stablecoin Configuration with Comparison")
    print("This script compares current vs new total supply calculations")
    
    # First verify the configuration for new chains
    if chains_to_test:
        for chain in chains_to_test:
            verify_chain_config(chain)
    
    # Run the test and comparison
    new_total_df = test_stables_config(chains_to_test)
    
    if new_total_df is not None:
        print(f"\n✅ Test completed successfully!")
        print("Review the comparison above.")
        print("If everything looks correct, you can load the new totals:")
        print("  # stablecoin_adapter.load(new_total_df)")
    else:
        print(f"\n❌ Test failed - check error messages above") 