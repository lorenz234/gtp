from src.adapters.adapter_raw_rpc import NodeAdapter
from src.adapters.rpc_funcs.utils import MaxWaitTimeExceededException, get_chain_config, Web3CC
from src.adapters.rpc_funcs.funcs_backfill import date_to_unix_timestamp, find_first_block_of_day
from src.db_connector import DbConnector
from datetime import datetime

def run_chain_from_date(chain_name, start_date=None, end_date=None):
    """
    Run extraction for a chain starting from a specific date rather than block number
    
    Args:
        chain_name (str): The name of the chain to test (e.g., 'celo', 'arbitrum_nova')
        start_date (str): Start date in YYYY-MM-DD format. If None, defaults to current date
        end_date (str): End date in YYYY-MM-DD format. If None, only processes start_date
    """
    # Initialize DbConnector
    db_connector = DbConnector()
    
    print(f"Testing chain: {chain_name}")

    # Get RPC configs for the chain
    active_rpc_configs, batch_size = get_chain_config(db_connector, chain_name)
    print(f"RPC Config: {active_rpc_configs}")

    # Initialize Web3 connection
    w3 = None
    for rpc_config in active_rpc_configs:
        try:
            print(f"Connecting to RPC URL: {rpc_config['url']}")
            w3 = Web3CC(rpc_config)
            break
        except Exception as e:
            print(f"Failed to connect to RPC URL: {rpc_config['url']} with error: {e}")
    
    if not w3:
        raise ConnectionError("Failed to connect to any provided RPC node.")
    
    print(f"Connected to RPC URL: {w3.provider.endpoint_uri}")
    
    # Convert date to timestamp and find corresponding blocks
    if start_date is None:
        # Default to current date
        start_date = datetime.now().strftime('%Y-%m-%d')
    
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    start_timestamp = date_to_unix_timestamp(start_date_obj.year, start_date_obj.month, start_date_obj.day)
    
    # Find the first block of the start date
    print(f"Finding first block for date: {start_date}")
    start_block = find_first_block_of_day(w3, start_timestamp)
    print(f"First block of {start_date}: {start_block}")

    # Initialize adapter
    adapter_params = {
        'rpc': 'local_node',
        'chain': chain_name,
        'rpc_configs': active_rpc_configs,
    }
    adapter = NodeAdapter(adapter_params, db_connector)

    # Set load parameters using the blocks we found
    load_params = {
        'block_start': start_block,
        'batch_size': batch_size,
    }
    
    try:
        adapter.extract_raw(load_params)
    except MaxWaitTimeExceededException as e:
        print(f"Extraction stopped due to maximum wait time being exceeded: {e}")
        raise e
    finally:
        adapter.log_stats()

if __name__ == "__main__":
    run_chain_from_date(
        chain_name='arbitrum_nova', 
        start_date='2025-05-07', 
    )