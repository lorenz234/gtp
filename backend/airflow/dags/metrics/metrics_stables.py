from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_stables',
    description='Load Stablecoin balances via RPCs',
    tags=['metrics', 'daily'],
    start_date=datetime(2024,4,21),
    schedule='30 01 * * *'
)

def etl():
    @task()
    def run_stables():
        from src.db_connector import DbConnector
        from src.adapters.adapter_stables import AdapterStablecoinSupply

        # Initialize DB Connector
        db_connector = DbConnector()
        days = 3

        # Create adapter params
        adapter_params = {
        }

        # Initialize the Stablecoin Adapter
        stablecoin_adapter = AdapterStablecoinSupply(adapter_params, db_connector)

        # Step 1: Get block data for all chains
        print("Step 1: Collecting block data...")
        block_params = {
            'days': days, 
            'load_type': 'block_data'
        }
        stablecoin_adapter.extract(block_params, update=True)
        print(f"Loaded block records")

        # Step 2: Get bridged stablecoin supply
        print("\nStep 2: Collecting bridged stablecoin data...")
        bridged_params = {
            'days': days,
            'load_type': 'bridged_supply'
        }
        stablecoin_adapter.extract(bridged_params, update=True)
        print(f"Loaded bridged stablecoin records")

        # Step 3: Get direct stablecoin supply
        print("\nStep 3: Collecting direct stablecoin data...")
        direct_params = {
            'days': days,
            'load_type': 'direct_supply'
        }
        stablecoin_adapter.extract(direct_params, update=True)
        print(f"Loaded direct stablecoin records")

        # Step 4: Get locked stablecoin supply
        print("\nStep 4: Getting locked supply...")
        locked_params = {
            'days': days,
            'load_type': 'locked_supply'
        }
        locked_df = stablecoin_adapter.extract(locked_params)
        stablecoin_adapter.load(locked_df)
        print(f"Loaded {len(locked_df)} locked stablecoin records")

        # Step 5: Calculate total stablecoin supply
        print("\nStep 4: Calculating total stablecoin supply...")
        total_params = {
            'days': 9999,
            'load_type': 'total_supply'
        }
        total_df = stablecoin_adapter.extract(total_params)
        stablecoin_adapter.load(total_df)
        print(f"Loaded {len(total_df)} total supply records")

        print("\nData collection complete!")
    
    run_stables()
etl()