from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_dune_hourly',
    description='Load aggregates metrics hourly.',
    tags=['metrics', 'hourly'],
    start_date=datetime(2026,2,10),
    schedule='5 * * * *'
)

def etl():
    @task()
    def run_fundamentals_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_fundamentals_hourly',
                    'query_id': 6715552,
                    'params': {'hours': 3}
                },
                {
                    'name': 'polygon_pos_fundamentals_hourly',
                    'query_id': 6715560,
                    'params': {'hours': 3}
                },
            ],
            'prepare_df': 'prepare_df_metric_hourly',
            'load_type': 'fact_kpis_granular'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    
    @task()
    def run_starknet_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'starknet_contract_level_hourly',
                    'query_id': 6683568,
                    'params': {'hours': 3}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for contract level.")
        
        # additional prep steps
        df['origin_key'] = 'starknet'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)
        
    @task()
    def run_megaeth_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_contract_level_hourly',
                    'query_id': 6683663,
                    'params': {'hours': 3}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for contract level.")
        
        # additional prep steps
        df['origin_key'] = 'megaeth'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)
        
    @task()
    def run_polygon_pos_contract_level_hourly():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'polygon_pos_contract_level_hourly',
                    'query_id': 6683678,
                    'params': {'hours': 3}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_hourly',
            'load_type': 'blockspace_fact_contract_level_hourly'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for contract level.")

        # additional prep steps
        df['origin_key'] = 'polygon_pos'
        df.set_index(['address', 'hour', 'origin_key'], inplace=True)

        # load
        ad.load(df)
        
    run_fundamentals_hourly()
    run_starknet_contract_level_hourly()
    run_megaeth_contract_level_hourly()
    run_polygon_pos_contract_level_hourly()
    
etl()