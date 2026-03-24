from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_coingecko',
    description='Load price, volume, and market_cap from coingecko API for all tracked tokens.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='30 00 * * *' ## data should be available by 0:10 utc according to https://docs.coingecko.com/v3.0.1/reference/coins-id-market-chart
)

def etl():
    @task()
    def run_direct():
        from src.db_connector import DbConnector
        from src.adapters.adapter_coingecko import AdapterCoingecko
        import os
        adapter_params = {
            'api_key' : os.getenv("COINGECKO_API")
        }
        
        load_params = {
            'load_type' : 'direct',
            'metric_keys' : ['price', 'volume', 'market_cap'],
            'coingecko_ids' : ['glo-dollar'],
            'days' : 'auto', # auto, max, or a number (as string)
            'vs_currencies' : ['usd', 'eth']
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
        
    @task()
    def run_apps():
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        from src.adapters.adapter_coingecko import AdapterCoingecko
        import os

        adapter_params = {
            'api_key' : os.getenv("COINGECKO_API")
        }

        load_params = {
            'load_type' : 'apps',
            'granularity': 'daily',
            'metric_keys' : ['price', 'volume', 'market_cap'],
            'days' : 'auto',
            'vs_currencies' : ['usd', 'eth'],
            #'app_mapping' : None,
        }

        # initialize adapter
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
        
    @task()
    def run_coins_list():
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        from src.adapters.adapter_coingecko import AdapterCoingecko
        import os

        adapter_params = {
            'api_key' : os.getenv("COINGECKO_API")
        }

        load_params = {
            'load_type' : 'coins_list',
        }

        # initialize adapter
        ad = AdapterCoingecko(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)
    
    run_direct()
    run_apps()
    run_coins_list()
etl()