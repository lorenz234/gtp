from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_stables_v2',
    description='Load Stablecoin balances via RPCs',
    tags=['metrics', 'daily'],
    start_date=datetime(2024,4,21),
    schedule='15 01 * * *'
)

def etl():
    @task()
    def run_stables():
        from src.db_connector import DbConnector
        from src.adapters.adapter_stables_v2 import AdapterStablecoinSupply

        # Initialize
        db_connector = DbConnector()
        ad = AdapterStablecoinSupply({}, db_connector)

        # define extract_params
        extract_params = {
            'origin_keys': ['*'],
            'token_ids': ['*'],
            'metric_keys': ['*']
        }
        
        # extract and load
        df = ad.extract(extract_params)
        ad.load(df)

    run_stables()
etl()