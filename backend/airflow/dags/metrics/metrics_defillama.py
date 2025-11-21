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
    dag_id='metrics_defillama',
    description='Load App fees',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='10 03 * * *'
)

def etl():
    @task()
    def run_app_fees():
        from src.db_connector import DbConnector
        from src.adapters.adapter_defillama import AdapterDefillama

        adapter_params = {}
        load_params = {
            'origin_keys' : None,
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDefillama(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    run_app_fees()
etl()