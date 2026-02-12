from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_sql_hourly',
    description='Load blockspace and other SQL-based metrics on an hourly basis.',
    tags=['metrics', 'hourly'],
    start_date=datetime(2026,2,10),
    schedule='5 * * * *'
)

def etl():
    @task()
    def run_blockspace_hourly():
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        from src.main_config import get_main_config
        main_config = get_main_config(db_connector)
        
        hours = 3

        for chain in main_config:
            if chain.runs_aggregate_blockspace and chain.origin_key not in ['starknet', 'imx', 'polygon_pos', 'megaeth']:
                print(f"Updating blockspace data for {chain.origin_key} and last {hours} hours...")
                db_connector.get_blockspace_contracts_hourly(chain.origin_key, hours)
                
    run_blockspace_hourly()

etl()