from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook


@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='ahoura')
    },
    dag_id='oli_mev_contract_airtable',
    description='Update Airtable with MEV contract data',
    tags=['oli', 'daily'],
    start_date=datetime(2025, 5, 26),
    schedule='00 09 * * *'
)
def mev_contract_etl():
    
    @task()
    def run_mev_script():
        from src.misc.oli_mev_contract_airtable import main_async, Config
        import argparse
        import asyncio

        # Create args object with default values
        class Args:
            batch_size = 50
            concurrency = 10
            max_records = None

        args = Args()

        # Run the async main function
        asyncio.run(main_async(args))

    run_mev_script()

mev_contract_etl()