from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_eip8004",
    description="Quick Bite on EIP-8004 + backfilling of raw tables for EIP-8004",
    tags=["other"],
    start_date=datetime(2026, 2, 13),
    schedule="41 1 * * *",  # Every day at 01:41
)
def run_dag():

    @task
    def scrape_eip8004_events():
        from src.adapters.adapter_eip8004 import EIP8004Adapter
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        ad = EIP8004Adapter({}, db_connector)
        df = ad.extract({
            'chains': ['*'],
            'events': ['*']
        })
        ad.load(df)
        
    scrape_eip8004_events()
        
run_dag()