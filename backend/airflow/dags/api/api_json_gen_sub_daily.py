from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

api_version = "v1"

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=1),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='api_json_gen_sub_daily',
    description='DAG to create JSON files multiple times a day for our frontend.',
    tags=['api', 'daily'],
    start_date=datetime(2025,8,28),
    schedule='*/15 * * * *' ## run every 15min
)

def run():
    @task()
    def run_create_streaks_today_json():
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        
        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_streaks_today_json()
        

    run_create_streaks_today_json()
    
run()