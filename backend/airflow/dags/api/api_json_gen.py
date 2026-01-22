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
    dag_id='api_json_gen',
    description='NEW DAG to create JSON files for our frontend.',
    tags=['api', 'daily'],
    start_date=datetime(2025,8,28),
    schedule='35 05 * * *'
)

def run():
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_metrics_per_chain_jsons():
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector

        db_connector = DbConnector()
        
        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_metric_jsons(level='chains')
        json_gen.create_metric_jsons(level='data_availability')
        
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_chain_overview_jsons():    
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_chains_jsons()
    
    @task(execution_timeout=timedelta(minutes=30))
    def run_create_chain_user_insights_jsons():    
        import os
        from src.api.json_gen import JsonGen
        from src.db_connector import DbConnector
        
        db_connector = DbConnector()

        json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
        json_gen.create_user_insights_json()
        
    # @task()
    # def run_create_ecosystem_jsons():    
    #     import os
    #     from src.api.json_gen import JsonGen
    #     from src.db_connector import DbConnector
        
    #     db_connector = DbConnector()

    #     json_gen = JsonGen(os.getenv("S3_CF_BUCKET"), os.getenv("CF_DISTRIBUTION_ID"), db_connector, api_version)
    #     json_gen.create_ecosystem_builders_json()
        
    
    run_create_metrics_per_chain_jsons()
    run_create_chain_overview_jsons()
    #run_create_chain_user_insights_jsons()
    #run_create_ecosystem_jsons()
    
run()