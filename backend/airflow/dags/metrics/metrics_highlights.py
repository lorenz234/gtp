import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

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
    dag_id='metrics_highlights',
    description='Run highlights (ATHs, growth, others).',
    tags=['metrics', 'daily'],
    start_date=datetime(2025,10,12),
    schedule='10 05 * * *' # after metrics_sql (04:30), before api_json_creation (05:30)
)

def etl():
    @task()
    def run_aths():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.main_config import get_main_config
        from src.config import highlights_daily_thresholds

        db_connector = DbConnector()
        main_config = get_main_config()
        days = 30

        ## dict that has metric_key as key and the ath_multiples dict as value (if exists)
        ath_thresholds = {k: v['ath_multiples'] for k, v in highlights_daily_thresholds.items() if 'ath_multiples' in v}

        for chain in main_config:
            if chain.api_in_main:
                print(f'Processing chain for ATHs: {chain.origin_key}')
                
                query_params = {
                    "origin_key": chain.origin_key,
                    "lookback_days": days,
                    "thresholds_by_metric": ath_thresholds
                }

                execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_ath.sql.j2', query_params)
                
    def run_growth():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.main_config import get_main_config
        from src.config import highlights_daily_thresholds

        db_connector = DbConnector()
        main_config = get_main_config()

        ## dict that has metric_key as key and the relative_growth dict as value (if exists)
        growth_config = {k: v['relative_growth'] for k, v in highlights_daily_thresholds.items() if 'relative_growth' in v}

        for chain in main_config:
            if chain.api_in_main:
                print(f'Processing chain: {chain.origin_key}')

                query_params = {
                    "metric_configs": growth_config,
                    "origin_key": chain.origin_key,
                }

                execute_jinja_query(db_connector, 'chain_metrics/upsert_highlights_growth.sql.j2', query_params)
                
    run_aths()
    run_growth()
etl()
