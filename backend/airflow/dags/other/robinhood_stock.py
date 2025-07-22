import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='robinhood_stock_QB',
    description='Data for Robinhood stock tracker.',
    tags=['other'],
    start_date=datetime(2025,7,22),
    schedule='12 3 * * *'
)

def run_dag():

    @task()
    def pull_data_from_dune():      
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)

        # first load, list of tokenized assets
        load_params = {
            'queries': [
                {
                    'name': 'Robinhood_stock_list',
                    'query_id': 5429585,
                    'params': {'days': 3}
                }
            ],
            'prepare_df': 'prepare_robinhood_list',
            'load_type': 'robinhood_stock_list'
        }
        df = ad.extract(load_params)
        ad.load(df)

        # second load, daily values for the tokenized assets
        load_params2 = {
            'queries': [
                {
                    'name': 'Robinhood_stock_daily',
                    'query_id': 5435746,
                    'params': {'days': 3}
                }
            ],
            'prepare_df': 'prepare_robinhood_daily',
            'load_type': 'robinhood_daily'
        }
        df = ad.extract(load_params2)
        ad.load(df)

    @task()
    def pull_data_from_yfinance():
        from src.adapters.adapter_yfinance_stocks import AdapterYFinance
        from src.db_connector import DbConnector

        db = DbConnector()
        yfi = AdapterYFinance({}, db)

        load_params = {
                'tickers': ['*'],
                'endpoints': ['Close'],
                'days': 5,
                'table': 'robinhood_daily',
                'prepare_df': 'prepare_df_robinhood_daily'
            }

        df = yfi.extract(load_params)
        yfi.load(df)

    pull_data_from_dune()
    pull_data_from_yfinance()
run_dag()