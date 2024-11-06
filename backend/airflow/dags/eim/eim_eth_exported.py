from datetime import datetime,timedelta
import getpass
sys_user = getpass.getuser()

import sys
sys.path.append(f"/home/{sys_user}/gtp/backend")

from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook
from src.db_connector import DbConnector
from eim.adapters.adapter_eth_exported import AdapterEthExported

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='eim_eth_exported',
    description='Load ETH locked on bridges as well as derivative prices.',
    tags=['eim', 'daily'],
    start_date=datetime(2024,10,30),
    schedule='35 02 * * *'
)

def run():
    @task()
    def run_first_block_of_day():
        adapter_params = {}
        load_params = {
            'load_type' : 'first_block_of_day',
            'days' : 3,
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_bridge_balances(x):
        adapter_params = {}
        load_params = {
            'load_type' : 'bridge_balances',
            'days' : 3,
            #'entities': ['arbitrum']
            'entities': None
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_conversion_rates(x):
        adapter_params = {}
        load_params = {
            'load_type' : 'conversion_rates',
            'days' : 3,
            #'assets': ['ETH']
            'assets': None
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_native_eth_exported(x):
        adapter_params = {}
        load_params = {
            'load_type' : 'native_eth_exported',
            'days' : 3
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    @task()
    def run_convert_usd(x):
        adapter_params = {}
        load_params = {
            'load_type' : 'eth_equivalent_in_usd',
            'days' : 3
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterEthExported(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    run_convert_usd(run_native_eth_exported(run_conversion_rates(run_bridge_balances(run_first_block_of_day()))))

run()