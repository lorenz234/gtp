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
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_blockscout',
    description='Scrape top contract information from blockscout',
    tags=['oli', 'daily'],
    start_date=datetime(2024,10,7),
    schedule='30 01 * * *'
)

def etl(): # TODO: make this create attestations to the OLI label pool rather than upserting it into oli_tag_mapping
    @task()
    def run_oss():
        from src.db_connector import DbConnector
        from src.adapters.adapter_blockscout import AdapterBlockscout
        
        adapter_params = {
        }
        load_params = {
            'number_of_contracts': 50,
            'days_back': 3
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterBlockscout(adapter_params, db_connector)

        for chain in ad.projects:
            # create load params for each chain
            chain_load_params = load_params.copy()
            chain_load_params['origin_key'] = chain.origin_key
            chain_load_params['aliases_blockscout_url'] = chain.aliases_blockscout_url
            # extract
            df = ad.extract(chain_load_params)
            # load
            ad.load(df)
    
    run_oss()

etl()