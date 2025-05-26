import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

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
    dag_id='api_json_quick_bites',
    description='Create json files for the Quick Bites section.',
    tags=['api', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='20 05 * * *'
)

def json_creation():
    @task()
    def run_pectra_fork():        
        
        import datetime
        import pandas as pd
        from datetime import datetime, timezone
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        data_dict = {
            "data": {
                "ethereum_blob_count" : {},
                "ethereum_blob_target" : {},
                "type4_tx_count" : {
                    "ethereum": {},
                    "optimism": {},
                    "base": {},
                    "unichain": {},
                }
            }    
        }

        ## Ethereum blob count and target
        query_parameters = {}
        df = execute_jinja_query(db_connector, "api/quick_bites/select_ethereum_blob_count_per_block.sql.j2", query_parameters, return_df=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['date'])

        df_blob_count = df[['unix', 'blob_count']]
        data_dict["data"]["ethereum_blob_count"]= {
            "daily": {
                "types": df_blob_count.columns.tolist(),
                "values": df_blob_count.values.tolist()
            }
        }

        df_blob_target = df[['unix', 'blob_target']]
        data_dict["data"]["ethereum_blob_target"]= {
            "daily": {
                "types": df_blob_target.columns.tolist(),
                "values": df_blob_target.values.tolist()
            }
        }    

        ##type4 tx count
        for origin_key in ['ethereum', 'optimism', 'base', 'unichain']:
            query_parameters = {
                'origin_key': origin_key,
                'metric_key': 'txcount_type4',
            }
            df = execute_jinja_query(db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
            df.sort_values(by=['date'], inplace=True, ascending=True)
            df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
            df = df.drop(columns=['date'])

            data_dict["data"]["type4_tx_count"][origin_key]= {
                "daily": {
                    "types": df.columns.tolist(),
                    "values": df.values.tolist()
                }
            }

        data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        data_dict = fix_dict_nan(data_dict, 'pectra-fork')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/pectra-fork', data_dict, cf_distribution_id)

    run_pectra_fork()    
json_creation()