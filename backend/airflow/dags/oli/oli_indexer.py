import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_indexer',
    description='Indexes the OLI Label Pool from onchain and offchain sources',
    tags=['indexing', 'oli', 'raw'],
    start_date=datetime(2025, 10, 27),
    schedule='*/2 * * * *',  # Runs every 2 minutes
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync_onchain():

        from backend.src.adapters.adapter_oli_onchain import AdapterOLI
        from src.db_connector import DbConnector
        import pandas as pd

        adapter_params = {'rpc_url': 'https://base.gateway.tenderly.co'}
        db_connector = DbConnector(db_name='oli')
        ad = AdapterOLI(adapter_params, db_connector)

        extract_params = {
            'from_block': -100,
            'to_block': 'latest'
        }

        attest_topics = [
            '0x8bf46bf4cfd674fa735a3d63ec1c9ad4153f033c290341f3a588b75685141b35', # Attest topic
            None,
            None,
            '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68' # Schema topic
        ]
        revoke_topics = [
            '0xf930a6e2523c9cc298691873087a740550b8fc85a0680830414c148ed927f615', # Revoke topic of onchain attestations
            None,
            None,
            '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68' # Schema topic
        ]

        # extract onchain attestations
        extract_params['topics'] = attest_topics
        df_attest = ad.extract(extract_params)

        # extract revocations of onchain attestations
        extract_params['topics'] = revoke_topics
        df_revokes = ad.extract(extract_params)

        # merge df_attest & df_revokes, if there are duplicate uid, then keep the one in df_revokes (overwrites tx_id)
        df = pd.concat([df_revokes, df_attest]).drop_duplicates(subset=['uid']).reset_index(drop=True)
        ad.load(df, 'attestations')

    sync_onchain()

main()
