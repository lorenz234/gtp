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
    schedule='*/30 * * * *',  # Runs every 30 minutes
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync():

        from backend.src.adapters.adapter_oli_onchain import AdapterOLIOnchain
        from backend.src.adapters.adapter_oli_offchain import AdapterOLIOffchain
        from src.db_connector import DbConnector
        import pandas as pd

        adapter_params = {'rpc_url': 'https://base.gateway.tenderly.co'}
        db_connector = DbConnector(db_name='oli')
        ad_onchain = AdapterOLIOnchain(adapter_params, db_connector)
        ad_offchain = AdapterOLIOffchain(adapter_params, db_connector)

        # ----------------- Onchain sync -----------------

        extract_params = {
            'from_block': -50,  # add last run block
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
        df_attest = ad_onchain.extract(extract_params)

        # extract onchain revocations of onchain attestations
        extract_params['topics'] = revoke_topics
        df_revokes = ad_onchain.extract(extract_params)

        # merge df_attest & df_revokes, if there are duplicate uid, then keep the one in df_revokes (overwrites tx_id)
        df = pd.concat([df_revokes, df_attest]).drop_duplicates(subset=['uid']).reset_index(drop=True)

        # load onchain attestations & revocations
        ad_onchain.load(df, 'attestations')

        # ----------------- Offchain sync -----------------

        extract_params = {
            'from_block': -50,  # add last run block
            'to_block': 'latest',
            'topics': [
                '0x92a1f7a41a7c585a8b09e25b195e225b1d43248daca46b0faf9e0792777a2229', # Revoke topic of offchain attestations
                None,
                None,
                None
            ]
        }

        # extract onchain revocations of offchain attestations
        df_revokes = ad_offchain.extract(extract_params)

        # load offchain revocations
        ad_offchain.load(df_revokes, 'attestations')


    sync()

main()
