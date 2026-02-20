from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 5,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=10),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='oli_reattest',
    description='Checks if new attestations have been added to the label pool and submits them to airtable for review.',
    tags=['oli', 'hourly'],
    start_date=datetime(2026,2,19),
    schedule='30 * * * *'
)

def etl():
    @task()
    def airtable_write_label_pool_reattest():
        """
        This task writes the approved label pool reattest table to Airtable.
        """
        from eth_utils import to_checksum_address
        from src.db_connector import DbConnector
        from src.misc.helper_functions import send_discord_message
        import src.misc.airtable_functions as at
        from pyairtable import Api
        import os

        # get new untrusted owner_project attestations from labels view in web3 db
        db_connector = DbConnector()
        yesterday = datetime.now() - timedelta(hours=1)
        load_params = {'date': yesterday.strftime('%Y-%m-%d')}
        df_air = db_connector.execute_jinja('/oli/extract_labels_for_review.sql.j2', load_params, load_into_df=True)

        if df_air.empty == False:

            # push to airtable
            AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
            AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
            api = Api(AIRTABLE_API_KEY)
            table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')
            
            ## if column chain_id starts with 'eip155' then do checksum address
            df_air['address'] = df_air.apply(
                lambda row: to_checksum_address(row['address']) if row['chain_id'].startswith('eip155') else row['address'], axis=1
            )
            
            df_air['attester'] = df_air['attester'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
            
            # exchange the category with the id & make it a list
            cat = api.table(AIRTABLE_BASE_ID, 'Sub Categories')
            df_cat = at.read_airtable(cat)
            df_air = df_air.replace({'usage_category': df_cat.set_index('category_id')['id']})
            df_air['usage_category'] = df_air['usage_category'].apply(lambda x: [x])
            
            # exchange the project with the id & make it a list
            proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
            df_proj = at.read_airtable(proj)
            df_air = df_air.replace({'owner_project': df_proj.set_index('Name')['id']})
            df_air['owner_project'] = df_air['owner_project'].apply(lambda x: [x])
            
            # exchange the chain chain_id with the id & make it a list
            chains = api.table(AIRTABLE_BASE_ID, 'Chains')
            df_chains = at.read_airtable(chains)
            df_air = df_air.replace({'chain_id': df_chains.set_index('caip2')['id']})
            df_air['chain_id'] = df_air['chain_id'].apply(lambda x: [x])
            
            # rename chain_id to origin_key
            df_air = df_air.rename(columns={'chain_id': 'origin_key'})
            
            # catch errors and log them in the error column (for owner_project and usage_category)
            df_air['error'] = None
            df_air['error'] = df_air.apply(
                lambda row: (row['error'] or '') + f" failed owner_project: '{row['owner_project'][0]}'" 
                if row['owner_project'][0] is not None and not row['owner_project'][0].startswith('rec') else row['error'], axis=1
            )
            df_air['error'] = df_air.apply(
                lambda row: (row['error'] or '') + f" failed usage_category: '{row['usage_category'][0]}'" 
                if row['usage_category'][0] is not None and not row['usage_category'][0].startswith('rec') else row['error'], axis=1
            )
            df_air['usage_category'] = df_air['usage_category'].apply(lambda x: x if x[0] is not None and x[0].startswith('rec') else [])
            df_air['owner_project'] = df_air['owner_project'].apply(lambda x: x if x[0] is not None and x[0].startswith('rec') else [])
            
            # write to airtable df
            at.push_to_airtable(table, df_air)
            
            # send discord message
            send_discord_message(f"{df_air.shape[0]} new attestations submitted to label pool for {df_air['owner_project'].unique().tolist()}, please review in airtable.", os.getenv('DISCORD_CONTRACTS'))

    airtable_write_label_pool_reattest()
etl()