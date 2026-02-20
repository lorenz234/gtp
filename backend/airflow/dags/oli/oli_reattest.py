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

        db_connector = DbConnector()
        hours = 1

        # from our db, get the labels that were attested in the last 24 hours
        yesterday = datetime.now() - timedelta(hours=hours)
        #yesterday = datetime.today() - timedelta(days=1)
        load_params = {'date': yesterday.strftime('%Y-%m-%d %H:00:00')}
        df_attested = db_connector.execute_jinja('/oli/extract_labels_for_review.sql.j2', load_params, load_into_df=True)

        if df_attested.empty == False:
            df_attested['address'] = df_attested.apply(
                lambda row: to_checksum_address(row['address']) if row['chain_id'].startswith('eip155') else row['address'], axis=1
            )
            df_attested = df_attested[['address', 'attester', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]

            # from airtable, get the labels that are in the Label Pool Reattest table
            AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
            AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
            api = Api(AIRTABLE_API_KEY)
            table = api.table(AIRTABLE_BASE_ID, 'Label Pool Reattest')

            # read all approved labels in 'Label Pool Reattest'
            df_in_airtable = at.read_all_label_pool_reattest(api, AIRTABLE_BASE_ID, table, approved=False)
            df_in_airtable = df_in_airtable[['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category']]
            df_in_airtable['address'] = df_in_airtable['address'].str.replace('\\x', '0x')

            ## create df_new with rows that are in df_air but not in df based on all columns
            df_new = df_attested.merge(df_in_airtable, on=['address', 'chain_id', 'contract_name', 'owner_project', 'usage_category'], how='left', indicator=True)
            df_new = df_new[df_new['_merge'] == 'left_only']
            df_new = df_new.drop(columns=['_merge'])
            
            if df_new.empty == False:
                ## if column chain_id starts with 'eip155' then do checksum address
                df_new['address'] = df_new.apply(
                    lambda row: to_checksum_address(row['address']) if row['chain_id'].startswith('eip155') else row['address'], axis=1
                )
                
                df_new['attester'] = df_new['attester'].apply(lambda x: to_checksum_address('0x' + bytes(x).hex()))
                
                # exchange the category with the id & make it a list
                cat = api.table(AIRTABLE_BASE_ID, 'Sub Categories')
                df_cat = at.read_airtable(cat)
                df_new = df_new.replace({'usage_category': df_cat.set_index('category_id')['id']})
                df_new['usage_category'] = df_new['usage_category'].apply(lambda x: [x])
                
                # exchange the project with the id & make it a list
                proj = api.table(AIRTABLE_BASE_ID, 'OSS Projects')
                df_proj = at.read_airtable(proj)
                df_new = df_new.replace({'owner_project': df_proj.set_index('Name')['id']})
                df_new['owner_project'] = df_new['owner_project'].apply(lambda x: [x])
                
                # exchange the chain chain_id with the id & make it a list
                chains = api.table(AIRTABLE_BASE_ID, 'Chains')
                df_chains = at.read_airtable(chains)
                df_new = df_new.replace({'chain_id': df_chains.set_index('caip2')['id']})
                df_new['chain_id'] = df_new['chain_id'].apply(lambda x: [x])
                
                # rename chain_id to origin_key
                df_new = df_new.rename(columns={'chain_id': 'origin_key'})
                
                # catch errors and log them in the error column (for owner_project and usage_category)
                df_new['error'] = None
                df_new['error'] = df_new.apply(
                    lambda row: (row['error'] or '') + f" failed owner_project: '{row['owner_project'][0]}'" 
                    if row['owner_project'][0] is not None and not row['owner_project'][0].startswith('rec') else row['error'], axis=1
                )
                df_new['error'] = df_new.apply(
                    lambda row: (row['error'] or '') + f" failed usage_category: '{row['usage_category'][0]}'" 
                    if row['usage_category'][0] is not None and not row['usage_category'][0].startswith('rec') else row['error'], axis=1
                )
                df_new['usage_category'] = df_new['usage_category'].apply(lambda x: x if x[0] is not None and x[0].startswith('rec') else [])
                df_new['owner_project'] = df_new['owner_project'].apply(lambda x: x if x[0] is not None and x[0].startswith('rec') else [])
                
                # write to airtable df
                at.push_to_airtable(table, df_new)

                # send discord message
                send_discord_message(f"{df_new.shape[0]} new attestations submitted to label pool, please review in airtable.", os.getenv('DISCORD_CONTRACTS'))
            else:
                print('No new attestations to submit to label pool.')
        else:
            print(f'No attestations since {yesterday}.')

    airtable_write_label_pool_reattest()
etl()