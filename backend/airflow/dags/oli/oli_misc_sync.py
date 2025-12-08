




    @task()
    def refresh_oli_tags(): # not needed anymore imo, needed but not here in OLI airtable DAG!
        """
        This task adds new oli tags to oli_tags table, only addative, doesn't remove tags!
        """
        from src.db_connector import DbConnector
        from src.misc.helper_functions import get_all_oli_tags_from_github

        # get tags from gtp-dna Github
        df = get_all_oli_tags_from_github()

        # upsert/update oli_tags table 
        db_connector = DbConnector()
        df = df.set_index('tag_id')
        db_connector.upsert_table('oli_tags', df, if_exists='update')


           @task()
    def refresh_trusted_entities(): # not needed anymore imo
        """
        This task gets the trusted entities from the gtp-dna Github and upserts them to the oli_trusted_entities table.
        """

        from src.misc.helper_functions import get_trusted_entities
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # get trusted entities from gtp-dna Github, rows with '*' are expanded based on public.oli_tags
        df = get_trusted_entities(db_connector)

        # turn attester into bytea
        df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])

        # set attester & tag_id as index
        df = df.set_index(['attester', 'tag_id'])

        # upsert to oli_trusted_entities table, making sure to delete first for full refresh
        db_connector.delete_all_rows('oli_trusted_entities')
        db_connector.upsert_table('oli_trusted_entities', df)


from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 5,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_label_pool',
    description='Loads raw labels into the data pool',
    tags=['contracts', 'oli', 'raw'],
    start_date=datetime(2023, 6, 5),
    schedule='*/30 * * * *',  # Runs every 30 minutes
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync_attestations():

        from src.db_connector import DbConnector
        from src.adapters.adapter_oli_label_pool import AdapterLabelPool

        db_connector = DbConnector()
        ad = AdapterLabelPool({}, db_connector)

        # get new labels from the GraphQL endpoint
        df = ad.extract() # you can set from when to load [optional] e.g. {'time_created': 1758504151}
        
        # load labels into bronze table, then also increment to silver and lastly pushes untrusted owner_project labels to airtable
        ad.load(df)

    sync_attestations()

main()
