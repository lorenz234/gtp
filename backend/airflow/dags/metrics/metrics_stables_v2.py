from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_stables_v2',
    description='Load Stablecoin balances via RPCs',
    tags=['metrics', 'daily'],
    start_date=datetime(2024,4,21),
    schedule='15 01 * * *'
)

def etl():

    @task()
    def pull_in_stables():
        from src.db_connector import DbConnector
        from src.adapters.adapter_stables_v2 import AdapterStablecoinSupply

        # Initialize
        db_connector = DbConnector()
        ad = AdapterStablecoinSupply({}, db_connector)

        # define extract_params
        extract_params = {
            'origin_keys': ['*'],
            'token_ids': ['*'],
            'metric_keys': ['*']
        }
        
        # extract and load
        df = ad.extract(extract_params)
        ad.load(df)

    @task()
    def calculate_totals():
        from src.misc.jinja_helper import execute_jinja_query
        from src.db_connector import DbConnector
        db_connector = DbConnector()

        # this is not the bottleneck, just aggregate all the data we have
        params = {
            'origin_keys': db_connector.get_table('sys_main_conf')['origin_key'].tolist(),
            'days': 9999
        }

        df = execute_jinja_query(
            db_connector,
            'chain_metrics/select_total_stable_supply.sql.j2',
            params,
            return_df=True
        )

        df = df.set_index(['origin_key', 'date', 'metric_key'])
        df_shape = db_connector.upsert_table('fact_kpis', df)
        print(f'Upserted {df_shape} rows into fact_kpis')

    # run one after the other
    pull_in_stables() >> calculate_totals()
    
etl()