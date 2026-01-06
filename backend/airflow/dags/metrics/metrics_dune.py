
from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='metrics_dune',
    description='Load aggregates metrics such as txcount, daa, fees paid, stablecoin mcap where applicable.',
    tags=['metrics', 'daily'],
    start_date=datetime(2023,6,5),
    schedule='55 01 * * *' ## needs to run before sql_materialize because of acthive addresses agg (i.e. megaeth AA)
)

def etl():
    @task()
    def run_fact_kpis():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'economics_da',
                    'query_id': 4046209,
                    'params': {'days': 3}
                },
                {
                    'name': 'combined_kpis', # combined different queries
                    'query_id': 5338492,
                    'params': {'days': 3}
                },
                {
                    'name': 'mega_fundamentals',
                    'query_id': 6357340,
                    'params': {'days': 3}
                },
                {
                    'name': 'polygon_pos_fundamentals',
                    'query_id': 6371012,
                    'params': {'days': 3}
                },
            ],
            'prepare_df': 'prepare_df_metric_daily',
            'load_type': 'fact_kpis'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)
        # load
        ad.load(df)

    # @task()
    # def run_inscriptions():
    #     import os
    #     from src.db_connector import DbConnector
    #     from src.adapters.adapter_dune import AdapterDune

    #     adapter_params = {
    #         'api_key' : os.getenv("DUNE_API")
    #     }
    #     load_params = {
    #         'queries': [
    #             {
    #                 'name': 'inscriptions',
    #                 'query_id': 3346613,
    #                 'params': {'days': 1000}
    #             }
    #         ],
    #         'prepare_df': 'prepare_df_incriptions',
    #         'load_type': 'inscription_addresses'
    #     }

    #     # initialize adapter
    #     db_connector = DbConnector()
    #     ad = AdapterDune(adapter_params, db_connector)
    #     # extract
    #     df = ad.extract(load_params)
    #     # load
    #     ad.load(df)
    
    #######################
    ### MegaETH tasks
    ########################
    
    @task()
    def run_mega_aa():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_aa',
                    'query_id': 6369584,
                    'params': {'days': 2}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_aa_daily',
            'load_type': 'CUSTOM'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for megaeth active addresses on contract level.")

        # prepare for fact_active_addresses_contract
        df_contract_aa = df.copy()
        df_contract_aa = df_contract_aa[df_contract_aa.address != '<nil>']

        df_contract_aa['origin_key'] = 'megaeth'
        df_contract_aa.set_index(['address', 'date', 'origin_key', 'from_address'], inplace=True)
        db_connector.upsert_table('fact_active_addresses_contract', df_contract_aa)

        # prepare for fact_active_addresses
        df_aa = df.copy()
        df_aa = df_aa.drop(columns=['address'])
        df_aa = df_aa.drop_duplicates(subset=['date', 'from_address'])
        df_aa = df_aa.rename(columns={'from_address': 'address'})

        # additional prep steps
        df_aa['origin_key'] = 'megaeth'
        df_aa.set_index(['address', 'date', 'origin_key'], inplace=True)
        db_connector.upsert_table('fact_active_addresses', df_aa)

        
    @task()
    def run_mega_contract_level():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_contract_level_daily',
                    'query_id': 6360383,
                    'params': {'days': 2}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_daily',
            'load_type': 'blockspace_fact_contract_level'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for contract level.")
        
        # additional prep steps
        df['origin_key'] = 'megaeth'
        df.set_index(['address', 'date', 'origin_key'], inplace=True)

        # load
        ad.load(df)
        
    @task()
    def run_mega_category_level():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'megaeth_category_level_daily',
                    'query_id': 6364531,
                    'params': {'days': 2}
                }
            ],
            'prepare_df': 'prepare_df_category_level_daily',
            'load_type': 'blockspace_fact_category_level'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for category level.")
        
        # additional prep steps
        df['origin_key'] = 'megaeth'
        df.set_index(['category_id', 'date', 'origin_key'], inplace=True)

        # load
        ad.load(df)
        
    #######################
    ### Polygon POS tasks
    ########################

    @task()
    def run_polygon_contract_level():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'polygon_pos_contract_level_daily',
                    'query_id': 6474479,
                    'params': {'days': 2}
                }
            ],
            'prepare_df': 'prepare_df_contract_level_daily',
            'load_type': 'blockspace_fact_contract_level'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for contract level.")
        
        # additional prep steps
        df['origin_key'] = 'polygon_pos'
        df.set_index(['address', 'date', 'origin_key'], inplace=True)

        # load
        ad.load(df)


    @task()
    def run_polygon_category_level():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'polygon_pos_category_level_daily',
                    'query_id': 6474511,
                    'params': {'days': 2}
                }
            ],
            'prepare_df': 'prepare_df_category_level_daily',
            'load_type': 'blockspace_fact_category_level'
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        # extract
        df = ad.extract(load_params)

        print(f"Loaded {df.shape[0]} rows for category level.")
        
        # additional prep steps
        df['origin_key'] = 'polygon_pos'
        df.set_index(['category_id', 'date', 'origin_key'], inplace=True)

        # load
        ad.load(df)

    @task()
    def run_glo_holders():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        
        ## if it's first day of month, run glo holders dag
        if datetime.now().day == 1:
            adapter_params = {
                'api_key' : os.getenv("DUNE_API")
            }
            load_params = {
                'queries': [
                    {
                        'name': 'glo_holders',
                        'query_id': 3732844
                    }
                ],
                'prepare_df': 'prepare_df_glo_holders',
                'load_type': 'glo_holders'
            }

            # initialize adapter
            db_connector = DbConnector()
            ad = AdapterDune(adapter_params, db_connector)
            # extract
            df = ad.extract(load_params)
            # load
            ad.load(df)
        else:
            print("Today is not 1st day of month, skipping GLO holders DAG run.")

    @task()
    def check_for_depreciated_L2_trx():
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        from src.misc.helper_functions import send_discord_message

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }
        load_params = {
            'queries': [
                {
                    'name': 'check-for-depreciated-L2-trx',
                    'query_id': 4544157
                }
            ]
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        df = ad.extract(load_params)
        for i, row in df.iterrows():
            send_discord_message(f"<@790276642660548619> The economics mapping function for **{row.l2}** has changed. Details: settlement on {row.settlement_layer}, {row.no_of_trx} trx per day, from_address: {row.from_address}, to_address: {row.to_address}, method: {row.method}.", os.getenv('DISCORD_ALERTS'))


    run_fact_kpis()
    #run_inscriptions() # paused as of Jan 2025, no one uses inscriptions. Backfilling easily possible if needed.
    run_glo_holders()
    check_for_depreciated_L2_trx()
    
    ## MegaETH tasks
    run_mega_aa()
    run_mega_contract_level()
    run_mega_category_level()
    
    ## Polygon POS tasks
    run_polygon_contract_level()
    run_polygon_category_level()
    
etl()