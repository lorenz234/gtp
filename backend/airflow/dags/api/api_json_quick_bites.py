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
                    "arbitrum": {}
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
        for origin_key in ['ethereum', 'optimism', 'base', 'unichain', 'arbitrum']:
            query_parameters = {
                'origin_key': origin_key,
                'metric_key': 'txcount_type4',
                'days': 120
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

    @task()
    def run_arbitrum_timeboost():
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
                "fees_paid_base_eth" : {},
                "fees_paid_priority_eth" : {}
            }    
        }    

        for metric_key in ['fees_paid_base_eth', 'fees_paid_priority_eth', 'fees_paid_priority_usd']:
            query_parameters = {
                'origin_key': 'arbitrum',
                'metric_key': metric_key,
                'days': (datetime.now(timezone.utc) - datetime(2025, 4, 10, tzinfo=timezone.utc)).days ## days since '2025-04-10' to today
            }
            df = execute_jinja_query(db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
            df.sort_values(by=['date'], inplace=True, ascending=True)
            df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
            df = df.drop(columns=['date'])

            data_dict["data"][metric_key]= {
                "daily": {
                    "types": df.columns.tolist(),
                    "values": df.values.tolist()
                }
            }

            if metric_key in ['fees_paid_priority_eth', 'fees_paid_priority_usd']:
                data_dict["data"][metric_key]["total"] = df['value'].sum()

        data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        data_dict = fix_dict_nan(data_dict, 'arbitrum-timeboost')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/arbitrum-timeboost', data_dict, cf_distribution_id)
        
    @task()
    def run_shopify_usdc():
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
                "gross_volume_usdc" : {},
                "total_unique_merchants": {},
                "total_unique_payers": {},
                "new_payers": {},
                "returning_payers": {},
                "new_merchants": {},
                "returning_merchants": {},
            }    
        }    

        for metric_key in ['gross_volume_usdc', 'total_unique_merchants', 'total_unique_payers', 'new_payers', 'returning_payers', 'new_merchants', 'returning_merchants']:
            query_parameters = {
                'origin_key': 'shopify_usdc',
                'metric_key': metric_key,
                'days': (datetime.now(timezone.utc) - datetime(2025, 6, 1, tzinfo=timezone.utc)).days ## days since '2025-06-01' to today
            }
            df = execute_jinja_query(db_connector, "api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
            df.sort_values(by=['date'], inplace=True, ascending=True)
            
            ## fill missing dates with 0
            df_all_dates = pd.DataFrame({'date': pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')})
            df = pd.merge(df_all_dates, df, on='date', how='left')
            df['value'] = df['value'].fillna(0)  # Fill NaN values with 0
            
            df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
            df = df.drop(columns=['date'])
                
            ## Get over time data for charts
            if metric_key in ['gross_volume_usdc', 'new_payers', 'returning_payers', 'new_merchants', 'returning_merchants']:
                data_dict["data"][metric_key]= {
                    "daily": {
                        "types": df.columns.tolist(),
                        "values": df.values.tolist()
                    }
                }

            ## Get total for KPI cards as sum
            if metric_key in ['gross_volume_usdc']:
                data_dict["data"][metric_key]["total"] = df['value'].sum()

            ## Get total for KPI cards as highest value
            if metric_key in ['total_unique_merchants', 'total_unique_payers']:
                data_dict["data"][metric_key]["total"] = df['value'].max()

        data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        data_dict = fix_dict_nan(data_dict, 'shopify-usdc')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/shopify-usdc', data_dict, cf_distribution_id)
        
    @task()
    def run_ethereum_scaling():
        ## TODO: have projection update? but not guaranteed that we'll hit 10k TPS in same timeframe...

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
                "historical_tps" : {},
                "projected_tps": {},
                "target_tps": {},
                "l2_projected_tps": {}
            }    
        }
        
        ## Historical TPS
        query_parameters = {
            'origin_key': 'ethereum'
        }
        df = execute_jinja_query(db_connector, "api/select_tps_historical.sql.j2", query_parameters, return_df=True)
        # Fix the 'month' column to proper datetime values before sorting
        df['month'] = pd.to_datetime(df['month'], errors='coerce')
        df = df.sort_values(by='month', ascending=True)

        df['unix'] = df['month'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['month'])

        df_tps = df[['unix', 'tps']].copy()

        data_dict["data"]['historical_tps']= {
            "monthly": {
                "types": df_tps.columns.tolist(),
                "values": df_tps.values.tolist()
            }
        }

        ## get last value for current tps
        data_dict["data"]['historical_tps']["total"] = df_tps['tps'].iloc[-1]

        ## Projected TPS Ethereum Mainnet
        query_parameters = {
            'start_day': '2025-10-01',
            'months_total': 69,
            #'starting_tps': df_tps['tps'].iloc[-1],
            'starting_tps': 20,
            'annual_factor': 3,
        }
        df = execute_jinja_query(db_connector, "api/select_tps_projected.sql.j2", query_parameters, return_df=True)
        # Fix the 'month' column to proper datetime values before sorting
        df['month'] = pd.to_datetime(df['month'], errors='coerce')
        df = df.sort_values(by='month', ascending=True)

        df['unix'] = df['month'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['month'])

        df_tps = df[['unix', 'tps']].copy()

        data_dict["data"]['projected_tps']= {
            "monthly": {
                "types": df_tps.columns.tolist(),
                "values": df_tps.values.tolist()
            }
        }

        ## Target TPS Ethereum Mainnet
        df_target = df[['unix', 'target_tps']].copy()

        data_dict["data"]['target_tps']= {
            "monthly": {
                "types": df_target.columns.tolist(),
                "values": df_target.values.tolist()
            }
        }

        ## Projected TPS Layer 2s
        query_parameters = {
            'start_day': '2025-10-01',
            'months_total': 69,
            #'starting_tps': df_tps['tps'].iloc[-1],
            'starting_tps': 350,
            'annual_factor': 4.1,
        }
        df = execute_jinja_query(db_connector, "api/select_tps_projected.sql.j2", query_parameters, return_df=True)
        # Fix the 'month' column to proper datetime values before sorting
        df['month'] = pd.to_datetime(df['month'], errors='coerce')
        df = df.sort_values(by='month', ascending=True)

        df['unix'] = df['month'].apply(lambda x: x.timestamp() * 1000)
        df = df.drop(columns=['month'])

        df_tps = df[['unix', 'tps']].copy()

        data_dict["data"]['l2_projected_tps']= {
            "monthly": {
                "types": df_tps.columns.tolist(),
                "values": df_tps.values.tolist()
            }
        }

        data_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        data_dict = fix_dict_nan(data_dict, 'ethereum-scaling')

        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/ethereum-scaling/data', data_dict, cf_distribution_id)
    
    @task()
    def run_network_graph():
        import pandas as pd
        from src.db_connector import DbConnector
        import time

        db_connector = DbConnector()

        df_main = pd.DataFrame()
        oks = ['optimism', 'mode', 'base', 'ethereum', 'arbitrum', 'linea', 'ink', 'zksync_era', 'zora', 'soneium', 'unichain', 'scroll', 'mantle', 'celo']
        #oks = ['optimism', 'mode', 'base', 'ethereum', 'arbitrum']

        for origin_key in oks:
            print(f'Processing {origin_key}')
            start_time = time.time()

            query_cca = f"""
            with excl_chain as (
                select
                    address,
                    origin_key
                from fact_active_addresses faa
                where faa."date" < current_date 
                    AND "date" >= current_date - interval '7 days'
                    and origin_key <> '{origin_key}'
                    and origin_key IN ('{"','".join(oks)}')
            )

            , tmp as (
                SELECT
                    aa.address AS address,
                    ex.origin_key as cca
                FROM fact_active_addresses aa
                    left join excl_chain ex on aa.address = ex.address
                WHERE aa."date" < current_date 
                    AND aa."date" >= current_date - interval '7 days'
                    and aa.origin_key = '{origin_key}'
                group by 1,2
            )

            select
                '{origin_key}' as origin_chain,
                coalesce(cca,'exclusive') as cross_chain,
                Count(*) as value
            from tmp
            group by 1,2

            """

            df_cca = db_connector.execute_query(query_cca, load_df=True)
            
            query_total = f"""
                select value 
                from fact_kpis 
                where origin_key = '{origin_key}'
                and metric_key = 'aa_last7d'
                order by date desc
                limit 1
            """

            total = db_connector.execute_query(query_total, load_df=True)
            total = total.value.values[0]

            ## add row to df_cca witch total
            df_cca = pd.concat([df_cca, pd.DataFrame([{
                'origin_chain': origin_key,
                'cross_chain': 'total',
                'value': total
            }])], ignore_index=True)

            df_cca['percentage'] = df_cca['value'] / total
            
            df_main = pd.concat([df_main, df_cca], ignore_index=True)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Processing {origin_key} took {elapsed_time:.2f} seconds")
            
        # rename columns to Source, Target, Value, Percentage
        df_main = df_main.rename(columns={
            'origin_chain': 'Source',
            'cross_chain': 'Target',
            'value': 'Value',
            'percentage': 'Percentage'
        })

        ## create dict from df
        df_dict = df_main.to_dict(orient='records')

        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        df_dict = fix_dict_nan(df_dict, 'network graph')
        upload_json_to_cf_s3(s3_bucket, f'v1/misc/interop/data', df_dict, cf_distribution_id)

    run_pectra_fork()    
    run_arbitrum_timeboost()
    run_shopify_usdc()
    run_ethereum_scaling()
    run_network_graph()

json_creation()