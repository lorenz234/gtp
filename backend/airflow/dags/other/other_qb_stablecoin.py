from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_stablecoin",
    description="",
    tags=["other"],
    start_date=datetime(2026, 1, 22),
    schedule="11 2 * * *",  # Every day at 02:21
)
def run_dag():

    @task
    def run_task():
        from src.misc.jinja_helper import execute_jinja_query
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        import pandas as pd
        import os

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        # get chains to process stablecoin supply for
        config = db_connector.get_table("sys_main_conf")
        config = config[config['chain_type'] == 'L2']
        config = config[config['api_in_main'] == True]

        df_chains = config[['origin_key', 'name']]
        chains = config['origin_key'].tolist()

        for chain in chains:
            print(f"Processing stablecoin supply for chain: {chain}")
            
            ### timeseries data
            
            if chain == 'ethereum':
                df = execute_jinja_query(db_connector, "api/quick_bites/stables_top_per_chain_timeseries_ethereum.sql.j2", {}, True)
            else:
                df = execute_jinja_query(db_connector, "api/quick_bites/stables_top_per_chain_timeseries.sql.j2", {'origin_key': chain}, True)
            
            # create jsons
            df['unix_timestamp'] = pd.to_datetime(df['date']).astype(int) // 10**6  # Convert to milliseconds
            
            # Sort by date and symbol to ensure consistent ordering
            df = df.sort_values(['date', 'symbol']).reset_index(drop=True)
            
            # Get unique dates and stablecoins
            unique_dates = df['date'].unique()
            stablecoin_list = sorted(df['symbol'].unique().tolist())
            
            # Create combined values array: [unix_timestamp, value_usdt, value_usdm, value_cusd, value_usdc]
            values = []
            for date in unique_dates:
                date_data = df[df['date'] == date]
                unix_ts = int(date_data['unix_timestamp'].iloc[0])  # Convert to native Python int
                
                # Create row starting with timestamp
                row = [unix_ts]
                
                # Add value for each stablecoin in sorted order
                for stablecoin in stablecoin_list:
                    stablecoin_value = date_data[date_data['symbol'] == stablecoin]['value_usd']
                    # Convert to native Python float
                    value = float(stablecoin_value.iloc[0]) if len(stablecoin_value) > 0 else 0.0
                    row.append(value)
                
                values.append(row)
            
            # Create types array
            types = ["unix"] + [f"{coin}" for coin in stablecoin_list]
            
            # Create the data dictionary
            data_dict = {
                "data": {
                    "timeseries": {
                        "types": types,
                        "values": values
                    },
                    "chain": chain
                }
            }
            
            # Fix NaN values
            data_dict = fix_dict_nan(data_dict, f'stablecoins_{chain}', send_notification=False)
            
            # Upload to S3
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/stablecoins/timeseries/top_{chain}', data_dict, cf_distribution_id, invalidate=False)
            

            ### table data

            if chain == 'ethereum':
                df = execute_jinja_query(db_connector, "api/quick_bites/stables_per_chain_table_ethereum.sql.j2", {}, True)
            else:
                df = execute_jinja_query(db_connector, "api/quick_bites/stables_per_chain_table.sql.j2", {'origin_key': chain}, True)
            
            # Define columns and types
            columns = [
                "origin_key",
                "metric_key", 
                "fiat",
                "value",
                "value_usd",
                "name",
                "symbol",
                "logo"
            ]

            types = [
                "string",
                "string", 
                "string",
                "number",
                "number",
                "string",
                "string",
                "string"
            ]

            # Convert DataFrame rows to list of lists
            rows_data = []
            for _, row in df.iterrows():
                row_list = []
                for col in columns:
                    row_list.append(row[col])
                rows_data.append(row_list)

            # Create the data structure
            data_dict = {
                "data": {
                    "stables_per_chain": {
                        "columns": columns,
                        "types": types,
                        "rows": rows_data
                    }
                }
            }

            # Fix NaN values
            data_dict = fix_dict_nan(data_dict, 'stables_per_chain_table', send_notification=True)
            
            # upload to S3
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/stablecoins/tables/{chain}', data_dict, cf_distribution_id, invalidate=False)


        ## dropdown
        ticker_name_list = df_chains[['origin_key', 'name']].to_dict(orient='records')
        dict_dropdown = {
            "dropdown_values": ticker_name_list,
        }
        # Fix NaN values in the dict_dropdown
        dict_dropdown = fix_dict_nan(dict_dropdown, 'chain_dropdown', send_notification=True)
        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/stablecoins/dropdown', dict_dropdown, cf_distribution_id, invalidate=False)


        ### empty_cloudfront_cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/stablecoins/*')



run_dag()