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
    def create_jsons():
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
        config = config[config['chain_type'].isin(['L1', 'L2'])]
        config = config[config['api_deployment_flag'] == 'PROD']


        df_chains = config[['origin_key', 'name']]
        chains = config['origin_key'].tolist()

        ### timeseries data (single query for all chains)
        df_all = execute_jinja_query(db_connector, "api/quick_bites/stables_top_per_chain_timeseries_v2.sql.j2", {}, True)

        dt = pd.to_datetime(df_all['date'], errors="raise", utc=True)
        df_all['unix_timestamp'] = (
            (dt - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")

        # load token metadata for symbol and color lookups
        df_stables_meta = db_connector.get_table("sys_stables_v2")
        token_color_map = df_stables_meta.set_index('token_id')['color_hex'].to_dict()
        token_symbol_map = df_stables_meta.set_index('token_id')['symbol'].to_dict()

        for chain in chains:
            print(f"Processing stablecoin supply for chain: {chain}")

            df = df_all[df_all['origin_key'] == chain].copy()
            if df.empty:
                print(f"  No data for {chain}, skipping.")
                continue

            df[['value', 'value_usd']] = df[['value', 'value_usd']].fillna(0)
            df = df.sort_values(['date', 'token_id']).reset_index(drop=True)

            unique_dates = df['date'].unique()
            token_list = sorted(df['token_id'].unique().tolist())

            values = []
            for date in unique_dates:
                date_data = df[df['date'] == date]
                unix_ts = int(date_data['unix_timestamp'].iloc[0])
                row = [unix_ts]
                for token in token_list:
                    token_value = date_data[date_data['token_id'] == token]['value_usd']
                    row.append(float(token_value.iloc[0]) if len(token_value) > 0 else 0.0)
                values.append(row)

            types = ["unix"] + token_list
            colors = ['#FFFFFF' if token == 'other' else token_color_map.get(token) for token in token_list]
            symbols = ['other' if token == 'other' else token_symbol_map.get(token) for token in token_list]

            data_dict = {
                "data": {
                    "timeseries": {
                        "types": types,
                        "values": values
                    },
                    "colors": colors,
                    "symbols": symbols,
                    "chain": chain
                }
            }

            data_dict = fix_dict_nan(data_dict, f'stablecoins_{chain}', send_notification=False)
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/stablecoins/chains/top_{chain}', data_dict, cf_distribution_id, invalidate=False)


        ### timeseries data per project (single query for all projects)
        df_all_proj = execute_jinja_query(db_connector, "api/quick_bites/stables_top_per_project_timeseries.sql.j2", {}, True)

        dt_proj = pd.to_datetime(df_all_proj['date'], errors="raise", utc=True)
        df_all_proj['unix_timestamp'] = (
            (dt_proj - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")

        projects = df_all_proj['owner_project'].dropna().unique().tolist()

        for project in projects:
            print(f"Processing stablecoin supply for project: {project}")

            df = df_all_proj[df_all_proj['owner_project'] == project].copy()
            if df.empty:
                continue

            df[['value', 'value_usd']] = df[['value', 'value_usd']].fillna(0)
            df = df.sort_values(['date', 'token_id']).reset_index(drop=True)

            unique_dates = df['date'].unique()
            token_list = sorted(df['token_id'].unique().tolist())

            values = []
            for date in unique_dates:
                date_data = df[df['date'] == date]
                unix_ts = int(date_data['unix_timestamp'].iloc[0])
                row = [unix_ts]
                for token in token_list:
                    token_value = date_data[date_data['token_id'] == token]['value_usd']
                    row.append(float(token_value.iloc[0]) if len(token_value) > 0 else 0.0)
                values.append(row)

            types = ["unix"] + token_list
            colors = ['#FFFFFF' if token == 'other' else token_color_map.get(token) for token in token_list]
            symbols = ['other' if token == 'other' else token_symbol_map.get(token) for token in token_list]

            data_dict = {
                "data": {
                    "timeseries": {
                        "types": types,
                        "values": values
                    },
                    "colors": colors,
                    "symbols": symbols,
                    "owner_project": project
                }
            }

            data_dict = fix_dict_nan(data_dict, f'stablecoins_project_{project}', send_notification=False)
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/stablecoins/projects/{project}', data_dict, cf_distribution_id, invalidate=False)


        ## project dropdown
        df_oss = db_connector.get_table("oli_oss_directory")
        df_proj_dropdown = df_oss[df_oss['name'].isin(projects)][['name', 'display_name', 'description', 'websites', 'github', 'social', 'logo_path']].copy()
        df_proj_dropdown = df_proj_dropdown.rename(columns={'name': 'owner_project'})
        project_dropdown_list = df_proj_dropdown.to_dict(orient='records')
        dict_proj_dropdown = {"dropdown_values": project_dropdown_list}
        dict_proj_dropdown = fix_dict_nan(dict_proj_dropdown, 'project_dropdown', send_notification=True)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/stablecoins/dropdown-projects', dict_proj_dropdown, cf_distribution_id, invalidate=False)


        ## dropdown
        ticker_name_list = df_chains[['origin_key', 'name']].to_dict(orient='records')
        dict_dropdown = {
            "dropdown_values": ticker_name_list,
        }
        # Fix NaN values in the dict_dropdown
        dict_dropdown = fix_dict_nan(dict_dropdown, 'chain_dropdown', send_notification=True)
        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/stablecoins/dropdown-chains', dict_dropdown, cf_distribution_id, invalidate=False)


        ### empty_cloudfront_cache
        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/stablecoins/*')

    create_jsons()

run_dag()
