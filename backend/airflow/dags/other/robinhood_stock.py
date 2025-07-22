import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='robinhood_stock_QB',
    description='Data for Robinhood stock tracker.',
    tags=['other'],
    start_date=datetime(2025,7,22),
    schedule='12 3 * * *'
)

def run_dag():

    @task()
    def pull_data_from_dune():      
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        adapter_params = {
            'api_key' : os.getenv("DUNE_API")
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)

        # first load, list of tokenized assets
        load_params = {
            'queries': [
                {
                    'name': 'Robinhood_stock_list',
                    'query_id': 5429585,
                    'params': {'days': 3}
                }
            ],
            'prepare_df': 'prepare_robinhood_list',
            'load_type': 'robinhood_stock_list'
        }
        df = ad.extract(load_params)
        ad.load(df)

        # second load, daily values for the tokenized assets
        load_params2 = {
            'queries': [
                {
                    'name': 'Robinhood_stock_daily',
                    'query_id': 5435746,
                    'params': {'days': 3}
                }
            ],
            'prepare_df': 'prepare_robinhood_daily',
            'load_type': 'robinhood_daily'
        }
        df = ad.extract(load_params2)
        ad.load(df)

    @task()
    def pull_data_from_yfinance():
        from src.adapters.adapter_yfinance_stocks import AdapterYFinance
        from src.db_connector import DbConnector

        db = DbConnector()
        yfi = AdapterYFinance({}, db)

        load_params = {
                'tickers': ['*'],
                'endpoints': ['Close'],
                'days': 5,
                'table': 'robinhood_daily',
                'prepare_df': 'prepare_df_robinhood_daily'
            }

        df = yfi.extract(load_params)
        yfi.load(df)

    @task()
    def create_json_file():
        import pandas as pd
        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        ### Load the first dataset for daily
        df = execute_jinja_query(db_connector, "api/quick_bites/robinhood_merged_daily.sql.j2", query_parameters={}, return_df=True)
        ticker_list = df['ticker'].unique().tolist()
        df['unix_timestamp'] = pd.to_datetime(df['date']).astype(int) // 10**6  # Convert to milliseconds

        def find_last_zero_market_value_index(series: pd.Series) -> int:
            """
            Find the index of the last zero in total_market_value before non-zero values begin.
            Returns -1 if no zeros found or series is empty.
            """
            # Find all indices where value is 0
            zero_indices = series[series == 0].index.tolist()
            
            if len(zero_indices) == 0:
                return -1
            
            # Return the last zero index
            return series.index.get_loc(zero_indices[-1])

        data_dict = {"data": {}}

        for ticker in ticker_list:
            ticker_data = df[df['ticker'] == ticker].copy()
            
            # Sort by date to ensure proper chronological order
            ticker_data = ticker_data.sort_values('date').reset_index(drop=True)
            
            # Find the last zero total_market_value index
            last_zero_idx = find_last_zero_market_value_index(ticker_data['total_market_value'])
            
            # Skip ticker if no zeros found
            if last_zero_idx < 0:
                continue
            
            # Filter data from last zero onwards
            filtered_data = ticker_data.iloc[last_zero_idx:]
            
            # Create combined values array: [unix_timestamp, close_price_used, total_market_value]
            values = [
                [row['unix_timestamp'], row['close_price_used'], row['total_market_value']] 
                for _, row in filtered_data.iterrows()
            ]
            
            data_dict["data"][ticker] = {
                "daily": {
                    "types": ["unix", "close_price", "market_value"],
                    "values": values
                }
            }

        # Use your existing fix_dict_nan function
        data_dict = fix_dict_nan(data_dict, 'robinhood_daily')


        ### Load the second dataset for totals
        df2 = execute_jinja_query(db_connector, "api/quick_bites/robinhood_totals_daily.sql.j2", query_parameters={}, return_df=True)
        df2['unix_timestamp'] = pd.to_datetime(df2['date']).astype(int) // 10**6  # Convert to milliseconds

        # Sort by date to ensure proper chronological order
        df2 = df2.sort_values('date').reset_index(drop=True)

        # Find the last zero total_market_value_sum index
        last_zero_idx = find_last_zero_market_value_index(df2['total_market_value_sum'])

        # Filter data from last zero onwards (skip if no zeros found)
        if last_zero_idx >= 0:
            filtered_df2 = df2.iloc[last_zero_idx:]
        else:
            filtered_df2 = df2  # Keep all data if no zeros found

        # Create data_dict2 for the totals data
        data_dict2 = {
            "data": {
                "total_market_value_sum": {
                    "daily": {
                        "types": ["value", "unix"],
                        "values": [[value, timestamp] for value, timestamp in zip(
                            filtered_df2['total_market_value_sum'].tolist(),
                            filtered_df2['unix_timestamp'].tolist()
                        )]
                    }
                }
            }
        }

        # fix NaN values in the data_dict2
        data_dict2 = fix_dict_nan(data_dict2, 'robinhood_totals')


        ### Load the stock table
        df3 = execute_jinja_query(db_connector, "api/quick_bites/robinhood_stock_table.sql.j2", query_parameters={}, return_df=True)

        # Create data_dict3 for the stock table
        data_dict3 = {
            "data": {
                "stocks": {
                    "columns": ["contract_address", "ticker", "name", "usd_outstanding", "stocks_tokenized", "usd_stock_price"],
                    "types": ["string", "string", "string", "number", "number", "number"],
                    "rows": df3[['contract_address', 'ticker', 'name', 'usd_outstanding', 'stocks_tokenized', 'usd_stock_price']].values.tolist()
                }
            }
        }

        # fix NaN values in the data_dict3
        data_dict3 = fix_dict_nan(data_dict3, 'robinhood_stocks')

        # Upload to S3
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood_daily', data_dict, cf_distribution_id)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood_totals', data_dict2, cf_distribution_id)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/robinhood_stock_table', data_dict3, cf_distribution_id)


    pull_data_from_dune()
    pull_data_from_yfinance()
    create_json_file()
run_dag()