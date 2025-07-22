import pandas as pd
from datetime import datetime

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

from yfinance import Tickers

"""
Example load_params for Robinhood stock data:

load_params = {
    'tickers': ['*'], # list of stock tickers you want to load data for or '*' to load all stocks from Robinhood
    'endpoints': ['Close'], # what to get from the api (can be: '*', 'Capital Gains', 'Low', 'High', 'Close', 'Open', 'Dividends', 'Stock Splits', 'Volume')
    'days': 50, # how many days of data to get
    'table': 'robinhood_daily', # sql table to upsert data into
    'prepare_df': 'prepare_df_robinhood_daily' # function to prepare the dataframe before loading into the database
}
"""


# Yahoo Finance API adapter for getting stock data such as daily prices, volumes, etc.
class AdapterYFinance(AbstractAdapter):
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("yfinance", adapter_params, db_connector)
        print_init(self.name, self.adapter_params)

    def extract(self, load_params:dict):
        """
        Extracts data from the Yahoo Finance API based on the provided load parameters.
        """
        self.load_params = load_params

        from yfinance import Tickers
        
        tickers = load_params.get('tickers', ['*'])
        endpoints = load_params.get('endpoints', ['*'])
        days = load_params.get('days', 5)

        if tickers == ['*']:
            tickers = self.get_robinhood_stock_list()['ticker'].tolist()

        # Use Tickers for batch processing
        tickers_obj = Tickers(' '.join(tickers))

        # Get historical data for all tickers at once
        hist_data = tickers_obj.history(period=f"{days}d")

        # only keep what is specified in load_params['endpoints']
        if endpoints != ['*']:
            hist_data = hist_data[endpoints]

        print_extract(self.name, self.load_params, hist_data.shape)

        return hist_data

    def load(self, df:pd.DataFrame):
        """
        Loads data into the database based on the provided table name in load parameters.
        """
        table = self.load_params.get('table', None)
        prepare_df = self.load_params.get('prepare_df', None)

        if prepare_df:
            prepare_func = getattr(self, prepare_df, None)
            if callable(prepare_func):
                df = prepare_func(df)
            else:
                raise ValueError(f"Prepare function {prepare_df} not found in adapter {self.name}!")

        if table is not None:
            try:
                upserted = self.db_connector.upsert_table(table, df)
                print_load(self.name, upserted, table)
            except Exception as e:
                print(f"Error loading {table}: {e}")
        else:
            print("No table specified in load_params. Data not loaded!")


    ## ----------------- Helper functions --------------------
    
    def prepare_df_robinhood_daily(self, df:pd.DataFrame):
        """
        Prepares the DataFrame for Robinhood daily data.
        """
        # melt the df into metric_key, date, ticker, value format
        df = df.reset_index().melt(id_vars=['Date'], var_name=['metric_key', 'ticker'], value_name='value')
        df['date'] = df['Date'].dt.date  # Extract date from Date column
        df = df.drop(columns=['Date'])
        stocks = self.get_robinhood_stock_list()
        # left join stocks onto df
        df = df.merge(stocks, on='ticker', how='left')
        # only keep relevant columns
        df = df[['date', 'contract_address', 'metric_key', 'value']]
        # set index
        df = df.set_index(['date', 'contract_address', 'metric_key'])
        return df
    
    def get_robinhood_stock_list(self):
        """
        Gets all robinhood stocks in a df from the database table robinhood_stock_list. Removed demo stocks.
        """
        df = self.db_connector.get_table('robinhood_stock_list')
        df = df[~df['ticker'].str.contains('demo token')]
        return df