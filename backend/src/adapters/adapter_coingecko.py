import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_df_kpis, upsert_to_kpis, get_missing_days_kpis, send_discord_message
from src.misc.helper_functions import print_init, print_load, print_extract

##ToDos: 
# Add logs (query execution, execution fails, etc)

class AdapterCoingecko(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Coingecko", adapter_params, db_connector)
        self.base_url = 'https://pro-api.coingecko.com/api/v3/coins/'

        main_conf = get_main_config()
        self.projects = [chain for chain in main_conf if chain.aliases_coingecko is not None]

        self.api_key = adapter_params.get('api_key', None)
        self.headers = {
            'x-cg-pro-api-key': self.api_key
        }

        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        metric_keys:list - list of metrics that should be loaded. E.g. prices, total_volumes, market_cap
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
        days:str - days of historical data that should be loaded, starting from today. Can be set to 'max'
        vs_currencies:list - list of currencies that we load financials for. E.g. eth, usd
        load_type:str - can be project or imx_tokens 
    """
    def extract(self, load_params:dict):

        self.load_type = load_params['load_type']
        self.granularity = load_params.get('granularity', 'daily')

        if self.load_type == 'project':
            ## Set variables
            metric_keys = load_params['metric_keys']
            origin_keys = load_params['origin_keys']
            days = load_params['days']
            vs_currencies = load_params['vs_currencies']
            
            ## Prepare projects to load (can be a subseth of all projects)
            check_projects_to_load(self.projects, origin_keys)
            projects_to_load = return_projects_to_load(self.projects, origin_keys)

            ## Load data
            df = self.extract_projects(
                projects_to_load=projects_to_load
                ,vs_currencies=vs_currencies
                ,days=days
                ,base_url=self.base_url
                ,metric_keys=metric_keys
                ,granularity=self.granularity
                )
        elif self.load_type == 'imx_tokens':
            df = self.extract_imx_tokens()
        elif self.load_type == 'direct':
            metric_keys = load_params['metric_keys']
            coingecko_ids = load_params['coingecko_ids']
            days = load_params['days']
            vs_currencies = load_params['vs_currencies']

            df = self.extract_projects(
                projects_to_load=coingecko_ids
                ,vs_currencies=vs_currencies
                ,days=days
                ,base_url=self.base_url
                ,metric_keys=metric_keys
                ,granularity=self.granularity
                ,load_type='direct'
                )
        else:
            raise ValueError(f"load_type {self.load_type} not supported")      

        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        if self.load_type == 'project' or self.load_type == 'direct':
            if self.granularity == 'daily':
                upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
                print_load(self.name, upserted, tbl_name)
            else:
                self.db_connector.upsert_table('fact_kpis_granular', df)
                print_load(self.name, df.shape[0], 'fact_kpis_granular')
        elif self.load_type == 'imx_tokens':
            self.db_connector.upsert_table('prices_daily', df)
            print_load(self.name, df.shape[0], 'prices_daily')
        else:
            raise ValueError(f"load_type {self.load_type} not supported")        


    ## ----------------- Helper functions --------------------

    def extract_projects(self, projects_to_load, vs_currencies, days, base_url, metric_keys, granularity='daily', load_type='project'):
        if granularity == 'hourly':
            if days == 'auto':
                days = '30'
                print(f"... hourly agg: auto set to 30 days")
            elif int(days) > 89:
                days = '89'
                print(f"... hourly agg: days set to 89 days (more isn't possible)")
            elif int(days) <= 2:
                days = '3'
                print(f"... hourly agg: days set to 3 day (less is automatically in smaller granularity)")
            interval = ''
        else:
            interval = '&interval=daily'
        
        dfMain = get_df_kpis()
        for token in projects_to_load:
            if load_type == 'project':
                origin_key = token.origin_key
                naming = token.aliases_coingecko
            elif load_type == 'direct':
                origin_key = naming = token
            else:
                raise ValueError(f"load_type {load_type} not supported")

            if days == 'auto':
                day_val = get_missing_days_kpis(self.db_connector, metric_key= 'price_usd', origin_key=origin_key)
            else:
                day_val = int(days)

            if day_val >= 730:
                day_val = 730
                print(f"... days set to 730 days (more isn't possible)")

            for currency in vs_currencies:
                url = f"{base_url}{naming}/market_chart?vs_currency={currency}&days={day_val}{interval}"

                response_json = api_get_call(url, sleeper=10, retries=10, header=self.headers)
                if response_json:
                    dfAllFi = pd.json_normalize(response_json)
                    for fi in metric_keys:
                        match fi:
                            case 'price':
                                series = dfAllFi['prices'].explode()
                            case 'volume':
                                series = dfAllFi['total_volumes'].explode()
                            case 'market_cap':
                                series = dfAllFi['market_caps'].explode()
                        df = pd.DataFrame(columns = ['date', 'value'], data = series.to_list())
                        df['date'] = pd.to_datetime(df['date'],unit='ms')                    
                        df['metric_key'] = f"{fi}_{currency}"
                        df['origin_key'] = origin_key

                        df.value.fillna(0, inplace=True)
                        dfMain = pd.concat([dfMain,df])
                        print(f"...{self.name} {origin_key} done for {currency} and {fi} with granularity {granularity}. Shape: {df.shape}")
                else:
                    print(f"...{self.name} {origin_key} failed for {currency} with url {url}")
                    send_discord_message(f"Failed to load {origin_key} for {currency} with url {url}")

                time.sleep(1) #only 10-50 calls allowed per minute with free tier

        ## Date prep
        if granularity == 'hourly':
            dfMain['timestamp'] = dfMain['date'].dt.floor('h')
            dfMain['granularity'] = 'hourly'
            dfMain.drop(columns=['date'], inplace=True)
            ## remove duplicates and set index
            dfMain.drop_duplicates(subset=['metric_key', 'origin_key', 'timestamp'], inplace=True)
            dfMain.set_index(['metric_key', 'origin_key', 'timestamp', 'granularity'], inplace=True)
        else:
            dfMain['date'] = pd.to_datetime(dfMain['date']).dt.date
            # today = datetime.today().date()
            # dfMain = dfMain[dfMain['date'] != today]

            ## remove duplicates and set index
            dfMain.drop_duplicates(subset=['metric_key', 'origin_key', 'date'], inplace=True)
            dfMain.set_index(['metric_key', 'origin_key', 'date'], inplace=True)
        return dfMain
    
    def get_imx_tokens(self, db_connector):
        exec_string = f'''
            SELECT 
                    "name", 
                    symbol, 
                    decimals, 
                    case when "name" = 'Ethereum' then null else concat('\\x', encode(token_address, 'hex'))end as token_address ,
                    coingecko_id 
            FROM public.imx_tokens
            where coingecko_id is not null
        '''
        df = pd.read_sql(exec_string, db_connector.engine.connect())
        return df

    def extract_imx_tokens(self):
        df_tokens = self.get_imx_tokens(self.db_connector)

        dfMain = pd.DataFrame()
        ## iterate over all tokens
        for index, row in df_tokens.iterrows():
            print(f"... loading price for {row['symbol']} with coingecko_id {row['coingecko_id']}")
            url = f"https://api.coingecko.com/api/v3/coins/{row['coingecko_id']}/market_chart?vs_currency=usd&days=2000&interval=daily"
            response = api_get_call(url)
            df = pd.DataFrame(response['prices'], columns=['timestamp', 'price'])
            df['token_symbol'] = row['symbol']
            df['token_address'] = row['token_address']

            dfMain = pd.concat([dfMain, df])
            time.sleep(7)

        ## unix timestamp to date
        dfMain['date'] = pd.to_datetime(dfMain['timestamp'], unit='ms')
        dfMain['date'] = dfMain['date'].dt.date
        dfMain.drop(columns=['timestamp'], inplace=True)

        ##change column price to price_usd
        dfMain.rename(columns={'price': 'price_usd'}, inplace=True)

        ## drop duplicates in date and token_symbol
        dfMain.drop_duplicates(subset=['date', 'token_symbol'], inplace=True)

        dfMain.set_index(['date', 'token_symbol'], inplace=True)
        return dfMain