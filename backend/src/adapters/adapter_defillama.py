import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, upsert_to_kpis
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterDefillama(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("DefiLlama", adapter_params, db_connector)
        self.base_url = 'https://api.llama.fi/'

        main_conf = get_main_config()
        self.projects = [chain for chain in main_conf if chain.aliases_defillama is not None]

        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
    """
    def extract(self, load_params:dict):
        origin_keys = load_params['origin_keys']

        check_projects_to_load(self.projects, origin_keys)
        projects_to_load = return_projects_to_load(self.projects, origin_keys)

        ## Load data
        df = self.extract_app_fees(projects_to_load=projects_to_load)
        
        print_extract(self.name, load_params,df.shape)
        return df

    def load(self, df:pd.DataFrame):
        upserted, tbl_name = upsert_to_kpis(df, self.db_connector)
        print_load(self.name, upserted, tbl_name)  


    ## ----------------- Helper functions --------------------

    ## TODO: 
    ## - Remove network fees (i.e. Arbitrum REV)
    def extract_app_fees(self, projects_to_load):
        df_main = pd.DataFrame()
        for chain in projects_to_load:
            alias = chain.aliases_defillama
            origin_key = chain.origin_key
            
            print(f'..processing {origin_key} with alias {alias}')

            url = f'{self.base_url}overview/fees/{alias}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false&dataType=dailyFees'
            print(f'..fetching: {url}')
            response_json = api_get_call(url)

            # Build a list of dicts, then create the DataFrame at once (avoiding deprecated .append)
            if len(response_json['totalDataChartBreakdown']) == 0:
                print(f'No data found for {origin_key}. Skipping...')
                continue
                
            rows = []
            for item in response_json['totalDataChartBreakdown']:
                for key, value in item[1:][0].items():
                    rows.append({
                        'unix': item[0],
                        'protocol': key,
                        'value': value
                    })
            df = pd.DataFrame(rows)
            df['origin_key'] = origin_key
            df['date'] = pd.to_datetime(df['unix'], unit='s')
            df = df.sort_values(by='date')
            
            # For Ethereum, we need to correct Tether and Circle fees
            if origin_key == 'ethereum':
                # load the stables data, merge it with the current df, calculate eth_dominance, calculate the adjusted fees
                df_stables = self.load_stables_df()
                df = pd.merge(df, df_stables[['date', 'protocol', 'eth_dominance']], on=['date', 'protocol'], how='left')
                df.loc[df['protocol'].isin(['Tether', 'Circle']), 'value'] *= df['eth_dominance']
            
            # For Arbitrum, we need to remove Timeboost (since it already included in the chain REV/revenue)
            if origin_key == 'arbitrum':
                df = df[~df['protocol'].isin(['Timeboost'])]
            
            df_main = pd.concat([df_main, df], ignore_index=True)
            time.sleep(1)  # Respect API rate limits

            
        df_main = df_main.drop(columns=['unix'])
        
        ## aggregate by origin_key, date (this will drop the protocol column -> might be interesting for later though on app level)
        df_main = df_main.groupby(['origin_key', 'date']).agg({'value': 'sum'}).reset_index()
        df_main['metric_key'] = 'app_fees_usd'

        df_main.set_index(['date', 'origin_key', 'metric_key'], inplace=True)
        return df_main
    
    def load_stables_df(self):
        stables = {
            'Tether': 1,
            'Circle': 2,
        }

        df_stables = pd.DataFrame()
        for name, id in stables.items():
            url = f"https://stablecoins.llama.fi/stablecoin/{id}"
            print(f'..fetching: {url}')
            response_json = api_get_call(url)

            # Build a list of dicts, then create the DataFrame at once (avoiding deprecated .append)
            rows = []
            for item in response_json['tokens']:
                rows.append({
                    'unix': int(item['date']),
                    'circ_total': item['circulating']['peggedUSD']
                })
            df = pd.DataFrame(rows)

            time.sleep(1)  # To avoid hitting the API too hard

            url = f"https://stablecoins.llama.fi/stablecoincharts/Ethereum?stablecoin={id}"
            print(f'..fetching: {url}')
            response_json = api_get_call(url)
            rows = []
            for item in response_json:
                rows.append({
                    'unix': int(item['date']),
                    'circ_ethereum': item['totalCirculating']['peggedUSD']
                })
            df2 = pd.DataFrame(rows)

            df = pd.merge(df, df2, on='unix', how='outer')
            df['protocol'] = name
            df_stables = pd.concat([df_stables, df], ignore_index=True)

            time.sleep(1)  # To avoid hitting the API too hard

        df_stables['date'] = pd.to_datetime(df_stables['unix'], unit='s')
        df_stables = df_stables.sort_values(by='date')
        df_stables['eth_dominance'] = df_stables['circ_ethereum'] / df_stables['circ_total']

        return df_stables