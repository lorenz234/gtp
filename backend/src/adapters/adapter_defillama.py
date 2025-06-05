import time
import pandas as pd

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import api_get_call, return_projects_to_load, check_projects_to_load, get_df_kpis, upsert_to_kpis, get_missing_days_kpis, send_discord_message
from src.misc.helper_functions import print_init, print_load, print_extract

##ToDos: 
# Add logs (query execution, execution fails, etc)

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

    def extract_app_fees(self, projects_to_load):
        df_main = pd.DataFrame()
        for chain in projects_to_load:
            alias = chain.aliases_defillama
            origin_key = chain.origin_key

            url = f'{self.base_url}overview/fees/{alias}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=false&dataType=dailyFees'
            print(f'Fetching: {url}')
            response_json = api_get_call(url)

            # Build a list of dicts, then create the DataFrame at once (avoiding deprecated .append)
            rows = []
            for item in response_json['totalDataChartBreakdown']:
                rows.append({
                    'unix': item[0],
                    'value': sum(item[1].values())
                })
            df = pd.DataFrame(rows)
            df['origin_key'] = origin_key
            df_main = pd.concat([df_main, df], ignore_index=True)
            time.sleep(1)  # Respect API rate limits

        df_main['date'] = pd.to_datetime(df_main['unix'], unit='s')
        df_main = df_main.sort_values(by='date')
        df_main = df_main.drop(columns=['unix'])
        df_main['metric_key'] = 'app_fees_usd'

        df_main.set_index(['date', 'origin_key', 'metric_key'], inplace=True)
        return df_main