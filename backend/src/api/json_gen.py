from src.main_config import get_main_config, get_multi_config
from src.da_config import get_da_config
from src.misc.helper_functions import upload_json_to_cf_s3, upload_parquet_to_cf_s3, db_addresses_to_checksummed_addresses, string_addresses_to_checksummed_addresses, fix_dict_nan, empty_cloudfront_cache, remove_file_from_s3, get_files_df_from_s3
import pandas as pd
from src.db_connector import DbConnector
from src.config import gtp_units, gtp_metrics

class JsonGen():
    def __init__(self, s3_bucket, cf_distribution_id, db_connector:DbConnector, api_version='v1'):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.units = gtp_units
        self.metrics = gtp_metrics
        
    ## HELPER functions
    
    def df_rename(self, df, units, col_name_removal=False):
        if col_name_removal:
            df.columns.name = None

        if 'usd' in units or 'eth' in units:
            for col in df.columns.to_list():
                if col == 'unix':
                    continue
                elif col.endswith('_eth'):
                    df.rename(columns={col: 'eth'}, inplace=True)
                else:
                    df.rename(columns={col: 'usd'}, inplace=True)

            if 'unix' in df.columns.to_list():
                df = df[['unix', 'usd', 'eth']]
            else:
                df = df[['usd', 'eth']]
        else:
            for col in df.columns.to_list():
                if col == 'unix':
                    continue
                else:
                    df.rename(columns={col: 'value'}, inplace=True)
        return df
    
    def create_7d_rolling_avg(self, list_of_lists):
        avg_list = []
        if len(list_of_lists[0]) == 2: ## all non USD metrics e.g. txcount
            for i in range(len(list_of_lists)):
                if i < 7:
                    avg_list.append([list_of_lists[i][0], list_of_lists[i][1]])
                else:
                    avg = (list_of_lists[i][1] + list_of_lists[i-1][1] + list_of_lists[i-2][1] + list_of_lists[i-3][1] + list_of_lists[i-4][1] + list_of_lists[i-5][1] + list_of_lists[i-6][1]) / 7
                    ## round to 2 decimals
                    avg = round(avg, 2)
                    avg_list.append([list_of_lists[i][0], avg])
        else: ## all USD metrics e.g. fees that have USD and ETH values
            for i in range(len(list_of_lists)):
                if i < 7:
                    avg_list.append([list_of_lists[i][0], list_of_lists[i][1], list_of_lists[i][2]] )
                else:
                    avg_1 = (list_of_lists[i][1] + list_of_lists[i-1][1] + list_of_lists[i-2][1] + list_of_lists[i-3][1] + list_of_lists[i-4][1] + list_of_lists[i-5][1] + list_of_lists[i-6][1]) / 7
                    avg_2 = (list_of_lists[i][2] + list_of_lists[i-1][2] + list_of_lists[i-2][2] + list_of_lists[i-3][2] + list_of_lists[i-4][2] + list_of_lists[i-5][2] + list_of_lists[i-6][2]) / 7
    
                    avg_list.append([list_of_lists[i][0], avg_1, avg_2])
        
        return avg_list

    # Function to get fact kpis from the database
    def get_raw_data_metric(self, origin_key:str, metric_key:str, days:int=None):
        """
        Get fact kpis from the database.
        """
        query_parameters = {
            'origin_key': origin_key,
            'metric_key': metric_key,
            'days': days
        }
        df = self.db_connector.execute_jinja_query("api/select_fact_kpis.sql.j2", query_parameters, return_df=True)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['metric_key'] = metric_key
        return df
    
    # This function prepares the metric data by trimming leading zeros and filling missing dates (it takes a DataFrame with 1 metric_key as input)
    def prepare_metric_key_data(df:pd.DataFrame, max_date_fill:bool=True):
        # trim leading zeros from the dataframe
        df = df[df["value"].cumsum() > 0]

        if max_date_fill:
            # if max_date_fill is True, fill missing rows until yesterday with 0
            df_all_dates = pd.DataFrame({'date': pd.date_range(start=df['date'].min(), end=pd.to_datetime('yesterday'), freq='D')})
            df = pd.merge(df_all_dates, df, on='date', how='left')
            df['value'] = df['value'].fillna(0)
        return df

    def process_metric(self, origin_key:str, metric_id:str, aggregation:str='daily', level:str='chain_level', start_date=None):
        print(f'..processing: Metric details for {metric_id} at {level}')
        metric_dict = self.metrics[level][metric_id]
        
        if start_date is not None:
            days = (pd.to_datetime('today') - pd.to_datetime(start_date)).days
        else:
            days = (pd.to_datetime('today') - pd.to_datetime('2020-01-01')).days
        
        df = pd.DataFrame()
        for mk in metric_dict['metric_keys']:
            df_tmp = self.get_fact_kpi_df(origin_key, mk, days)
            df_tmp = self.prepare_metric_data(df_tmp, max_date_fill=metric_dict['max_date_fill'])
            df = pd.concat([df, df_tmp], ignore_index=True)

        df['unix'] = df['date'].apply(lambda x: x.timestamp() * 1000)
        df.sort_values(by=['unix'], inplace=True, ascending=True)
        df = df.drop(columns=['date'])
        
        df = df.pivot(index='unix', columns='metric_key', values='value').reset_index()
        df.sort_values(by=['unix'], inplace=True, ascending=True)
        
        df = self.df_rename(df, metric_dict['units'], col_name_removal=True)
        
        mk_list = df.values.tolist() ## creates a list of lists
        
        if len(metric_dict['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(metric_dict['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        return mk_list_int, df_tmp.columns.to_list()



    def create_metric_per_chain_json(self, origin_key, metric_id):
        """
        Create a JSON file for a metric and a chain.
        """
        chains_dict = {}
        metric_dict = self.metrics['chain_level'][metric_id]
        print(f'..generating json: Metric details for {origin_key} - {metric_id}')

        ## Daily
        mk_list = self.process_metric(origin_key, metric_id, aggregation='daily', level='chain_level')
        mk_list_int = mk_list[0]
        mk_list_columns = mk_list[1]

        ## Weekly
        mk_list_weekly = self.process_metric(origin_key, metric_id, aggregation='weekly', level='chain_level', rolling_avg=True)
        mk_list_int_weekly = mk_list_weekly[0]
        mk_list_columns_weekly = mk_list_weekly[1]

        
        ## Monthly
        mk_list_monthly = self.process_metric(origin_key, metric_id, aggregation='monthly', level='chain_level', rolling_avg=True)
        mk_list_int_monthly = mk_list_monthly[0]
        mk_list_columns_monthly = mk_list_monthly[1]

        chains_dict[origin_key] = {
            'daily': {
                'types' : mk_list_columns,
                'data' : mk_list_int
            },
            'weekly': {
                'types' : mk_list_columns_weekly,
                'data' : mk_list_int_weekly
            },
            'monthly': {
                'types' : mk_list_columns_monthly,
                'data' : mk_list_int_monthly
            },
            ## TODO: will add later though
            #'last_30d': self.value_last_30d(df, metric, origin_key),
            #'changes': self.create_changes_dict(df, metric, origin_key),
            #'changes_monthly': self.create_changes_dict_monthly(df, metric, origin_key),
        }

        ## check if metric should be averagd and add 7d rolling avg field
        if metric_dict['avg'] == True:
            mk_list_int_7d = self.create_7d_rolling_avg(mk_list_int)
            chains_dict[origin_key]['daily_7d_rolling'] = {
                'types' : mk_list_columns,
                'data' : mk_list_int_7d
            }

        details_dict = {
            'data': {
                'metric_id': metric_id,
                'metric_name': metric_dict['name'],
                ##'source': self.db_connector.get_metric_sources(metric, []),
                ##'monthly_agg': 'distinct' if self.metrics[metric]['monthly_agg'] in ['maa'] else self.metrics[metric]['monthly_agg'],
            }
        }
