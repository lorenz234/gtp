from src.main_config import get_main_config, get_multi_config
from src.da_config import get_da_config
from src.misc.helper_functions import upload_json_to_cf_s3, upload_parquet_to_cf_s3, db_addresses_to_checksummed_addresses, string_addresses_to_checksummed_addresses, fix_dict_nan, empty_cloudfront_cache, remove_file_from_s3, get_files_df_from_s3
from src.misc.glo_prep import Glo
from src.db_connector import DbConnector


class JsonGen():
    def __init__(self, s3_bucket, cf_distribution_id, db_connector:DbConnector, api_version='v1'):
        ## Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        
    
    ## helper functions
    def generate_daily_list(self, df, metric_id, origin_key, start_date = None, metric_type='default', rolling_avg = False):
        tmp_metrics_dict = self.get_metric_dict(metric_type)            

        mks = tmp_metrics_dict[metric_id]['metric_keys']
        df_tmp = df.loc[(df.origin_key==origin_key) & (df.metric_key.isin(mks)), ["unix", "value", "metric_key", "date"]]

        ## if start_date is not None, filter df_tmp date to only values after start date
        if start_date is not None:
            df_tmp = df_tmp.loc[(df_tmp.date >= start_date), ["unix", "value", "metric_key", "date"]]
        
        max_date = df_tmp['date'].max()
        max_date = pd.to_datetime(max_date).replace(tzinfo=None)
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.date()

        ## if max_date_fill is True, fill missing rows until yesterday with 0
        if tmp_metrics_dict[metric_id]['max_date_fill']:
            #check if max_date is yesterday
            if max_date.date() != yesterday:
                print(f"max_date in df for {mks} is {max_date}. Will fill missing rows until {yesterday} with None.")

                date_range = pd.date_range(start=max_date + timedelta(days=1), end=yesterday, freq='D')

                for mkey in mks:
                    new_data = {'date': date_range, 'value': [0] * len(date_range), 'metric_key': mkey}
                    new_df = pd.DataFrame(new_data)
                    new_df['unix'] = new_df['date'].apply(lambda x: x.timestamp() * 1000)

                    df_tmp = pd.concat([df_tmp, new_df], ignore_index=True)

        ## trim leading zeros
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        df_tmp = df_tmp.groupby('metric_key').apply(self.trim_leading_zeros).reset_index(drop=True)

        df_tmp.drop(columns=['date'], inplace=True)
        df_tmp = df_tmp.pivot(index='unix', columns='metric_key', values='value').reset_index()
        df_tmp.sort_values(by=['unix'], inplace=True, ascending=True)
        
        df_tmp = self.df_rename(df_tmp, metric_id, tmp_metrics_dict, col_name_removal=True)

        mk_list = df_tmp.values.tolist() ## creates a list of lists

        if len(tmp_metrics_dict[metric_id]['units']) == 1:
            mk_list_int = [[int(i[0]),i[1]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        elif len(tmp_metrics_dict[metric_id]['units']) == 2:
            mk_list_int = [[int(i[0]),i[1], i[2]] for i in mk_list] ## convert the first element of each list to int (unix timestamp)
        else:
            raise NotImplementedError("Only 1 or 2 units are supported")
        
        if rolling_avg == True:
            mk_list_int = self.create_7d_rolling_avg(mk_list_int)
        
        return mk_list_int, df_tmp.columns.to_list()



    def create_metric_per_chain_json(self, origin_key, metric_id):
        """
        Create a JSON file for a metric and a chain.
        """
        
        print(f'..processing: Metric details for {origin_key} - {metric_id}')

        mk_list = self.generate_daily_list(df, metric, origin_key)
        mk_list_int = mk_list[0]
        mk_list_columns = mk_list[1]

        mk_list_monthly = self.generate_monthly_list(df, metric, origin_key)
        mk_list_int_monthly = mk_list_monthly[0]
        mk_list_columns_monthly = mk_list_monthly[1]

        chains_dict[origin_key] = {
            'chain_name': chain.name,
            'changes': self.create_changes_dict(df, metric, origin_key),
            'changes_monthly': self.create_changes_dict_monthly(df, metric, origin_key),
            'daily': {
                'types' : mk_list_columns,
                'data' : mk_list_int
            },
            'last_30d': self.value_last_30d(df, metric, origin_key),
            'monthly': {
                'types' : mk_list_columns_monthly,
                'data' : mk_list_int_monthly
            }
        }

        ## check if metric should be averagd and add 7d rolling avg field
        if self.metrics[metric]['avg'] == True:
            mk_list_int_7d = self.create_7d_rolling_avg(mk_list_int)
            chains_dict[origin_key]['daily_7d_rolling'] = {
                'types' : mk_list_columns,
                'data' : mk_list_int_7d
            }

    details_dict = {
        'data': {
            'metric_id': metric,
            'metric_name': self.metrics[metric]['name'],
            'source': self.db_connector.get_metric_sources(metric, []),
            'avg': self.metrics[metric]['avg'],
            'monthly_agg': 'distinct' if self.metrics[metric]['monthly_agg'] in ['maa'] else self.metrics[metric]['monthly_agg'],
            'chains': chains_dict
        }
    }

    details_dict['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    details_dict = fix_dict_nan(details_dict, f'metrics/{metric}')

    if self.s3_bucket == None:
        self.save_to_json(details_dict, f'metrics/{metric}')
    else:
        upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/metrics/{metric}', details_dict, self.cf_distribution_id, invalidate=False)
    print(f'DONE -- Metric details export for {metric}')
