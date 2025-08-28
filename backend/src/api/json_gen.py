import os
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.db_connector import DbConnector
from src.config import gtp_units, gtp_metrics
from src.main_config import get_main_config
from src.misc.helper_functions import fix_dict_nan, upload_json_to_cf_s3, empty_cloudfront_cache

# --- Set up a proper logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants for aggregation methods and metric IDs to improve readability ---
AGG_METHOD_SUM = 'sum'
AGG_METHOD_MEAN = 'mean'
AGG_METHOD_LAST = 'last'

AGG_CONFIG_SUM = 'sum'
AGG_CONFIG_AVG = 'avg'
AGG_CONFIG_MAA = 'maa'

METRIC_DAA = 'daa'
METRIC_MAA = 'maa'
METRIC_WAA = 'waa'
METRIC_AA_7D = 'aa_last7d'
METRIC_AA_30D = 'aa_last30d'


class JsonGen():
    def __init__(self, s3_bucket: str, cf_distribution_id: str, db_connector: DbConnector, api_version: str = 'v1'):
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.units = gtp_units
        self.metrics = gtp_metrics
        self.main_config = get_main_config(api_version=self.api_version)
        
    def _save_to_json(self, data, path):
        #create directory if not exists
        os.makedirs(os.path.dirname(f'output/{path}.json'), exist_ok=True)
        ## save to file
        with open(f'output/{path}.json', 'w') as fp:
            json.dump(data, fp, ignore_nan=True)

    def _get_raw_data_metric(self, origin_key: str, metric_key: str, days: Optional[int] = None) -> pd.DataFrame:
        """Get fact kpis from the database for a specific metric key."""
        query_parameters = {'origin_key': origin_key, 'metric_key': metric_key, 'days': days}
        df = self.db_connector.execute_jinja("api/select_fact_kpis.sql.j2", query_parameters, load_into_df=True)
        
        if df.empty:
            return pd.DataFrame()
            
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('UTC')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df['metric_key'] = metric_key
        return df

    @staticmethod
    def _prepare_metric_key_data(df: pd.DataFrame, metric_key: str, max_date_fill: bool = True) -> pd.DataFrame:
        """Prepares metric data by trimming leading zeros and filling missing dates."""
        if df.empty:
            return df
            
        # Trim leading zeros for a cleaner chart start
        df = df.loc[df["value"].cumsum() > 0].copy()
        if df.empty:
            return df

        if max_date_fill:
            # Fill missing rows until yesterday with 0 for continuous time-series
            yesterday = pd.to_datetime('today', utc=True).normalize() - pd.Timedelta(days=1)
            start_date = min(df['date'].min(), yesterday)
            
            all_dates = pd.date_range(start=start_date, end=yesterday, freq='D', tz='UTC')
            df = df.set_index('date').reindex(all_dates, fill_value=0).reset_index().rename(columns={'index': 'date'})
            
            #make sure metric_key is set correctly
            df['metric_key'] = metric_key
        return df

    def _get_prepared_timeseries_df(self, origin_key: str, metric_keys: List[str], start_date: Optional[str], max_date_fill: bool) -> pd.DataFrame:
        """
        Fetches, prepares, and pivots the timeseries data for given metric keys,
        returning a single DataFrame.
        """
        days = (pd.to_datetime('today') - pd.to_datetime(start_date or '2020-01-01')).days

        df_list = [
            self._prepare_metric_key_data(self._get_raw_data_metric(origin_key, mk, days), mk, max_date_fill)
            for mk in metric_keys
        ]

        # Filter out empty dataframes before concatenating
        valid_dfs = [df for df in df_list if not df.empty]
        if not valid_dfs:
            return pd.DataFrame()

        df_full = pd.concat(valid_dfs, ignore_index=True)
        df_pivot = df_full.pivot(index='date', columns='metric_key', values='value').sort_index()
        return df_pivot
        
    def _format_df_for_json(self, df: pd.DataFrame, units: List[str]) -> Tuple[List[List], List[str]]:
        """
        Takes a DataFrame, renames columns based on units, converts index to unix timestamp,
        and formats it into a list of lists and a list of column types for the JSON output.
        """
        if df.empty:
            return [], []
        
        df_formatted = df.reset_index()
        df_formatted.rename(columns={'date': 'unix'}, inplace=True)
        
        # Rename value columns based on units config
        rename_map = {}
        value_cols = [col for col in df_formatted.columns if col != 'unix']
        if 'usd' in units or 'eth' in units:
            for col in value_cols:
                rename_map[col] = 'eth' if col.endswith('_eth') else 'usd'
        elif value_cols:
            rename_map[value_cols[0]] = 'value'
        
        df_formatted = df_formatted.rename(columns=rename_map)

        # Convert datetime to unix timestamp in milliseconds (as integer) efficiently.
        df_formatted['unix'] = (df_formatted['unix'].astype('int64') // 1_000_000)
        
        # Ensure standard column order for consistency in the final JSON
        base_order = ['unix']
        present_cols = [col for col in ['usd', 'eth', 'value'] if col in df_formatted.columns]
        column_order = base_order + present_cols
        
        final_df = df_formatted[column_order]
        
        values_list = final_df.values.tolist()
        columns_list = final_df.columns.to_list()
        
        return values_list, columns_list

    def _create_changes_dict(self, df: pd.DataFrame, metric_id: str, level: str, periods: Dict[str, int], agg_window: int, agg_method: str) -> Dict:
        """
        Calculates percentage change based on rolling aggregate windows over daily data.
        
        Args:
            df (pd.DataFrame): Input DataFrame with daily data, DatetimeIndex, and value columns.
            metric_id (str): The ID of the metric for config lookup.
            level (str): The level of the data (e.g., 'chain_level').
            periods (Dict[str, int]): Maps change key (e.g., '30d') to lookback period in days (e.g., 30).
            agg_window (int): The size of the aggregation window in days (e.g., 30 for monthly).
            agg_method (str): The aggregation method ('sum', 'mean', or 'last' for pre-aggregated values like MAA).
        """
        if df.empty:
            return {**{key: [] for key in periods}, 'types': []}

        metric_dict = self.metrics[level][metric_id]
        units = metric_dict.get('units', [])
        
        # Determine the final column types ('usd', 'eth', 'value') and their order
        reverse_rename_map, final_types_ordered = self._get_column_type_mapping(df.columns.tolist(), units)

        changes_dict = {
            'types': final_types_ordered,
            **{key: [] for key in periods.keys()}
        }

        for final_type in final_types_ordered:
            original_col = reverse_rename_map.get(final_type)
            series = df[original_col]

            for key, period in periods.items():
                change_val = None
                # Ensure there is enough data for both current and previous windows
                if len(series) >= period + agg_window:
                    if agg_method == AGG_METHOD_LAST: # For point-to-point or pre-aggregated metrics
                        cur_val = series.iloc[-1]
                        prev_val = series.iloc[-(1 + period)]
                    else: # For rolling sum or mean
                        cur_val = series.iloc[-agg_window:].agg(agg_method)
                        prev_val = series.iloc[-(agg_window + period) : -period].agg(agg_method)

                    # Calculate percentage change with safety checks
                    if pd.notna(cur_val) and pd.notna(prev_val) and prev_val > 0 and cur_val >= 0:
                        change = (cur_val - prev_val) / prev_val
                        change_val = round(change, 4)
                        # Cap extreme growth for frontend display purposes to prevent visual distortion.
                        if change_val > 100: 
                            change_val = 99.99
                
                changes_dict[key].append(change_val)
        return changes_dict

    def _get_column_type_mapping(self, df_columns: List[str], units: List[str]) -> Tuple[Dict[str, str], List[str]]:
        """Helper to map dataframe columns to final JSON types ('usd', 'eth', 'value')."""
        reverse_rename_map = {}
        if 'usd' in units or 'eth' in units:
            for col in df_columns:
                reverse_rename_map['eth' if col.endswith('_eth') else 'usd'] = col
        elif df_columns:
            reverse_rename_map['value'] = df_columns[0]
            
        final_types_ordered = []
        if 'usd' in reverse_rename_map: final_types_ordered.append('usd')
        if 'eth' in reverse_rename_map: final_types_ordered.append('eth')
        if 'value' in reverse_rename_map: final_types_ordered.append('value')
        return reverse_rename_map, final_types_ordered
    
    def _create_summary_values_dict(self, df: pd.DataFrame, metric_id: str, level: str, agg_window: int, agg_method: str) -> Dict:
        """
        Calculates a single aggregated value for the most recent period (e.g., last 30 days).
        """
        metric_dict = self.metrics[level][metric_id]
        units = metric_dict.get('units', [])
        reverse_rename_map, final_types_ordered = self._get_column_type_mapping(df.columns.tolist(), units)

        if df.empty or len(df) < agg_window:
            return {'types': final_types_ordered, 'data': [None] * len(final_types_ordered)}

        if agg_method == AGG_METHOD_LAST:
            aggregated_series = df.iloc[-1]
        else:
            aggregated_series = df.iloc[-agg_window:].agg(agg_method)
        
        data_list = [aggregated_series.get(reverse_rename_map.get(t)) for t in final_types_ordered]
        
        return {'types': final_types_ordered, 'data': data_list}

    def create_metric_per_chain_dict(self, origin_key: str, metric_id: str, level: str = 'chain_level', start_date: Optional[str] = None) -> Optional[Dict]:
        """Creates a dictionary for a metric/chain with daily, weekly, and monthly aggregations."""
        metric_dict = self.metrics[level][metric_id]
        daily_df = self._get_prepared_timeseries_df(origin_key, metric_dict['metric_keys'], start_date, metric_dict.get('max_date_fill', False))

        if daily_df.empty:
            logging.warning(f"No data found for {origin_key} - {metric_id}. Skipping.")
            return None

        agg_config = metric_dict.get('monthly_agg')
        
        if agg_config == AGG_CONFIG_SUM:
            agg_method = AGG_METHOD_SUM
        elif agg_config == AGG_CONFIG_AVG:
            agg_method = AGG_METHOD_MEAN
        elif agg_config == AGG_CONFIG_MAA:
            agg_method = AGG_METHOD_LAST # 'maa' implies pre-aggregated values, so we take the 'last' value
        else:
            raise ValueError(f"Invalid monthly_agg config '{agg_config}' for metric {metric_id}")
            
        # --- AGGREGATIONS ---
        if agg_config == AGG_CONFIG_MAA:
            weekly_df = self._get_prepared_timeseries_df(origin_key, [METRIC_WAA], start_date, metric_dict.get('max_date_fill', False))
            monthly_df = self._get_prepared_timeseries_df(origin_key, [METRIC_MAA], start_date, metric_dict.get('max_date_fill', False))
        else:
            weekly_df = daily_df.resample('W-MON').agg(agg_method)
            monthly_df = daily_df.resample('MS').agg(agg_method)

        daily_7d_list = None
        if metric_dict.get('avg', False):
            rolling_avg_df = daily_df.rolling(window=7).mean().dropna(how='all')
            daily_7d_list, _ = self._format_df_for_json(rolling_avg_df, metric_dict['units'])
        
        # --- FORMATTING TIMESERIES FOR JSON ---
        daily_list, daily_cols = self._format_df_for_json(daily_df, metric_dict['units'])
        weekly_list, weekly_cols = self._format_df_for_json(weekly_df, metric_dict['units'])
        monthly_list, monthly_cols = self._format_df_for_json(monthly_df, metric_dict['units'])
        
        timeseries_data = {
            'daily': {'types': daily_cols, 'data': daily_list},
            'weekly': {'types': weekly_cols, 'data': weekly_list},
            'monthly': {'types': monthly_cols, 'data': monthly_list},
        }
        if daily_7d_list is not None:
            timeseries_data['daily_7d_rolling'] = {'types': daily_cols, 'data': daily_7d_list}

        # --- CHANGES CALCULATION ---
        daily_periods = {'1d': 1, '7d': 7, '30d': 30, '90d': 90, '180d': 180, '365d': 365}
        weekly_periods = {'7d': 7, '30d': 30, '90d': 90, '180d': 180, '365d': 365}
        monthly_periods = {'30d': 30, '90d': 90, '180d': 180, '365d': 365}

        daily_changes = self._create_changes_dict(daily_df, metric_id, level, daily_periods, agg_window=1, agg_method=AGG_METHOD_LAST)
        
        if metric_id == METRIC_DAA:
            df_aa_weekly = self._get_prepared_timeseries_df(origin_key, [METRIC_AA_7D], start_date, metric_dict.get('max_date_fill', False))
            df_aa_monthly = self._get_prepared_timeseries_df(origin_key, [METRIC_AA_30D], start_date, metric_dict.get('max_date_fill', False))
            
            weekly_changes = self._create_changes_dict(df_aa_weekly, metric_id, level, weekly_periods, agg_window=7, agg_method=AGG_METHOD_LAST)
            monthly_changes = self._create_changes_dict(df_aa_monthly, metric_id, level, monthly_periods, agg_window=30, agg_method=AGG_METHOD_LAST)
        else:
            weekly_changes = self._create_changes_dict(daily_df, metric_id, level, weekly_periods, agg_window=7, agg_method=agg_method)
            monthly_changes = self._create_changes_dict(daily_df, metric_id, level, monthly_periods, agg_window=30, agg_method=agg_method)
        
        changes_data = {
            'daily': daily_changes,
            'weekly': weekly_changes,
            'monthly': monthly_changes,
        }

        # --- SUMMARY VALUES CALCULATION ---
        if metric_id == METRIC_DAA:
            summary_data = {
                'last_1d': self._create_summary_values_dict(daily_df, metric_id, level, agg_window=1, agg_method=AGG_METHOD_LAST),
                'last_7d': self._create_summary_values_dict(df_aa_weekly, metric_id, level, agg_window=7, agg_method=AGG_METHOD_LAST),
                'last_30d': self._create_summary_values_dict(df_aa_monthly, metric_id, level, agg_window=30, agg_method=AGG_METHOD_LAST),
            }
        else:
            summary_data = {
                'last_1d': self._create_summary_values_dict(daily_df, metric_id, level, agg_window=1, agg_method=agg_method),
                'last_7d': self._create_summary_values_dict(daily_df, metric_id, level, agg_window=7, agg_method=agg_method),
                'last_30d': self._create_summary_values_dict(daily_df, metric_id, level, agg_window=30, agg_method=agg_method),
            }

        # --- FINAL OUTPUT DICT ---
        output = {
            'details': {
                'metric_id': metric_id,
                'metric_name': metric_dict['name'],
                'timeseries': timeseries_data,
                'changes': changes_data,
                'summary': summary_data
            },
        }
        
        output['last_updated_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        output = fix_dict_nan(output, f'test/metrics/{origin_key}/{metric_id}')
        return output
    
    def _process_and_save_metric(self, origin_key: str, metric_id: str, level: str, start_date: str):
        """
        Worker function to process and save/upload a single metric-chain combination.
        This is designed to be called by the ThreadPoolExecutor.
        """
        logging.info(f"Processing: {origin_key} - {metric_id}")
        
        metric_dict = self.create_metric_per_chain_dict(origin_key, metric_id, level, start_date)

        if metric_dict:
            s3_path = f'{self.api_version}/test/metrics/{origin_key}/{metric_id}'
            if self.s3_bucket is None:
                # Assuming local saving for testing still uses a similar path structure
                self._save_to_json(metric_dict, s3_path)
            else:
                upload_json_to_cf_s3(self.s3_bucket, s3_path, metric_dict, self.cf_distribution_id, invalidate=False)
            logging.info(f"SUCCESS: Exported {origin_key} - {metric_id}")
        else:
            logging.warning(f"NO DATA: Skipped export for {origin_key} - {metric_id}")

    def create_metric_jsons(self, metric_ids: Optional[List[str]] = None, origin_keys: Optional[List[str]] = None, level: str = 'chain_level', start_date='2020-01-01', max_workers: int = 10):
        """
        Generates and uploads all metric JSONs in parallel using a thread pool.
        """
        tasks = []
        if metric_ids:
            logging.info(f"Filtering tasks for specific metric IDs: {metric_ids}")
        else:
            logging.info(f"Generating task list for ALL metric IDs: {list(self.metrics[level].keys())}")
            
        if origin_keys:
            logging.info(f"Filtering tasks for specific origin keys: {origin_keys}") 
        else:
            logging.info(f"Generating task list for ALL origin keys: {list(chain.origin_key for chain in self.main_config)}")

        # 1. Generate the full list of tasks to be executed
        for metric_id in self.metrics[level].keys():
            if metric_ids and metric_id not in metric_ids:
                continue
            if not self.metrics[level][metric_id].get('fundamental', False):
                continue

            for chain in self.main_config:
                origin_key = chain.origin_key
                if origin_keys and origin_key not in origin_keys:
                    continue
                if not chain.api_in_main:
                    continue
                if metric_id in chain.api_exclude_metrics:
                    continue
                
                tasks.append((origin_key, metric_id))
        
        logging.info(f"Found {len(tasks)} metric/chain combinations to process.")
        logging.info(f"Starting parallel processing with max_workers={max_workers}...")

        # 2. Execute tasks in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks to the executor
            future_to_task = {executor.submit(self._process_and_save_metric, origin_key, metric_id, level, start_date): (origin_key, metric_id) for origin_key, metric_id in tasks}

            # Process results as they complete
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    future.result()  # We call result() to raise any exceptions that occurred
                except Exception as exc:
                    logging.error(f'Task {task} generated an exception: {exc}')

        logging.info("All metric JSONs have been processed.")
        
        # 3. Invalidate the cache after all files have been uploaded
        if self.s3_bucket and self.cf_distribution_id:
            logging.info("Invalidating CloudFront cache for all metrics...")
            # Note: The path might need adjustment if you removed '/test/'
            invalidation_path = f'/{self.api_version}/metrics/*'
            empty_cloudfront_cache(self.cf_distribution_id, invalidation_path)
            logging.info(f"CloudFront invalidation submitted for path: {invalidation_path}")
        else:
            logging.info("Skipping CloudFront invalidation (S3 bucket or Distribution ID not set).")