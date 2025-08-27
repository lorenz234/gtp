import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple

# Assume these imports exist and are correct
from src.db_connector import DbConnector
from src.config import gtp_units, gtp_metrics

# --- Set up a proper logger ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class JsonGen():
    def __init__(self, s3_bucket: str, cf_distribution_id: str, db_connector: DbConnector, api_version: str = 'v1'):
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.units = gtp_units
        self.metrics = gtp_metrics

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
    def _prepare_metric_key_data(df: pd.DataFrame, max_date_fill: bool = True) -> pd.DataFrame:
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
            
            # Forward-fill identifier columns
            for col in ['metric_key', 'origin_key']:
                if col in df.columns:
                    df[col] = df[col].fillna(method='ffill')
        return df

    def _get_prepared_daily_df(self, origin_key: str, metric_id: str, level: str, start_date: Optional[str]) -> Tuple[pd.DataFrame, Dict]:
        """
        Fetches, prepares, and pivots the daily data for a given metric,
        returning a single DataFrame with a DatetimeIndex and the metric config.
        """
        metric_dict = self.metrics[level][metric_id]
        days = (pd.to_datetime('today') - pd.to_datetime(start_date or '2020-01-01')).days

        df_list = [
            self._prepare_metric_key_data(self._get_raw_data_metric(origin_key, mk, days), metric_dict['max_date_fill'])
            for mk in metric_dict['metric_keys']
        ]

        # Filter out empty dataframes before concatenating
        valid_dfs = [df for df in df_list if not df.empty]
        if not valid_dfs:
            return pd.DataFrame(), metric_dict

        df_full = pd.concat(valid_dfs, ignore_index=True)
        df_pivot = df_full.pivot(index='date', columns='metric_key', values='value').sort_index()
        return df_pivot, metric_dict
        
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

        # Convert date to unix timestamp in milliseconds
        df_formatted['unix'] = df_formatted['unix'].apply(lambda x: int(x.timestamp() * 1000))
        
        # Ensure standard column order if usd/eth are present for consistency
        column_order = ['unix']
        if 'usd' in df_formatted.columns: column_order.append('usd')
        if 'eth' in df_formatted.columns: column_order.append('eth')
        if 'value' in df_formatted.columns: column_order.append('value')
        
        final_df = df_formatted[column_order]
        
        values_list = final_df.values.tolist()
        columns_list = final_df.columns.to_list()
        
        ## make sure unix value no decimals
        for row in values_list:
            row[0] = int(row[0])
        
        return values_list, columns_list


    def create_metric_per_chain_json(self, origin_key: str, metric_id: str, level: str = 'chain_level', start_date: Optional[str] = None) -> Optional[Dict]:
        """
        Creates a dictionary for a metric/chain with daily, weekly, and monthly aggregations.
        """
        logging.info(f"Generating aggregations for {origin_key} - {metric_id}")

        daily_df, metric_dict = self._get_prepared_daily_df(origin_key, metric_id, level, start_date)

        if daily_df.empty:
            logging.warning(f"No data found for {origin_key} - {metric_id}. Skipping.")
            return None

        ## TODO: add AA aggregation method
        # Determine aggregation method
        agg_config = metric_dict.get('monthly_agg')
        if agg_config == 'avg':
            agg_method = 'mean'
        elif agg_config == 'sum':
            agg_method = 'sum'
        elif agg_config == 'maa':
            ## implement monthly active addresses aggregation (and weekly)
            pass
        else:
            raise ValueError(f"Unknown aggregation method: {agg_config}")

        # Perform aggregations
        ## TODO: check if the dates are same as currently
        weekly_df = daily_df.resample('W-MON').agg(agg_method)
        monthly_df = daily_df.resample('MS').agg(agg_method)

        # Rolling average calculation
        if metric_dict.get('avg', False):
            rolling_avg_df = daily_df.rolling(window=7).mean()
            # Remove leading NaN values
            rolling_avg_df = rolling_avg_df[rolling_avg_df.notna().any(axis=1)]
            # Convert to list format
            daily_7d_list, _ = self._format_df_for_json(rolling_avg_df, metric_dict['units'])
        else:
            daily_7d_list = None
            
        # Format all dataframes into the required list structure
        daily_list, daily_cols = self._format_df_for_json(daily_df, metric_dict['units'])
        weekly_list, weekly_cols = self._format_df_for_json(weekly_df, metric_dict['units'])
        monthly_list, monthly_cols = self._format_df_for_json(monthly_df, metric_dict['units'])

        # Build the final dictionary
        timeseries_data = {
            'daily': {'types': daily_cols, 'data': daily_list},
            'weekly': {'types': weekly_cols, 'data': weekly_list},
            'monthly': {'types': monthly_cols, 'data': monthly_list},
        }
        
        changes_data = {
            'daily': None,
            'weekly': None,
            'monthly': None,
        }

        if daily_7d_list is not None:
            timeseries_data['daily_7d_rolling'] = {'types': daily_cols, 'data': daily_7d_list}

        output = {
            'details': {
                'metric_id': metric_id,
                'metric_name': metric_dict['name'],
                'timeseries': timeseries_data,
                'changes': changes_data
            },
        }
        
        logging.info(f"Successfully generated data for {origin_key} - {metric_id}")
        return output