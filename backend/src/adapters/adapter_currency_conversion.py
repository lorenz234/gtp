"""
Currency Conversion Adapter for Non-USD Stablecoins

This adapter fetches live exchange rates from multiple APIs and provides
currency conversion functionality for non-USD stablecoins.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Tuple, List

from src.adapters.abstract_adapters import AbstractAdapter
from src.currency_config import (
    get_supported_currencies,
    calculate_forex_rate_from_coingecko,
    calculate_forex_rate_from_alternative,
    get_primary_exchange_rate_url,
    get_backup_exchange_rate_url,
    RATE_FETCH_CONFIG
)
from src.misc.helper_functions import api_get_call, print_init, print_load, print_extract

class AdapterCurrencyConversion(AbstractAdapter):
    """
    Adapter for fetching and managing exchange rates for currency conversion.
    
    This adapter supports:
    1. Real-time exchange rate fetching from multiple APIs
    2. Rate validation and error handling
    3. Database storage for historical tracking
    4. Caching to minimize API calls
    5. Fallback mechanisms for API failures
    
    adapter_params can include:
        currencies: list - Specific currencies to fetch (default: all supported)
    """
    
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("Currency Conversion", adapter_params, db_connector)
        
        # Configuration
        self.currencies = adapter_params.get('currencies', get_supported_currencies())
        self.force_refresh = adapter_params.get('force_refresh', False)
        
        # Remove 'usd' from currencies list since USD/USD = 1.0
        if 'usd' in self.currencies:
            self.currencies.remove('usd')
            
        print_init(self.name, self.adapter_params)
        
    def extract(self, load_params: dict) -> pd.DataFrame:
        """
        Extract exchange rates from APIs.
        
        load_params can include:
            load_type: str - 'current_rates' or 'historical_rates'
            date: str - Specific date for historical rates (YYYY-MM-DD)
            currencies: list - Override currencies to fetch
        """
        load_type = load_params.get('load_type', 'current_rates')
        target_date = load_params.get('date')
        currencies = load_params.get('currencies', self.currencies)
        
        if load_type == 'current_rates':
            df = self._fetch_current_rates(currencies)
        elif load_type == 'historical_rates':
            df = self._fetch_historical_rates(currencies, target_date)
        else:
            raise ValueError(f"Unsupported load_type: {load_type}")
            
        print_extract(self.name, load_params, df.shape)
        return df
    
    def load(self, df: pd.DataFrame):
        """
        Load exchange rates to database.
        """
        if df.empty:
            print("No exchange rate data to load")
            return
            
        # Store in existing fact_kpis table
        table_name = 'fact_kpis'
        
        try:
            # Upsert to database
            self.db_connector.upsert_table(table_name, df)
            print_load(self.name, df.shape[0], table_name)
            
        except Exception as e:
            print(f"Failed to load exchange rates to database: {e}")
            raise
    
    def _fetch_current_rates(self, currencies: List[str]) -> pd.DataFrame:
        """
        Fetch current exchange rates for specified currencies.
        """
        rates_data = []
        current_time = datetime.now()
        
        for currency in currencies:
            if currency == 'usd':
                continue  # Skip USD
            
            # Fetch fresh rate
            rate, source = self._fetch_single_rate(currency, 'usd')
            
            if rate is not None:
                rates_data.append({
                    'date': current_time.date(),
                    'metric_key': 'price_usd',
                    'origin_key': f'fiat_{currency}',
                    'value': rate
                })
                
                print(f"Fetched {currency.upper()}/USD rate: {rate:.6f} from {source}")
            else:
                print(f"Failed to fetch rate for {currency.upper()}")
        
        df = pd.DataFrame(rates_data)
        
        # Set proper index for fact_kpis table structure
        if not df.empty:
            df = df.set_index(['metric_key', 'origin_key', 'date'])
            
        return df
    
    def _fetch_single_rate(self, base_currency: str, target_currency: str) -> Tuple[Optional[float], str]:
        """
        Fetch exchange rate for a single currency pair with fallback.
        
        Returns:
            Tuple[Optional[float], str]: (rate, source) or (None, '') if failed
        """
        # Try primary API (CoinGecko)
        rate = self._fetch_from_coingecko(base_currency, target_currency)
        if rate is not None:
            return rate, 'coingecko'
            
        # Fallback to alternative API
        print(f"CoinGecko failed for {base_currency}/{target_currency}, trying backup API")
        rate = self._fetch_from_alternative_api(base_currency, target_currency)
        if rate is not None:
            return rate, 'exchangerate_api'
            
        print(f"All APIs failed for {base_currency}/{target_currency}")
        return None, ''
    
    def _fetch_from_coingecko(self, base_currency: str, target_currency: str) -> Optional[float]:
        """
        Fetch exchange rate from CoinGecko API.
        """
        try:
            url = get_primary_exchange_rate_url()
            
            response_data = api_get_call(
                url, 
                sleeper=1, 
                retries=RATE_FETCH_CONFIG['max_retries']
            )
            
            if not response_data:
                return None
                
            rate = calculate_forex_rate_from_coingecko(base_currency, target_currency, response_data)
            return rate
                
        except Exception as e:
            print(f"CoinGecko API error for {base_currency}/{target_currency}: {e}")
            return None
    
    def _fetch_from_alternative_api(self, base_currency: str, target_currency: str) -> Optional[float]:
        """
        Fetch exchange rate from alternative API.
        """
        try:
            url = get_backup_exchange_rate_url()
            
            response_data = api_get_call(
                url,
                sleeper=1,
                retries=RATE_FETCH_CONFIG['max_retries']
            )
            
            if not response_data:
                return None
                
            rate = calculate_forex_rate_from_alternative(base_currency, target_currency, response_data)
            return rate
                
        except Exception as e:
            print(f"Alternative API error for {base_currency}/{target_currency}: {e}")
            return None
    
    def get_exchange_rate(self, base_currency: str, target_currency: str = 'usd') -> Optional[float]:
        """
        Get exchange rate for currency pair (public method for other adapters).
        
        Args:
            base_currency (str): Base currency code (e.g., 'eur', 'brl')
            target_currency (str): Target currency code (default: 'usd')
            
        Returns:
            Optional[float]: Exchange rate or None if not available
        """
        if base_currency == target_currency:
            return 1.0
        
        # 1) Try database for today's rate first
        origin_key = f"fiat_{base_currency.lower()}"
        try:
            df = self.db_connector.get_data_from_table(
                'fact_kpis',
                filters={
                    'metric_key': 'price_usd',
                    'origin_key': origin_key,
                },
                days=1
            )
            if not df.empty:
                # Ensure date column exists and is today
                if 'date' in df.columns:
                    df_today = df[df['date'].dt.date == datetime.now().date()]
                    if not df_today.empty and 'value' in df_today.columns:
                        latest_val = df_today.sort_values('date', ascending=False)['value'].iloc[0]
                        return float(latest_val)
        except Exception as e:
            print(f"Warning: DB lookup for {origin_key} failed: {e}")
        
        # 2) If missing/stale, fetch from APIs and upsert into DB
        rate, source = self._fetch_single_rate(base_currency, target_currency)
        if rate is not None:
            try:
                df_upsert = pd.DataFrame([
                    {
                        'date': datetime.now().date(),
                        'metric_key': 'price_usd',
                        'origin_key': origin_key,
                        'value': rate
                    }
                ]).set_index(['metric_key', 'origin_key', 'date'])
                self.db_connector.upsert_table('fact_kpis', df_upsert)
            except Exception as e:
                print(f"Warning: failed to upsert {origin_key} rate to DB: {e}")
        return rate
    
    # Cache-related helpers removed: DB is now source of truth for rates
