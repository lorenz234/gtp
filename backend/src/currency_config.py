# Currency Configuration for Non-USD Stablecoins
# This file manages fiat currency mappings and exchange rate sources for converting
# non-USD stablecoin supplies to USD values for consistent reporting.

from typing import Dict, List, Optional

# Fiat currency configuration for exchange rate fetching
FIAT_CURRENCY_CONFIG = {
    "eur": {
        "name": "Euro",
        "symbol": "EUR",
        "coingecko_forex_id": "eur",  # CoinGecko exchange rates API key
        "vs_currency": "usd",
        "decimal_places": 6,  # High precision for exchange rates
        "description": "European Union Euro"
    },
    "brl": {
        "name": "Brazilian Real", 
        "symbol": "BRL",
        "coingecko_forex_id": "brl",  # CoinGecko exchange rates API key
        "vs_currency": "usd", 
        "decimal_places": 6,  # High precision for exchange rates
        "description": "Brazilian Real"
    },
    "usd": {
        "name": "US Dollar",
        "symbol": "USD", 
        "coingecko_forex_id": "usd",
        "vs_currency": "usd",
        "decimal_places": 2,
        "description": "United States Dollar"
    },
    "chf": {
        "name": "Swiss Franc",
        "symbol": "CHF",
        "coingecko_forex_id": "chf",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "Swiss Franc"
    },
    "sgd": {
        "name": "Singapore Dollar",
        "symbol": "SGD",
        "coingecko_forex_id": "sgd",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "Singapore Dollar"
    },
    "rub": {
        "name": "Russian Ruble",
        "symbol": "RUB",
        "coingecko_forex_id": "rub",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "Russian Ruble"
    },
    "php": {
        "name": "Philippine Peso",
        "symbol": "PHP",
        "coingecko_forex_id": "php",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "Philippine Peso"
    },
    "aud": {
        "name": "Australian Dollar",
        "symbol": "AUD",
        "coingecko_forex_id": "aud",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "Australian Dollar"
    },
    "gbp": {
        "name": "British Pound Sterling",
        "symbol": "GBP",
        "coingecko_forex_id": "gbp",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "British Pound Sterling"
    },
    "xof": {
        "name": "West African CFA Franc",
        "symbol": "XOF",
        "coingecko_forex_id": "xof",
        "vs_currency": "usd",
        "decimal_places": 6,
        "description": "West African CFA Franc"
    }
}

# Exchange rate API endpoints
EXCHANGE_RATE_APIS = {
    "coingecko": {
        "url": "https://api.coingecko.com/api/v3/exchange_rates",
        "method": "GET",
        "timeout": 10,
        "description": "CoinGecko exchange rates (BTC-relative)"
    },
    "exchangerate_api": {
        "url": "https://api.exchangerate-api.com/v4/latest/USD", 
        "method": "GET",
        "timeout": 10,
        "description": "ExchangeRate-API (USD-based rates)"
    },
    # Historical: Frankfurter API (ECB-backed)
    "frankfurter": {
        "url_template": "https://api.frankfurter.app/{date}?from=USD",
        "method": "GET",
        "timeout": 10,
        "description": "Frankfurter historical USD-based rates"
    }
}

def get_supported_currencies() -> List[str]:
    """
    Get list of supported fiat currencies.
    
    Returns:
        List[str]: List of supported currency codes
    """
    return list(FIAT_CURRENCY_CONFIG.keys())


def get_coingecko_exchange_rate_url() -> str:
    """
    Get primary exchange rate API URL.
    
    Returns:
        str: CoinGecko exchange rates API URL
    """
    return EXCHANGE_RATE_APIS["coingecko"]["url"]

def get_historical_exchange_rate_url(date_str: str) -> str:
    """
    Get historical exchange rate API URL for a specific date (YYYY-MM-DD).
    Uses Frankfurter with USD base.
    """
    return EXCHANGE_RATE_APIS["frankfurter"]["url_template"].format(date=date_str)

def calculate_forex_rate_from_coingecko(base_currency: str, target_currency: str, rates_data: dict) -> Optional[float]:
    """
    Calculate forex rate from CoinGecko exchange rates data.
    
    CoinGecko rates are relative to Bitcoin (BTC = 1.0), so to get EUR/USD:
    EUR/USD = USD_rate / EUR_rate
    
    Args:
        base_currency (str): Base currency code (e.g., 'eur')  
        target_currency (str): Target currency code (e.g., 'usd')
        rates_data (dict): CoinGecko rates response data
        
    Returns:
        Optional[float]: Exchange rate or None if calculation fails
    """
    try:
        if base_currency == target_currency:
            return 1.0
            
        rates = rates_data.get('rates', {})
        
        base_rate = rates.get(base_currency.lower(), {}).get('value')
        target_rate = rates.get(target_currency.lower(), {}).get('value')
        
        if base_rate is None or target_rate is None:
            return None
            
        # Calculate rate: base/target = target_rate / base_rate
        return target_rate / base_rate
        
    except (KeyError, ZeroDivisionError, TypeError):
        return None

# Error handling configuration
RATE_FETCH_CONFIG = {
    "max_retries": 3,
    "retry_delay": 1.0,  # seconds
    "timeout": 10.0,     # seconds
}


