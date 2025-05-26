import asyncio
import aiohttp
import os
import json
import logging
import argparse
import time
from datetime import datetime
import urllib.parse
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv

# ===============================================
# CONFIGURATION
# ===============================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ConcurrentContractAnalyzer')

# Load environment variables
load_dotenv()

class Config:
    """Central configuration class for the application."""
    
    # Airtable configuration
    AIRTABLE_TOKEN = os.getenv('AIRTABLE_API_KEY')
    AIRTABLE_BASE_URL = 'https://api.airtable.com/v0/appZWDvjvDmVnOici' # TODO: Make this configurable or ensure it's the correct base
    TABLE_NAME = 'tblcXnFAf0IEvAQA6' # TODO: Make this configurable or ensure it's the correct table
    # TODO: Add a field in Airtable like "v2_processing_status" (e.g., "pending", "processing", "complete", "error")
    # TODO: Add a view for records where "v2_processing_status" = "pending"
    TARGET_VIEW_ID_V2 = 'viwF2Xc24CGNO7u5C' # TODO: Replace with actual view ID for V2 processing

    CHAIN_TABLE_ID = 'tblK3YcdB8jaFtMgS'
    
    # GitHub configuration
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    
    # API Headers
    AIRTABLE_HEADERS = {
        'Authorization': f'Bearer {AIRTABLE_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    GITHUB_HEADERS = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Blockscout API Keys
    BLOCKSCOUT_API_KEYS = {
        1: os.getenv("BLOCKSCOUT_API_KEY_ETH"),
        10: os.getenv("BLOCKSCOUT_API_KEY_OPTIMISM"),
        324: os.getenv("BLOCKSCOUT_API_KEY_ZKSYNC"),
        8453: os.getenv("BLOCKSCOUT_API_KEY_BASE"),
        42161: os.getenv("BLOCKSCOUT_API_KEY_ARBITRUM"),
        534352: os.getenv("BLOCKSCOUT_API_KEY_SCROLL"),
        1301: os.getenv("BLOCKSCOUT_API_KEY_UNICHAIN"),
        42170: os.getenv("BLOCKSCOUT_API_KEY_NOVA")
    }
    
    EXCLUDED_REPOS = [
        "HelayLiu/utils_download",
        "KeystoneHQ/Smart-Contract-Metadata-Registry",
        "tangtj/",
        "drumonbase/txdata",
        "tahaghazi/research"
    ]
    
    SUPPORTED_CHAINS = {
        "base": 8453,
        "ethereum": 1,
        "optimism": 10,
        "arbitrum": 42161,
        "zksync": 324,
        "zksync era": 324,
        "mode": 34443,
        "scroll": 534352,
        "mantle": 5000,
        "taiko": 167000,
        "linea": 59144,
        "polygon zkevm": 1101,
        "unichain": 1301,
        "arbitrum nova": 42170,
        "zora": 7777777,
    }
    
    CHAIN_NAME_ALIASES = {
        "op mainnet": "optimism",
        "optimism mainnet": "optimism",
        "ethereum mainnet": "ethereum",
        "eth mainnet": "ethereum",
        "mainnet": "ethereum",
        "arbitrum one": "arbitrum",
        "arbitrum mainnet": "arbitrum",
        "base mainnet": "base",
        "zksync era": "zksync",
        "zksync": "zksync",
        "mode network": "mode",
        "polygon zk": "polygon zkevm",
        "taiko alethia": "taiko",
        "arbitrum nova": "arbitrum nova",
        "linea mainnet": "linea"
    }
    
    CHAIN_EXPLORER_CONFIG = {
        1: {"name": "Ethereum", "urls": ["https://eth.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        10: {"name": "Optimism", "urls": ["https://optimism.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        324: {"name": "zkSync Era", "urls": ["https://zksync.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        8453: {"name": "Base", "urls": ["https://base.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        42161: {"name": "Arbitrum One", "urls": ["https://arbitrum.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        34443: {"name": "Mode", "urls": ["https://explorer-mode-mainnet-0.t.conduit.xyz/api/v2/"], "auth_type": "header", "requires_auth": False},
        534352: {"name": "Scroll", "urls": ["https://scroll.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        5000: {"name": "Mantle", "urls": ["https://explorer.mantle.xyz/api/v2/"], "auth_type": "header", "requires_auth": False},
        167000: {"name": "Taiko", "urls": ["https://blockscoutapi.mainnet.taiko.xyz/api/v2/"], "auth_type": "header", "requires_auth": False},
        59144: {"name": "Linea", "urls": ["https://api-explorer.linea.build/api/v2/"], "auth_type": "header", "requires_auth": False, "skip_logs": True, "timeout": 30, "max_retries": 1},
        1101: {"name": "Polygon zkEVM", "urls": ["https://zkevm.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": False},
        1301: {"name": "Unichain", "urls": ["https://unichain.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        42170: {"name": "Arbitrum Nova", "urls": ["https://arbitrum-nova.blockscout.com/api/v2/"], "auth_type": "header", "requires_auth": True},
        7777777: {"name": "Zora", "urls": ["https://explorer.zora.energy/api/v2/"], "auth_type": "header", "requires_auth": False}
    }
    
    @classmethod
    def validate_config(cls) -> bool:
        if not all([cls.AIRTABLE_TOKEN, cls.GITHUB_TOKEN]):
            logger.error("Missing AIRTABLE_TOKEN or GITHUB_TOKEN environment variables!")
            return False
        missing_keys = []
        for chain_id, config in cls.CHAIN_EXPLORER_CONFIG.items():
            if config.get("requires_auth", False):
                if chain_id not in cls.BLOCKSCOUT_API_KEYS or cls.BLOCKSCOUT_API_KEYS[chain_id] is None:
                    missing_keys.append(f"{config['name']} (ID: {chain_id})")
        if missing_keys:
            logger.warning(f"Missing API keys for: {', '.join(missing_keys)}")
        return True

is_config_valid = Config.validate_config()
if is_config_valid:
    logger.info("Configuration loaded successfully.")

# Global chain mapping (can be populated from Airtable as in the original script)
ORIGIN_KEY_TO_CHAIN_MAP: Dict[str, str] = {} 

# ===============================================
# ASYNC API CLASSES (Placeholders)
# ===============================================

class AirtableAPI_Async:
    def __init__(self, session: aiohttp.ClientSession, headers: Dict, base_url: str, table_name: str, chain_table_id: str):
        self.session = session
        self.headers = headers
        self.base_url = base_url
        self.table_name = table_name
        self.chain_table_id = chain_table_id

    async def _request(self, method: str, url: str, **kwargs) -> Dict:
        # Basic rate limiting / retry can be added here if needed
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.session.request(method, url, headers=self.headers, timeout=30, **kwargs) as response:
                    if response.status == 429: # Rate limited
                        retry_after = int(response.headers.get("Retry-After", "5"))
                        logger.warning(f"Airtable rate limit hit. Retrying after {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientResponseError as e:
                logger.error(f"Airtable API Error ({e.status}) for {url}: {e.message}")
                if attempt == max_retries - 1:
                    raise
            except aiohttp.ClientError as e: # Broader network errors
                logger.error(f"Airtable request error for {url}: {e}")
                if attempt == max_retries - 1:
                    raise
            except asyncio.TimeoutError:
                logger.error(f"Airtable request timed out for {url}")
                if attempt == max_retries - 1:
                    raise
            
            await asyncio.sleep(2 ** attempt) # Exponential backoff for other retries
        raise Exception(f"Failed to fetch {url} after {max_retries} retries")


    async def fetch_chain_mappings(self) -> Dict[str, str]:
        url = f"{self.base_url}/{self.chain_table_id}"
        chain_mappings = {}
        try:
            logger.info("Fetching chain mappings from Airtable...")
            data = await self._request("GET", url)
            records = data.get('records', [])
            for record in records:
                record_id = record.get('id')
                fields = record.get('fields', {})
                chain_name = None
                for field_name in ['name', 'chain', 'chainName', 'chain_name']:
                    if field_name in fields:
                        chain_name = fields[field_name]
                        break
                if record_id and chain_name:
                    chain_mappings[record_id] = chain_name.lower()
            logger.info(f"Fetched {len(chain_mappings)} chain mappings.")
        except Exception as e:
            logger.error(f"Error fetching chain mappings: {str(e)}")
        return chain_mappings

    async def fetch_records_to_process(self, view_id: str, batch_size: int, offset: Optional[str] = None, max_records: Optional[int] = None) -> Tuple[List[Dict], Optional[str]]:
        """Fetch records from the specified view, supporting pagination."""
        # TODO: Add a filter_formula if needed, e.g., to fetch records with a specific "v2_processing_status"
        # filter_formula = "{v2_processing_status}='pending'" 
        # For now, fetches all from view, make sure view is filtered correctly in Airtable
        
        params = {
            "view": view_id,
            "pageSize": batch_size,
            # "fields[]": ["address", "origin_key"] # Specify only needed fields for fetching
        }
        if offset:
            params["offset"] = offset
        
        # Using urllib.parse.urlencode for params to handle potential special characters
        query_string = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        url = f"{self.base_url}/{self.table_name}?{query_string}"
        
        all_records_batch: List[Dict] = []
        
        try:
            logger.info(f"Fetching records from Airtable view {view_id} (Offset: {offset or 'start'})")
            data = await self._request("GET", url)
            records = data.get('records', [])
            all_records_batch.extend(records)
            logger.info(f"Fetched {len(records)} records in this batch.")
            
            new_offset = data.get('offset')
            return all_records_batch, new_offset
        except Exception as e:
            logger.error(f"Error fetching records to process: {str(e)}")
            return [], None # Return empty list and no offset on error


    async def update_record(self, record_id: str, fields_to_update: Dict) -> bool:
        """Update an Airtable record with the given fields."""
        url = f"{self.base_url}/{self.table_name}/{record_id}"
        request_body = {
            "fields": fields_to_update,
            "typecast": True 
        }
        try:
            await self._request("PATCH", url, json=request_body)
            logger.info(f"Successfully updated record {record_id}.")
            return True
        except Exception as e:
            logger.error(f"Failed to update Airtable record {record_id}: {str(e)}")
            return False
    
    async def batch_update_records(self, records_data: List[Dict[str, Any]]) -> Dict:
        """
        Batch update records in Airtable. Airtable API allows up to 10 records per batch update.
        Each item in records_data should be a dict like: {"id": "record_id", "fields": {"field1": "value1", ...}}
        """
        url = f"{self.base_url}/{self.table_name}"
        all_updated_ids = []
        all_failed_ids = []

        for i in range(0, len(records_data), 10):
            batch = records_data[i:i+10]
            request_body = {"records": batch, "typecast": True}
            try:
                response_data = await self._request("PATCH", url, json=request_body)
                updated_records_in_batch = response_data.get("records", [])
                for rec in updated_records_in_batch:
                    all_updated_ids.append(rec["id"])
                logger.info(f"Batch update successful for {len(updated_records_in_batch)} records.")
            except Exception as e:
                logger.error(f"Airtable batch update failed for a chunk starting at index {i}: {e}")
                # Add all IDs from this failed batch chunk to failed_ids
                for record_data in batch:
                    all_failed_ids.append(record_data.get("id", "unknown_id_in_failed_batch"))
            await asyncio.sleep(0.2) # Small delay between batch requests

        return {"updated_ids": all_updated_ids, "failed_ids": all_failed_ids}


class GitHubAPI_Async:
    def __init__(self, session: aiohttp.ClientSession, headers: Dict, excluded_repos: List[str]):
        self.session = session
        self.headers = headers
        self.excluded_repos = excluded_repos
        self.search_url = "https://api.github.com/search/code"

    async def search_contract_address(self, address: str) -> Tuple[bool, int]:
        params = {"q": address, "per_page": 10} # Fetch a few to check existence and count
        max_retries = 3
        current_retry = 0

        while current_retry < max_retries:
            try:
                async with self.session.get(self.search_url, headers=self.headers, params=params, timeout=30) as response:
                    if response.status == 403: # Rate limit
                        remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                        reset_time = int(response.headers.get('X-RateLimit-Reset', time.time()))
                        if remaining == 0:
                            sleep_time = max(1, reset_time - int(time.time())) + 5
                            logger.warning(f"GitHub rate limit exceeded. Waiting {sleep_time} seconds...")
                            await asyncio.sleep(sleep_time)
                            continue # Retry the request
                    
                    response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                    search_results = await response.json()
                    
                    valid_items = []
                    excluded_count = 0
                    for item in search_results.get('items', []):
                        repo_full_name = item.get('repository', {}).get('full_name', '')
                        if not repo_full_name:
                            continue
                        
                        should_exclude = False
                        for excluded_repo in self.excluded_repos:
                            if excluded_repo.endswith('/'):
                                if repo_full_name.startswith(excluded_repo):
                                    should_exclude = True
                                    break
                            elif repo_full_name == excluded_repo:
                                should_exclude = True
                                break
                        
                        if not should_exclude:
                            valid_items.append(item)
                        else:
                            excluded_count += 1
                    
                    valid_count = len(valid_items)
                    # total_api_count = search_results.get('total_count', 0) # This is total across all pages
                    # For our purpose, if valid_items > 0, it's found. valid_count is enough.
                    logger.debug(f"GitHub search for {address}: Found {valid_count} valid items, Excluded {excluded_count}")
                    return valid_count > 0, valid_count

            except aiohttp.ClientResponseError as e:
                logger.error(f"GitHub API Error searching {address} (Attempt {current_retry+1}/{max_retries}): {e.status} - {e.message}")
            except aiohttp.ClientError as e: # Includes connection errors, timeouts handled by aiohttp
                logger.error(f"GitHub request error for {address} (Attempt {current_retry+1}/{max_retries}): {e}")
            except asyncio.TimeoutError:
                logger.error(f"GitHub request timed out for {address} (Attempt {current_retry+1}/{max_retries})")
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from GitHub for {address}: {str(e)}")
                return False, 0 # Don't retry on JSON decode error for this search

            current_retry += 1
            if current_retry < max_retries:
                await asyncio.sleep(2 ** current_retry) # Exponential backoff
        
        logger.warning(f"GitHub search for {address} failed after {max_retries} retries.")
        return False, 0

class BlockscoutAPI_Async:
    def __init__(self, session: aiohttp.ClientSession, api_keys: Dict[int, str], chain_config: Dict[int, Dict]):
        self.session = session
        self.api_keys = api_keys
        self.chain_config = chain_config
        self._log_api_key_status()

    def _log_api_key_status(self) -> None:
        keys_available = 0
        keys_missing = 0
        logger.debug("Checking API keys for chains that require authentication (Async Blockscout):")
        for chain_id, config_item in self.chain_config.items():
            if config_item.get("requires_auth", False):
                has_key = chain_id in self.api_keys and self.api_keys[chain_id] is not None
                if has_key:
                    logger.debug(f"  ✓ API key available for {config_item['name']} (ID: {chain_id})")
                    keys_available += 1
                else:
                    logger.warning(f"  ✗ No API key found for {config_item['name']} (ID: {chain_id}) - required for authentication")
                    keys_missing += 1
        logger.debug(f"Async Blockscout API key status: {keys_available} available, {keys_missing} missing.")

    async def _make_request(self, url: str, chain_id: int, chain_name_for_log: str, timeout_override: Optional[int] = None) -> Optional[Dict]:
        headers = {}
        chain_conf = self.chain_config[chain_id]
        auth_type = chain_conf['auth_type']
        requires_auth = chain_conf.get('requires_auth', False)
        request_timeout = timeout_override or chain_conf.get('timeout', 20) # Default 20s for blockscout

        if "scan.org" in url: auth_type = "param" # Etherscan-style

        if requires_auth:
            api_key = self.api_keys.get(chain_id)
            if api_key:
                if auth_type == "header": headers['Authorization'] = f'Bearer {api_key}'
                elif auth_type == "param" and "apikey=" not in url and "api_key=" not in url:
                    separator = "&" if "?" in url else "?"
                    url = f"{url}{separator}apikey={api_key}"
            elif requires_auth: # No key but required
                logger.warning(f"Auth required for {chain_name_for_log} ({url}) but no API key.")
                # Depending on strictness, could return None or raise here.
                # For now, let it try; some endpoints might work without auth even if docs say otherwise.

        max_retries_for_chain = chain_conf.get('max_retries', 3)
        for attempt in range(max_retries_for_chain):
            try:
                logger.debug(f"[{chain_name_for_log}] Requesting {url} (Attempt {attempt + 1}, Timeout: {request_timeout}s)")
                async with self.session.get(url, headers=headers, timeout=request_timeout) as response:
                    if response.status == 429: # Rate limited
                        # Blockscout doesn't usually send Retry-After, so use exponential backoff
                        sleep_time = (2 ** attempt) + 2 # Basic backoff
                        logger.warning(f"[{chain_name_for_log}] Rate limit hit for {url}. Retrying after {sleep_time}s...")
                        await asyncio.sleep(sleep_time)
                        continue
                    elif response.status == 503 and "nginx" in await response.text():
                         logger.warning(f"[{chain_name_for_log}] Nginx 503 error for {url}. Retrying...")
                         await asyncio.sleep((2**attempt) + 3)
                         continue
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientResponseError as e:
                logger.warning(f"[{chain_name_for_log}] API Error for {url} (Attempt {attempt+1}): {e.status} - {e.message}")
                if e.status in [400, 401, 403, 404]: break # Don't retry client errors like bad request/auth/not found
            except aiohttp.ClientError as e: # Broader network errors
                logger.warning(f"[{chain_name_for_log}] Request error for {url} (Attempt {attempt+1}): {e}")
            except asyncio.TimeoutError:
                logger.warning(f"[{chain_name_for_log}] Request timed out for {url} (Attempt {attempt+1})")
            except json.JSONDecodeError as e:
                 logger.error(f"[{chain_name_for_log}] JSON decode error for {url}: {await response.text()[:200]}... Error: {e}")
                 break # Don't retry if response is not JSON
            if attempt < max_retries_for_chain - 1:
                 await asyncio.sleep( (2 ** attempt) + 1 ) # Exponential backoff
        return None # Failed after retries

    async def _try_endpoints(self, chain_id: int, endpoint_template: str, **kwargs) -> Optional[Dict]:
        if chain_id not in self.chain_config:
            logger.error(f"Chain ID {chain_id} not supported in BlockscoutAPI_Async config.")
            return None
        
        chain_conf = self.chain_config[chain_id]
        urls_to_try = chain_conf["urls"]
        chain_name_for_log = chain_conf["name"]
        
        for url_base in urls_to_try:
            formatted_endpoint = endpoint_template.format(url_base=url_base.rstrip('/'), **kwargs)
            data = await self._make_request(formatted_endpoint, chain_id, chain_name_for_log)
            if data is not None: # Success or non-empty error from _make_request parsing
                # Check if data itself indicates an error (e.g. Etherscan error object)
                if isinstance(data, dict) and data.get('message') == 'NOTOK':
                    logger.warning(f"[{chain_name_for_log}] Endpoint {formatted_endpoint} returned 'NOTOK': {data.get('result')}")
                    continue # Try next endpoint if available
                return data
        logger.warning(f"All Blockscout endpoints failed for chain {chain_name_for_log}, template {endpoint_template.split('?')[0]}")
        return None

    async def get_transactions(self, address: str, chain_id: int, limit: int = 5) -> List[Dict]:
        chain_conf = self.chain_config.get(chain_id)
        if not chain_conf: return []
        chain_name = chain_conf["name"]
        logger.debug(f"[{chain_name}] Fetching transactions for {address} (async)")

        # Special handling for Linea if needed (copied from sync version)
        if chain_id == 59144: # Linea
            logger.info(f"Using simplified transaction fetching for {chain_name} (async)")
            return BlockscoutAPI_Async._get_linea_transactions_simplified(address, limit)

        data = await self._try_endpoints(chain_id, "{url_base}/addresses/{address}/transactions", address=address)
        
        transactions = []
        if data and isinstance(data.get('items'), list):
            transactions = data['items']
        elif data and isinstance(data.get('result'), list): # Etherscan-like
            transactions = data['result']
        
        if not transactions:
             logger.debug(f"[{chain_name}] No txs from primary endpoint, trying /address/ (singular)...")
             data_alt = await self._try_endpoints(chain_id, "{url_base}/address/{address}/transactions", address=address)
             if data_alt and isinstance(data_alt.get('items'), list):
                 transactions = data_alt['items']

        parsed_txs = []
        for tx in transactions[:limit]:
            tx_data = {}
            if 'hash' in tx: # Blockscout format
                tx_data = {
                    'hash': tx.get('hash'), 'status': tx.get('status'),
                    'method': tx.get('method'), 'timestamp': tx.get('timestamp'),
                    'value': tx.get('value'),
                    'from': tx.get('from', {}).get('hash') if isinstance(tx.get('from'), dict) else tx.get('from'),
                    'to': tx.get('to', {}).get('hash') if isinstance(tx.get('to'), dict) else tx.get('to'),
                    'to_contract_name': tx.get('to', {}).get('name') if isinstance(tx.get('to'), dict) and tx.get('to', {}).get('name') else None,
                    'method_call': tx.get('decoded_input', {}).get('method_call') if tx.get('decoded_input') else None,
                    'parameters': tx.get('decoded_input', {}).get('parameters') if tx.get('decoded_input') else None
                }
            elif 'txHash' in tx or 'transactionHash' in tx : # Some other formats (e.g. Etherscan for logs)
                # This part is more for logs, but good to have a basic structure
                tx_data = {
                    'hash': tx.get('txHash') or tx.get('transactionHash'),
                    'timestamp': tx.get('timeStamp') # Etherscan uses timeStamp (string unix)
                     # Minimal for now, expand if this path is actually hit for txs
                }
                if tx_data['timestamp'] and tx_data['timestamp'].isdigit():
                    tx_data['timestamp'] = datetime.fromtimestamp(int(tx_data['timestamp'])).isoformat()
           
            if tx_data: parsed_txs.append(tx_data)
       
        logger.debug(f"[{chain_name}] Found {len(parsed_txs)} transactions for {address}")
        return parsed_txs

    @staticmethod # Can be static as it doesn't depend on self
    def _get_linea_transactions_simplified(address: str, limit: int = 5) -> List[Dict]:
        logger.debug(f"Creating simplified transaction data for {address} on Linea (async)")
        return [{
            'hash': f"0x{i:02d}{'0'*62}", 'status': 'unknown', 'method': 'unknown',
            'timestamp': datetime.now().isoformat(), 'value': '0',
            'from': address, 'to': address, 'logs': []
        } for i in range(min(limit, 3))]

    async def get_transaction_logs(self, tx_hash: str, chain_id: int) -> List[Dict]:
        chain_conf = self.chain_config.get(chain_id)
        if not chain_conf: return []
        chain_name = chain_conf["name"]
        logger.debug(f"[{chain_name}] Fetching logs for tx {tx_hash} (async)")

        # Skip logs for chains configured to do so (e.g., Linea)
        if chain_conf.get("skip_logs", False):
            logger.info(f"[{chain_name}] Skipping log fetching for tx {tx_hash} as per chain config.")
            return []

        data = await self._try_endpoints(chain_id, "{url_base}/transactions/{tx_hash}/logs", tx_hash=tx_hash)
        
        logs = []
        if data and isinstance(data.get('items'), list):
            logs = data['items']
        elif data and isinstance(data.get('result'), list): # Etherscan-like for logs (common)
            logs = data['result']

        parsed_logs = []
        for log_item in logs:
            if not isinstance(log_item, dict): 
                logger.warning(f"[{chain_name}] Skipping non-dict log item: {str(log_item)[:100]}")
                continue

            topics = log_item.get('topics', [])
            is_transfer = False
            if topics and isinstance(topics, list) and len(topics) > 0 and \
                   topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                is_transfer = True
            
            token_name = None
            log_address_field = log_item.get('address') # Blockscout puts token info in 'address' object
            if isinstance(log_address_field, dict):
                token_name = log_address_field.get('name')
                actual_address = log_address_field.get('hash')
            else:
                actual_address = log_address_field # Etherscan just has address string

            log_data = {
                'address': actual_address,
                'token_name': token_name,
                'is_transfer': is_transfer,
                'topics': topics,
                'data': log_item.get('data'),
                'method_call': log_item.get('decoded', {}).get('method_call') if log_item.get('decoded') else None,
                'parameters': log_item.get('decoded', {}).get('parameters') if log_item.get('decoded') else None
            }
            parsed_logs.append(log_data)
           
        logger.debug(f"[{chain_name}] Found {len(parsed_logs)} logs for tx {tx_hash}")
        return parsed_logs

class ContractDataProcessor_Async:
    def __init__(self, blockscout_api: BlockscoutAPI_Async, supported_chains_map: Dict[str, int], chain_aliases_map: Dict[str, str], chain_explorer_config: Dict[int, Dict]):
        self.blockscout_api = blockscout_api
        self.supported_chains_map = supported_chains_map
        self.chain_aliases_map = chain_aliases_map
        self.chain_explorer_config = chain_explorer_config # For validation

    async def process_contract_data(self, contract_address: str, chain_name_str: str, fetch_logs: bool = True, tx_limit: int = 5) -> Dict[str, Any]:
        """
        Fetches and processes blockchain data for a contract to extract name, tx methods, tokens, etc.
        Returns a dictionary of fields suitable for updating Airtable.
        """
        if not contract_address:
            logger.error("[ContractProcessor] No contract address provided.")
            return {"blockscout_fetch_status": "API Error - No Address"}

        chain_name_str_lower = chain_name_str.lower()
        if chain_name_str_lower in self.chain_aliases_map:
            final_chain_name = self.chain_aliases_map[chain_name_str_lower]
            logger.debug(f"[ContractProcessor] Mapped chain alias '{chain_name_str}' to '{final_chain_name}'")
        else:
            final_chain_name = chain_name_str_lower
        
        chain_id = self.supported_chains_map.get(final_chain_name)

        if not chain_id:
            logger.error(f"[ContractProcessor] Unsupported chain: {chain_name_str} (resolved to {final_chain_name})")
            return {"blockscout_fetch_status": f"Unsupported Chain: {chain_name_str}"}
        
        if chain_id not in self.chain_explorer_config:
            logger.error(f"[ContractProcessor] Chain ID {chain_id} ({final_chain_name}) missing from CHAIN_EXPLORER_CONFIG.")
            return {"blockscout_fetch_status": f"Config Error - Chain ID {chain_id}"}

        # Initialize results with default values
        results = {
            "contract_name": None,
            "recent_tx_methods": None,
            "involved_tokens": None,
            "involves_token_transfer": False,
            "blockscout_fetch_status": "No Transactions Found", # Default if no txs
            "transaction_summary_json": None,
            "last_activity_timestamp": None
        }

        try:
            logger.debug(f"[ContractProcessor] Getting transactions with logs for {contract_address} on {final_chain_name} (ID: {chain_id})")
            raw_transactions = await self.blockscout_api.get_transactions(contract_address, chain_id, limit=tx_limit)
            
            processed_transactions_with_logs = []
            if raw_transactions:
                results["blockscout_fetch_status"] = "Partial Data" # Initial status if we have txs
                
                # Fetch logs concurrently for these transactions if needed
                log_tasks = []
                chain_conf = self.blockscout_api.chain_config.get(chain_id, {})
                should_fetch_logs_for_chain = fetch_logs and not chain_conf.get("skip_logs", False)
                
                if should_fetch_logs_for_chain:
                    for tx in raw_transactions:
                        if tx.get('hash'):
                            log_tasks.append(self.blockscout_api.get_transaction_logs(tx['hash'], chain_id))
                        else:
                            log_tasks.append(asyncio.sleep(0, result=[])) # Placeholder for txs without hash for some reason
                
                all_logs_list = []
                if log_tasks:
                    logger.debug(f"[ContractProcessor] Fetching logs for {len(log_tasks)} transactions concurrently.")
                    all_logs_list = await asyncio.gather(*log_tasks, return_exceptions=True)
                
                for i, tx in enumerate(raw_transactions):
                    tx_copy = tx.copy() # Work with a copy
                    if i < len(all_logs_list) and isinstance(all_logs_list[i], list):
                        tx_copy['logs'] = all_logs_list[i]
                    elif i < len(all_logs_list) and isinstance(all_logs_list[i], Exception):
                        logger.warning(f"[ContractProcessor] Error fetching logs for tx {tx.get('hash')}: {all_logs_list[i]}")
                        tx_copy['logs'] = [] # Empty logs on error
                    else:
                        tx_copy['logs'] = [] # No logs fetched or error
                    processed_transactions_with_logs.append(tx_copy)
           
            if not processed_transactions_with_logs:
                logger.info(f"[ContractProcessor] No transactions found for {contract_address} on {final_chain_name}.")
                # blockscout_fetch_status remains "No Transactions Found" as set by default
                return results 

            # ---- Process transactions for desired fields ----
            results["blockscout_fetch_status"] = "Success" # Assume success if we got this far with txs
            
            if processed_transactions_with_logs[0].get('timestamp'):
                results["last_activity_timestamp"] = processed_transactions_with_logs[0]['timestamp']

            # Extract contract name from transactions if 'to_contract_name' matches address
            for tx in processed_transactions_with_logs:
                if tx.get('to_contract_name') and tx.get('to', '').lower() == contract_address.lower():
                    results["contract_name"] = tx['to_contract_name']
                    logger.info(f"[ContractProcessor] Found contract name '{results['contract_name']}' for {contract_address}")
                    break
            
            tx_methods = set()
            for tx in processed_transactions_with_logs:
                if tx.get('method'): tx_methods.add(tx['method'])
                if tx.get('method_call'): tx_methods.add(tx['method_call'])
            if tx_methods: results["recent_tx_methods"] = ", ".join(sorted(list(tx_methods)))[:1024] # Airtable text limit

            involves_transfer_flag = False
            involved_tokens_set = set()
            for tx in processed_transactions_with_logs:
                for log in tx.get('logs', []):
                    if log.get('is_transfer'): involves_transfer_flag = True
                    token_name = log.get('token_name')
                    if token_name: involved_tokens_set.add(token_name)
                    elif log.get('address') and isinstance(log['address'], str) and log['address'].startswith('0x') and len(log['address']) == 42:
                        involved_tokens_set.add(log['address']) # Add address if no name
            results["involves_token_transfer"] = involves_transfer_flag
            if involved_tokens_set: results["involved_tokens"] = ", ".join(sorted(list(involved_tokens_set)))[:1024]

            # Sanitize and truncate transaction_summary_json
            try:
                sanitized_tx_summary = []
                for tx_original in processed_transactions_with_logs[:5]: # Limit to 5 txs for summary
                    tx_sum_item = {k: v for k, v in tx_original.items() if k not in ['parameters', 'from', 'to']} # Remove bulky/less useful items for summary
                    if "logs" in tx_sum_item:
                        sanitized_logs = []
                        for log_original in tx_sum_item["logs"][:3]: # Limit to 3 logs per tx
                            log_sum_item = {
                                "address": log_original.get('address'),
                                "is_transfer": log_original.get('is_transfer', False),
                                "token_name": log_original.get('token_name'),
                                "method_call": log_original.get('method_call') # Keep decoded log method if present
                            }
                            sanitized_logs.append(log_sum_item)
                        tx_sum_item["logs"] = sanitized_logs
                    sanitized_tx_summary.append(tx_sum_item)
                
                limited_tx_data_for_json = {
                    "status": results["blockscout_fetch_status"], 
                    "transactions": sanitized_tx_summary
                }
                json_str = json.dumps(limited_tx_data_for_json)
                if len(json_str) < 95000:
                    results["transaction_summary_json"] = json_str
                else:
                    logger.warning(f"[ContractProcessor] Transaction JSON for {contract_address} too large ({len(json_str)} chars), truncating further.")
                    limited_tx_data_for_json["transactions"] = limited_tx_data_for_json["transactions"][:2] # Truncate to 2 txs
                    json_str = json.dumps(limited_tx_data_for_json)
                    if len(json_str) < 95000:
                        results["transaction_summary_json"] = json_str
                    else:
                        logger.warning(f"[ContractProcessor] Still too large. Omitting transaction_summary_json for {contract_address}.")
            except Exception as e:
                logger.error(f"[ContractProcessor] Error serializing transaction_summary_json for {contract_address}: {e}")

        except Exception as e:
            logger.error(f"[ContractProcessor] Error processing contract data for {contract_address} on {final_chain_name}: {e}", exc_info=True)
            results["blockscout_fetch_status"] = "API Error - Processing Exception"
            # Potentially add error details to another field
        
        return results

# ===============================================
# CORE PROCESSING LOGIC (Placeholders)
# ===============================================

async def process_record_concurrently(record: Dict, 
                                      session: aiohttp.ClientSession, 
                                      semaphore: asyncio.Semaphore, 
                                      github_api: GitHubAPI_Async, 
                                      blockscout_api: BlockscoutAPI_Async, 
                                      contract_processor: ContractDataProcessor_Async \
                                      ) -> Optional[Dict]:
    """
    Processes a single record:
    1. Fetches GitHub info.
    2. Fetches contract name and transaction data (methods, tokens, transfer flag).
    Returns a dictionary with data to update in Airtable, or None if processing fails.
    """
    record_id = record.get('id')
    fields = record.get('fields', {})
    contract_address = fields.get('address')
    origin_key_list = fields.get('origin_key') # It's a list of record IDs

    if not contract_address or not record_id:
        logger.warning(f"Skipping record due to missing address or ID: {record}")
        return None

    # Resolve chain (simplified, assuming first origin_key is the one)
    chain_name_str = "base" # Default
    if origin_key_list and isinstance(origin_key_list, list) and len(origin_key_list) > 0:
        chain_ref_id = origin_key_list[0]
        chain_name_str = ORIGIN_KEY_TO_CHAIN_MAP.get(chain_ref_id, "base")
    elif isinstance(origin_key_list, str): # Should not happen based on typical Airtable structure but good to handle
        chain_name_str = ORIGIN_KEY_TO_CHAIN_MAP.get(origin_key_list, "base")

    logger.info(f"Processing record {record_id}: Address {contract_address} on chain {chain_name_str}")
    
    results_to_update = {}

    async with semaphore: # Ensure only a limited number of these blocks run concurrently
        try:
            # 1. Fetch GitHub Info
            logger.debug(f"[{record_id}] Fetching GitHub info for {contract_address}...")
            github_found, repo_count = await github_api.search_contract_address(contract_address)
            results_to_update["github_found"] = github_found
            results_to_update["repo_count"] = repo_count
            logger.debug(f"[{record_id}] GitHub info: Found={github_found}, Count={repo_count}")

            # 2. Fetch Contract Name & Transaction Data
            logger.debug(f"[{record_id}] Processing contract data for {contract_address} on {chain_name_str}...")
            contract_data_results = await contract_processor.process_contract_data(contract_address, chain_name_str)
            results_to_update.update(contract_data_results) # Merge in all fields from contract_processor
            
            logger.info(f"[{record_id}] Processed {contract_address}. GH: {github_found}, Name: {results_to_update.get('contract_name')}, Status: {results_to_update.get('blockscout_fetch_status')}")
            return results_to_update
            
        except Exception as e:
            logger.error(f"Error processing record {record_id} ({contract_address}): {e}", exc_info=True)
            return None # Indicate failure for this record

# ===============================================
# MAIN EXECUTION
# ===============================================

async def main_async(args):
    """Main asynchronous function to orchestrate the processing."""
    if not is_config_valid:
        logger.error("Invalid configuration. Exiting.")
        return

    async with aiohttp.ClientSession() as session:
        airtable_api = AirtableAPI_Async(session, Config.AIRTABLE_HEADERS, Config.AIRTABLE_BASE_URL, Config.TABLE_NAME, Config.CHAIN_TABLE_ID)
        github_api = GitHubAPI_Async(session, Config.GITHUB_HEADERS, Config.EXCLUDED_REPOS)
        blockscout_api = BlockscoutAPI_Async(session, Config.BLOCKSCOUT_API_KEYS, Config.CHAIN_EXPLORER_CONFIG)
        contract_processor = ContractDataProcessor_Async(blockscout_api, Config.SUPPORTED_CHAINS, Config.CHAIN_NAME_ALIASES, Config.CHAIN_EXPLORER_CONFIG)
        
        # Initialize ORIGIN_KEY_TO_CHAIN_MAP
        global ORIGIN_KEY_TO_CHAIN_MAP
        ORIGIN_KEY_TO_CHAIN_MAP = await airtable_api.fetch_chain_mappings()
        if not ORIGIN_KEY_TO_CHAIN_MAP:
            logger.warning("Chain mappings are empty. Processing might be affected.")

        # Overall counters
        total_records_processed = 0
        total_successfully_updated = 0
        total_failed_updates = 0
        total_processing_errors = 0

        semaphore = asyncio.Semaphore(args.concurrency)
        
        # TODO: Implement fetching records from Airtable in batches
        # For now, let's simulate a few records
        
        current_offset = None
        records_processed_in_run = 0
        
        while True: # Loop for pagination
            if args.max_records and records_processed_in_run >= args.max_records:
                logger.info(f"Reached max_records limit of {args.max_records}. Stopping.")
                break

            records_to_process_batch, new_offset = await airtable_api.fetch_records_to_process(
                view_id=Config.TARGET_VIEW_ID_V2, # Use the new view ID from Config
                batch_size=args.batch_size,
                offset=current_offset
            )

            if not records_to_process_batch:
                if current_offset is None and not records_processed_in_run: # No records found at all
                    logger.info("No records found in the target view to process.")
                else: # No more records in subsequent pages
                    logger.info("No more records to process.")
                break
            
            batch_tasks = []
            for record in records_to_process_batch:
                if args.max_records and records_processed_in_run >= args.max_records:
                    break 
                # TODO: Potentially update record status to "processing" in Airtable here (individually or batch)
                # Consider adding a "v2_processing_status" field and updating it.
                # Example: await airtable_api.update_record(record['id'], {"v2_processing_status": "processing"})
                batch_tasks.append(process_record_concurrently(record, session, semaphore, github_api, blockscout_api, contract_processor))
                records_processed_in_run += 1
            
            if not batch_tasks: # if max_records was hit before adding any tasks from this batch
                break

            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Prepare records for batch update
            successful_updates_payload = []

            for i, result_or_exc in enumerate(results):
                # Ensure index is within bounds of the fetched batch
                if i >= len(records_to_process_batch): continue 
                
                record_id = records_to_process_batch[i].get("id", "Unknown")
                fields_to_update_on_completion = {} # Store final status updates

                if isinstance(result_or_exc, Exception):
                    logger.error(f"Error processing record {record_id}: {result_or_exc}", exc_info=result_or_exc)
                    total_processing_errors += 1
                    fields_to_update_on_completion["v2_processing_status"] = "error" 
                    # Optionally add an error message field: fields_to_update_on_completion["v2_error_message"] = str(result_or_exc)[:255]
                elif result_or_exc is not None: 
                    logger.info(f"Successfully processed record {record_id}.")
                    # result_or_exc should be the dict of fields to update from process_record_concurrently
                    # Merge with status update
                    fields_to_update = {**result_or_exc, "v2_processing_status": "complete"}
                    successful_updates_payload.append({"id": record_id, "fields": fields_to_update})
                    # No need for fields_to_update_on_completion here as it's part of the main payload
                else:
                    logger.warning(f"Processing for record {record_id} completed with no data to update (or a handled failure).")
                    fields_to_update_on_completion["v2_processing_status"] = "complete_no_data"
                
                # If there's a status to update due to error or no_data, and it wasn't part of a successful payload
                if fields_to_update_on_completion and not isinstance(result_or_exc, dict):
                     successful_updates_payload.append({"id": record_id, "fields": fields_to_update_on_completion})


                total_records_processed +=1 # This counts attempts

            # Batch update Airtable for successfully processed records and status updates
            if successful_updates_payload:
                logger.info(f"Attempting to batch update {len(successful_updates_payload)} records in Airtable.")
                update_results = await airtable_api.batch_update_records(successful_updates_payload)
                total_successfully_updated += len(update_results.get("updated_ids", []))
                total_failed_updates += len(update_results.get("failed_ids", []))
                if update_results.get("failed_ids"):
                    logger.error(f"Failed to batch update records: {update_results['failed_ids']}")


            current_offset = new_offset
            if not current_offset:
                logger.info("Reached the end of records for this view.")
                break
            
            logger.info(f"Processed {records_processed_in_run} records so far. Fetching next batch...")
            await asyncio.sleep(1) # Brief pause before fetching next page


    logger.info("Concurrent processing finished.")
    logger.info(f"Summary: Processed: {total_records_processed}, Updated: {total_successfully_updated}, Failed Updates: {total_failed_updates}, Errors: {total_processing_errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Concurrently process smart contracts for GitHub info, names, and transaction data.')
    parser.add_argument('--batch-size', type=int, default=50, help='Number of records to fetch and process from Airtable in each batch.')
    parser.add_argument('--concurrency', type=int, default=10, help='Maximum number of records to process concurrently.')
    parser.add_argument('--max-records', type=int, default=None, help='Maximum number of records to process in total (for testing).')
    # TODO: Add argument for Airtable View ID if it's not hardcoded or if multiple views are supported

    args = parser.parse_args()

    if not Config.AIRTABLE_TOKEN or not Config.GITHUB_TOKEN:
        logger.error("REACT_APP_AIRTABLE_TOKEN and GITHUB_TOKEN environment variables must be set.")
    else:
        asyncio.run(main_async(args)) 