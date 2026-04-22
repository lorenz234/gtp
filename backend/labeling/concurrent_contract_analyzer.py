import asyncio
import aiohttp
import os
import json
import logging
import argparse
import time
import random
import ssl
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv

# Fix SSL certificate issues on macOS
def create_ssl_context():
    """Create SSL context that handles certificate verification issues"""
    # For macOS, we need to disable SSL verification due to certificate issues
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return ssl_context

# ===============================================
# ENHANCED ERROR HANDLING AND RATE LIMITING
# ===============================================

class ErrorType(Enum):
    """Categorize errors for better handling"""
    TRANSIENT = "transient"  # Temporary issues, should retry
    PERMANENT = "permanent"  # Permanent failures, don't retry
    RATE_LIMIT = "rate_limit"  # Rate limiting, special handling
    AUTH = "auth"  # Authentication issues
    NOT_FOUND = "not_found"  # Resource not found

class ServiceHealth:
    """Track service health for circuit breaker pattern"""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.is_circuit_open = False
        self.recent_errors = deque(maxlen=10)  # Track recent error types
    
    def record_success(self):
        """Record successful operation"""
        self.failure_count = max(0, self.failure_count - 1)  # Gradual recovery
        if self.failure_count == 0:
            self.is_circuit_open = False
    
    def record_failure(self, error_type: ErrorType):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        self.recent_errors.append(error_type)
        
        if self.failure_count >= self.failure_threshold:
            self.is_circuit_open = True
    
    def should_attempt_request(self) -> bool:
        """Check if we should attempt a request or if circuit is open"""
        if not self.is_circuit_open:
            return True
        
        # Check if enough time has passed for recovery attempt
        if self.last_failure_time and (time.time() - self.last_failure_time) > self.recovery_timeout:
            return True
        
        return False
    
    def get_health_status(self) -> str:
        """Get human-readable health status"""
        if self.is_circuit_open:
            time_until_retry = max(0, self.recovery_timeout - (time.time() - (self.last_failure_time or 0)))
            return f"Circuit Open (retry in {int(time_until_retry)}s)"
        elif self.failure_count > 0:
            return f"Degraded ({self.failure_count} failures)"
        return "Healthy"

class RateLimitManager:
    """Centralized rate limiting with improved backoff strategies"""
    def __init__(self):
        self.service_limits = {}
        self.service_health = defaultdict(ServiceHealth)
    
    def update_rate_limit(self, service: str, remaining: int, total: int, reset_time: Optional[int] = None):
        """Update rate limit info for a service"""
        self.service_limits[service] = {
            'remaining': remaining,
            'total': total,
            'reset_time': reset_time or (int(time.time()) + 3600),
            'last_updated': time.time()
        }
    
    async def wait_if_needed(self, service: str) -> bool:
        """Check if we need to wait due to rate limits. Returns False if service is unavailable."""
        health = self.service_health[service]
        
        # Circuit breaker check
        if not health.should_attempt_request():
            return False
        
        limits = self.service_limits.get(service)
        if not limits:
            return True
        
        # Check if rate limit is nearly exhausted
        if limits['remaining'] <= 2:
            wait_time = max(0, limits['reset_time'] - int(time.time()))
            if wait_time > 0:
                # Add jitter to prevent thundering herd
                jitter = random.uniform(0.1, 0.3) * wait_time
                total_wait = min(wait_time + jitter, 300)  # Cap at 5 minutes
                logger.info(f"[{service}] Rate limit nearly exhausted. Waiting {total_wait:.1f}s")
                await asyncio.sleep(total_wait)
        
        return True
    
    def calculate_backoff(self, attempt: int, base_wait: float = 2.0, max_wait: float = 60.0) -> float:
        """Calculate exponential backoff with jitter"""
        wait_time = min(base_wait * (2 ** attempt), max_wait)
        jitter = random.uniform(0.1, 0.3) * wait_time
        return wait_time + jitter
    
    def record_success(self, service: str):
        """Record successful operation for circuit breaker"""
        self.service_health[service].record_success()
    
    def record_failure(self, service: str, error_type: ErrorType):
        """Record failed operation for circuit breaker"""
        self.service_health[service].record_failure(error_type)
    
    def get_service_status(self, service: str) -> str:
        """Get service health status"""
        return self.service_health[service].get_health_status()

# Global rate limit manager
rate_limit_manager = RateLimitManager()

class GracefulErrorHandler:
    """Centralized error handling with graceful degradation"""
    
    @staticmethod
    def categorize_error(error: Exception, status_code: Optional[int] = None) -> ErrorType:
        """Categorize error type for appropriate handling"""
        if isinstance(error, aiohttp.ClientResponseError):
            if error.status == 429:
                return ErrorType.RATE_LIMIT
            elif error.status in [401, 403]:
                return ErrorType.AUTH
            elif error.status == 404:
                return ErrorType.NOT_FOUND
            elif error.status >= 500:
                return ErrorType.TRANSIENT
            else:
                return ErrorType.PERMANENT
        elif isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError)):
            return ErrorType.TRANSIENT
        elif isinstance(error, json.JSONDecodeError):
            return ErrorType.PERMANENT
        else:
            return ErrorType.TRANSIENT
    
    @staticmethod
    def should_retry(error_type: ErrorType, attempt: int, max_retries: int) -> bool:
        """Determine if we should retry based on error type"""
        if attempt >= max_retries:
            return False
        
        return error_type in [ErrorType.TRANSIENT, ErrorType.RATE_LIMIT]
    
    @staticmethod
    def log_error_appropriately(service: str, error: Exception, attempt: int, max_retries: int, context: str = ""):
        """Log errors at appropriate levels to reduce spam"""
        error_type = GracefulErrorHandler.categorize_error(error)
        
        # Use debug for retryable errors on early attempts
        if error_type in [ErrorType.TRANSIENT, ErrorType.RATE_LIMIT] and attempt < max_retries - 1:
            log_level = logging.DEBUG
        elif error_type == ErrorType.NOT_FOUND:
            log_level = logging.DEBUG  # 404s are often expected
        elif error_type == ErrorType.AUTH:
            log_level = logging.WARNING  # Auth issues are important but not critical
        else:
            log_level = logging.WARNING
        
        status_code = getattr(error, 'status', 'N/A')
        message = f"[{service}] {error_type.value} error ({status_code})"
        if context:
            message += f" - {context}"
        if attempt < max_retries - 1 and GracefulErrorHandler.should_retry(error_type, attempt, max_retries):
            message += f" (attempt {attempt + 1}/{max_retries}, will retry)"
        else:
            message += f" (attempt {attempt + 1}/{max_retries}, final)"
        
        logger.log(log_level, f"{message}: {str(error)}")

# ===============================================
# CONFIGURATION
# ===============================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger('ConcurrentContractAnalyzer')

# Load environment variables
load_dotenv()

class Config:
    """Central configuration class for the application."""

    # GitHub configuration
    # Prefer GitHub App credentials (30 req/min for code search) over personal token (10 req/min).
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    GITHUB_APP_ID = os.getenv('GITHUB_APP_ID')
    GITHUB_APP_INSTALLATION_ID = os.getenv('GITHUB_APP_INSTALLATION_ID')
    # PEM stored as base64 env var — no file needed, works locally and in Airflow
    GITHUB_APP_PRIVATE_KEY = os.getenv('GITHUB_APP_PRIVATE_KEY')  # base64-encoded PEM content

    @classmethod
    def get_github_headers(cls) -> dict:
        """Return auth headers. Uses GitHub App installation token if configured, else personal token."""
        if cls.GITHUB_APP_ID and cls.GITHUB_APP_INSTALLATION_ID and cls.GITHUB_APP_PRIVATE_KEY:
            try:
                import jwt as pyjwt
                import base64
                import requests as _requests

                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                pem_bytes = base64.b64decode(cls.GITHUB_APP_PRIVATE_KEY)
                private_key = load_pem_private_key(pem_bytes, password=None)
                now = int(time.time())
                payload = {'iat': now - 60, 'exp': now + 540, 'iss': str(cls.GITHUB_APP_ID)}
                jwt_token = pyjwt.encode(payload, private_key, algorithm='RS256')

                resp = _requests.post(
                    f'https://api.github.com/app/installations/{cls.GITHUB_APP_INSTALLATION_ID}/access_tokens',
                    headers={'Authorization': f'Bearer {jwt_token}', 'Accept': 'application/vnd.github.v3+json'},
                    timeout=10,
                )
                resp.raise_for_status()
                token = resp.json()['token']
                logger.info("[GitHub] Using GitHub App installation token (30 req/min)")
                return {'Authorization': f'token {token}', 'Accept': 'application/vnd.github.v3+json'}
            except Exception as e:
                import traceback as _tb
                logger.warning(
                    f"[GitHub] App token fetch failed ({type(e).__name__}: {e}) — "
                    f"falling back to personal token. Check GITHUB_APP_ID, "
                    f"GITHUB_APP_INSTALLATION_ID, and GITHUB_APP_PRIVATE_KEY (must be base64-encoded PEM)."
                )
                logger.debug(f"[GitHub] App token traceback:\n{_tb.format_exc()}")

        return {'Authorization': f'token {cls.GITHUB_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}

    # API Headers — evaluated at import time for the personal token path;
    # call get_github_headers() at runtime to get a fresh App token.
    GITHUB_HEADERS = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Tenderly Node API key — needed for tenderly_getTransactionsRange fallback.
    # Set TENDERLY_ACCESS_KEY in .env. Without it, the range fallback is skipped.
    TENDERLY_ACCESS_KEY = os.getenv("TENDERLY_ACCESS_KEY")

    # Blockscout API Keys — populated by load_from_db(); single pro key covers all chains
    BLOCKSCOUT_API_KEYS: dict = {}

    EXCLUDED_REPOS = [
        "HelayLiu/utils_download",
        "KeystoneHQ/Smart-Contract-Metadata-Registry",
        "tangtj/",
        "drumonbase/txdata",
        "tahaghazi/research",
        "msinghal28/TGNN",
        "0xtorch/datasource",
        "genTx/json_test.go",
        "flyq/opblock",
        "celo-org/celo-kona",
        "Layr-Labs/celo-kona",
        "base/infra",
        "edgex-Tech/go-ethereum"
    ]
    
    # Chains the automated labeler supports (must have Blockscout Pro API access).
    # origin_key → chain_id populated by load_from_db(); only the keys list is configured here.
    # Use exact origin_keys as they appear in sys_main_conf (e.g. "polygon_pos" not "polygon").
    SUPPORTED_ORIGIN_KEYS: list = [
        "ethereum", "optimism", "base", "arbitrum", "scroll",
        "polygon_pos", "unichain", "celo", "megaeth", "mode",
    ]
    SUPPORTED_CHAINS: dict = {}  # populated by load_from_db()

    # chain_id → (tenderly_network_slug, avg_block_time_seconds)
    # Populated by load_from_db() from sys_main_conf.labeling_tenderly_slug + labeling_avg_block_time_seconds.
    CHAIN_TO_NODE: dict = {}

    # Chain explorer config — populated by load_from_db(); URL pattern derived from chain_id.
    CHAIN_EXPLORER_CONFIG: dict = {}

    @classmethod
    def load_from_db(cls, engine) -> None:
        """Populate SUPPORTED_CHAINS, BLOCKSCOUT_API_KEYS, CHAIN_EXPLORER_CONFIG,
        and CHAIN_TO_NODE from sys_main_conf."""
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT origin_key, caip2, name, "
                "       labeling_tenderly_slug, labeling_avg_block_time_seconds "
                "FROM sys_main_conf "
                "WHERE caip2 IS NOT NULL AND caip2 LIKE 'eip155:%'"
            )).fetchall()

        # r: (origin_key, caip2, name, tenderly_slug, block_time)
        caip2_map = {r[0]: r for r in rows}
        pro_key = os.getenv("BLOCKSCOUT_API_KEY_PRO")

        supported_chains = {}
        blockscout_api_keys = {}
        chain_explorer_config = {}
        chain_to_node = {}

        for origin_key in cls.SUPPORTED_ORIGIN_KEYS:
            if origin_key not in caip2_map:
                logger.warning(f"[Config] origin_key '{origin_key}' not found in sys_main_conf — skipping")
                continue
            _, caip2, display_name, tenderly_slug, block_time = caip2_map[origin_key]
            try:
                chain_id = int(caip2.split(":")[1])
            except (IndexError, ValueError):
                logger.warning(f"[Config] Cannot parse chain_id from caip2='{caip2}' for {origin_key} — skipping")
                continue

            supported_chains[origin_key] = chain_id
            blockscout_api_keys[chain_id] = pro_key
            chain_explorer_config[chain_id] = {
                "name": display_name or origin_key,
                "urls": [f"https://api.blockscout.com/{chain_id}/api/v2/"],
                "auth_type": "param",
                "requires_auth": True,
            }

            if tenderly_slug and block_time is not None:
                chain_to_node[chain_id] = (tenderly_slug, float(block_time))

        cls.SUPPORTED_CHAINS = supported_chains
        cls.BLOCKSCOUT_API_KEYS = blockscout_api_keys
        cls.CHAIN_EXPLORER_CONFIG = chain_explorer_config
        cls.CHAIN_TO_NODE = chain_to_node
        logger.info(
            f"[Config] Loaded {len(supported_chains)} chains from DB: {list(supported_chains)}  "
            f"tenderly_nodes={len(chain_to_node)}"
        )

    @classmethod
    def validate_config(cls) -> bool:
        if not cls.GITHUB_TOKEN:
            logger.warning("Missing GITHUB_TOKEN environment variable — GitHub search will be skipped.")
        missing_keys = []
        for chain_id, config in cls.CHAIN_EXPLORER_CONFIG.items():
            if config.get("requires_auth", False):
                if chain_id not in cls.BLOCKSCOUT_API_KEYS or cls.BLOCKSCOUT_API_KEYS[chain_id] is None:
                    missing_keys.append(f"{config['name']} (ID: {chain_id})")
        if missing_keys:
            logger.info(f"Optional API keys not configured: {', '.join(missing_keys)} (some features may be limited)")
        return True

is_config_valid = Config.validate_config()
if is_config_valid:
    logger.info("Configuration loaded successfully.")

# ===============================================
# ASYNC API CLASSES
# ===============================================

class GitHubAPI_Async:
    # GitHub code search: 10 req/min for personal tokens, 30 req/min for GitHub Apps.
    # We proactively space requests to stay just under the limit instead of hitting it
    # and waiting a full 60-second reset window.
    _REQUESTS_PER_MINUTE = 30 if (os.getenv('GITHUB_APP_ID') and os.getenv('GITHUB_APP_PRIVATE_KEY')) else 10
    _MIN_INTERVAL = 60.0 / _REQUESTS_PER_MINUTE  # 2.0s with App token, 6.0s with personal token

    def __init__(self, session: aiohttp.ClientSession, headers: Dict, excluded_repos: List[str]):
        self.session = session
        self.headers = headers
        self.excluded_repos = excluded_repos
        self.search_url = "https://api.github.com/search/code"
        self._lock = asyncio.Lock()
        self._last_call_at: float = 0.0

    async def _throttle(self) -> None:
        """Ensure at least _MIN_INTERVAL seconds between GitHub search calls."""
        async with self._lock:
            now = time.time()
            wait = self._MIN_INTERVAL - (now - self._last_call_at)
            if wait > 0:
                logger.debug(f"[GitHub] Throttling {wait:.1f}s to stay under rate limit")
                await asyncio.sleep(wait)
            self._last_call_at = time.time()

    async def search_contract_address(self, address: str) -> Tuple[bool, int]:
        """Return (has_results, total_count). Uses per_page=1 — we only need existence, not items."""
        service_name = "github"
        params = {"q": address, "per_page": 1}
        max_retries = 3

        # Check circuit breaker
        if not await rate_limit_manager.wait_if_needed(service_name):
            logger.debug(f"[GitHub] Service unavailable: {rate_limit_manager.get_service_status(service_name)}")
            return False, 0

        await self._throttle()

        for attempt in range(max_retries):
            try:
                async with self.session.get(self.search_url, headers=self.headers, params=params, timeout=30) as response:
                    if response.status == 403:
                        remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                        reset_time = int(response.headers.get('X-RateLimit-Reset', time.time()))

                        if remaining == 0:
                            sleep_time = max(1, reset_time - int(time.time())) + 5
                            rate_limit_manager.update_rate_limit(service_name, 0, 100, reset_time)
                            rate_limit_manager.record_failure(service_name, ErrorType.RATE_LIMIT)
                            if attempt < max_retries - 1:
                                logger.info(f"[GitHub] Rate limit hit. Waiting {sleep_time}s for reset...")
                                await asyncio.sleep(sleep_time)
                                continue
                            else:
                                logger.warning(f"[GitHub] Rate limit exceeded on final attempt")
                                break
                        else:
                            rate_limit_manager.record_failure(service_name, ErrorType.AUTH)
                            logger.warning(f"[GitHub] Authentication error (403) for {address}")
                            return False, 0

                    response.raise_for_status()
                    data = await response.json()

                    remaining = int(response.headers.get('X-RateLimit-Remaining', 100))
                    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 3600))
                    rate_limit_manager.update_rate_limit(service_name, remaining, 1000, reset_time)
                    rate_limit_manager.record_success(service_name)

                    total = data.get('total_count', 0)
                    logger.debug(f"[GitHub] Search for {address}: total_count={total}")
                    return total > 0, total

            except Exception as e:
                error_type = GracefulErrorHandler.categorize_error(e)
                rate_limit_manager.record_failure(service_name, error_type)
                GracefulErrorHandler.log_error_appropriately(service_name, e, attempt, max_retries, f"search {address}")

                if not GracefulErrorHandler.should_retry(error_type, attempt, max_retries):
                    if error_type == ErrorType.PERMANENT:
                        return False, 0
                    break

                if attempt < max_retries - 1:
                    wait_time = rate_limit_manager.calculate_backoff(attempt)
                    await asyncio.sleep(wait_time)

        logger.warning(f"[GitHub] Search for {address} failed after {max_retries} retries.")
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
        service_name = f"blockscout_{chain_name_for_log.lower()}"
        headers = {}
        chain_conf = self.chain_config[chain_id]
        auth_type = chain_conf['auth_type']
        requires_auth = chain_conf.get('requires_auth', False)
        request_timeout = timeout_override or chain_conf.get('timeout', 20)

        if "scan.org" in url: auth_type = "param"  # Etherscan-style

        # Check circuit breaker
        if not await rate_limit_manager.wait_if_needed(service_name):
            logger.debug(f"[{chain_name_for_log}] Service unavailable: {rate_limit_manager.get_service_status(service_name)}")
            return None

        if requires_auth:
            api_key = self.api_keys.get(chain_id)
            if api_key:
                if auth_type == "header": 
                    headers['Authorization'] = f'Bearer {api_key}'
                elif auth_type == "param" and "apikey=" not in url and "api_key=" not in url:
                    separator = "&" if "?" in url else "?"
                    url = f"{url}{separator}apikey={api_key}"
            elif requires_auth:
                logger.debug(f"[{chain_name_for_log}] Auth required but no API key available")
                rate_limit_manager.record_failure(service_name, ErrorType.AUTH)
                return None

        max_retries_for_chain = chain_conf.get('max_retries', 3)
        _last_error_type: ErrorType = ErrorType.TRANSIENT
        for attempt in range(max_retries_for_chain):
            try:
                logger.debug(f"[{chain_name_for_log}] Requesting {url} (Attempt {attempt + 1}, Timeout: {request_timeout}s)")
                async with self.session.get(url, headers=headers, timeout=request_timeout) as response:
                    if response.status == 429:  # Rate limited
                        sleep_time = rate_limit_manager.calculate_backoff(attempt, base_wait=2.0)
                        rate_limit_manager.update_rate_limit(service_name, 0, 100, int(time.time()) + int(sleep_time))
                        _last_error_type = ErrorType.RATE_LIMIT

                        if attempt < max_retries_for_chain - 1:
                            logger.info(f"[{chain_name_for_log}] Rate limit hit. Waiting {sleep_time:.1f}s...")
                            await asyncio.sleep(sleep_time)
                            continue
                        else:
                            logger.warning(f"[{chain_name_for_log}] Rate limit hit on final attempt")
                            break

                    elif response.status == 503:
                        # Check if it's nginx error (common for blockscout)
                        response_text = await response.text()
                        if "nginx" in response_text.lower():
                            sleep_time = rate_limit_manager.calculate_backoff(attempt, base_wait=3.0)
                            if attempt < max_retries_for_chain - 1:
                                logger.debug(f"[{chain_name_for_log}] Nginx 503 error. Retrying in {sleep_time:.1f}s...")
                                await asyncio.sleep(sleep_time)
                                continue
                        _last_error_type = ErrorType.TRANSIENT

                    elif response.status == 500:
                        # 500 on Blockscout is usually contract-specific (their DB missing the record),
                        # not a service outage — don't record as circuit-breaker failure, just return None.
                        logger.debug(f"[{chain_name_for_log}] HTTP 500 for {url} (contract-specific, not service failure)")
                        return None

                    response.raise_for_status()
                    rate_limit_manager.record_success(service_name)
                    return await response.json()

            except Exception as e:
                _last_error_type = GracefulErrorHandler.categorize_error(e)
                GracefulErrorHandler.log_error_appropriately(service_name, e, attempt, max_retries_for_chain, f"{chain_name_for_log} - {url}")

                if not GracefulErrorHandler.should_retry(_last_error_type, attempt, max_retries_for_chain):
                    break

                if attempt < max_retries_for_chain - 1:
                    wait_time = rate_limit_manager.calculate_backoff(attempt)
                    await asyncio.sleep(wait_time)

        # Record a single circuit-breaker failure for this URL (not one per retry).
        rate_limit_manager.record_failure(service_name, _last_error_type)
        return None  # Failed after retries

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

    async def get_transactions(self, address: str, chain_id: int, limit: int = 20) -> List[Dict]:
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
                    'timestamp': tx.get('timeStamp'), # Etherscan uses timeStamp (string unix)
                    'from': tx.get('from')
                     # Minimal for now, expand if this path is actually hit for txs
                }
                if tx_data['timestamp'] and tx_data['timestamp'].isdigit():
                    tx_data['timestamp'] = datetime.fromtimestamp(int(tx_data['timestamp'])).isoformat()
           
            if tx_data: parsed_txs.append(tx_data)
       
        logger.debug(f"[{chain_name}] Found {len(parsed_txs)} transactions for {address}")
        return parsed_txs

    async def get_contract_info(self, address: str, chain_id: int) -> Dict:
        """Fetch contract metadata from Blockscout: name, is_verified, is_proxy, impl_address."""
        chain_conf = self.chain_config.get(chain_id)
        if not chain_conf:
            return {}
        chain_name = chain_conf["name"]
        logger.debug(f"[{chain_name}] Fetching contract info for {address}")

        data = await self._try_endpoints(chain_id, "{url_base}/addresses/{address}", address=address)
        if not data:
            return {}

        name = data.get('name') or data.get('contract_name') or ''
        is_verified = bool(data.get('is_verified'))
        is_proxy = bool(data.get('is_proxy'))
        impl_name = ''
        impl_address = ''
        if is_proxy:
            impl = data.get('implementations')
            if isinstance(impl, list) and impl:
                impl_name = impl[0].get('name', '')
                impl_address = impl[0].get('address', '')
            elif isinstance(impl, dict):
                impl_name = impl.get('name', '')
                impl_address = impl.get('address', '')

        return {
            'contract_name': impl_name or name,
            'is_verified': is_verified,
            'is_proxy': is_proxy,
            'impl_address': impl_address,
        }

    async def get_token_transfers(self, address: str, chain_id: int, limit: int = 20) -> list:
        """Fetch recent ERC20/NFT token transfers involving this address.

        Returns deduplicated list of {token_name, token_symbol, token_type} dicts.
        Used to enrich novel_tokens / common_tokens signals without relying on Tenderly traces.
        """
        chain_conf = self.chain_config.get(chain_id)
        if not chain_conf:
            return []

        data = await self._try_endpoints(
            chain_id,
            "{url_base}/addresses/{address}/token-transfers",
            address=address,
        )
        if not data:
            return []

        items = data.get('items') or []
        seen, result = set(), []
        for item in items[:limit]:
            if not isinstance(item, dict):
                continue
            token = item.get('token') or {}
            name = token.get('name') or ''
            symbol = token.get('symbol') or ''
            token_type = token.get('type') or ''
            if name and name not in seen:
                seen.add(name)
                result.append({'token_name': name, 'token_symbol': symbol, 'token_type': token_type})

        logger.debug(f"[{chain_conf['name']}] Got {len(result)} unique token transfers for {address}")
        return result

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

    async def get_address_logs(self, address: str, chain_id: int, limit: int = 50) -> List[Dict]:
        """Fetch recent logs emitted by this contract address.

        Returns a list of dicts with keys: topic0, decoded_name, emitting_address.
        Used to detect contract type from event signatures (AccessControl, ERC20, Bridge, etc.)
        """
        chain_conf = self.chain_config.get(chain_id)
        if not chain_conf:
            return []
        if chain_conf.get('skip_logs', False):
            return []

        chain_name = chain_conf['name']
        logger.debug(f"[{chain_name}] Fetching address logs for {address}")

        data = await self._try_endpoints(
            chain_id,
            "{url_base}/addresses/{address}/logs",
            address=address,
        )

        if not data:
            return []

        items = data.get('items') or []
        result = []
        for item in items[:limit]:
            if not isinstance(item, dict):
                continue
            topics = item.get('topics') or []
            topic0 = topics[0] if topics else None
            decoded = item.get('decoded') or {}
            result.append({
                'topic0': topic0,
                'decoded_name': decoded.get('method_call') or decoded.get('event_name'),
                'emitting_address': (
                    item['address']['hash']
                    if isinstance(item.get('address'), dict)
                    else item.get('address')
                ),
            })

        logger.debug(f"[{chain_name}] Got {len(result)} address logs for {address}")
        return result


class TenderlyAPI_Async:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.base_url = "https://api.tenderly.co/api/v1/public-contract"
        # Rate limit tracking
        self.rate_limit_remaining = None
        self.rate_limit_reset = None
        self.rate_limit_total = None
        self.last_rate_limit_update = 0
        self._rate_limit_lock = asyncio.Lock()  # Lock for rate limit updates

    def _update_rate_limits(self, response_headers):
        """Update rate limit information from response headers."""
        try:
            # Update rate limit information from headers
            self.rate_limit_remaining = int(response_headers.get('X-RateLimit-Remaining', 0))
            self.rate_limit_total = int(response_headers.get('X-RateLimit-Limit', 0))
            reset_time = response_headers.get('X-RateLimit-Reset')
            if reset_time:
                # Convert reset time to timestamp if it's not already
                try:
                    self.rate_limit_reset = int(reset_time)
                except ValueError:
                    # If it's in a different format (e.g., ISO date), convert appropriately
                    self.rate_limit_reset = int(time.time()) + 60  # Default to 1 minute if parsing fails
            self.last_rate_limit_update = time.time()
            
            logger.debug(f"[Tenderly] Rate limits updated - Remaining: {self.rate_limit_remaining}, "
                        f"Total: {self.rate_limit_total}, Reset: {self.rate_limit_reset}")
        except Exception as e:
            logger.debug(f"[Tenderly] Error updating rate limits: {e}")

    async def _check_rate_limit(self):
        """Check if we should wait due to rate limits."""
        async with self._rate_limit_lock:
            if self.rate_limit_remaining is not None and self.rate_limit_remaining <= 1:
                if self.rate_limit_reset:
                    wait_time = max(0, self.rate_limit_reset - int(time.time()))
                    if wait_time > 0:
                        logger.warning(f"[Tenderly] Rate limit near zero. Waiting {wait_time} seconds.")
                        await asyncio.sleep(wait_time + 1)  # Add 1 second buffer
                        # Reset our tracking after waiting
                        self.rate_limit_remaining = None
                        self.rate_limit_reset = None

    async def _make_request(self, url: str, method: str = "GET", **kwargs) -> Dict:
        """Make a rate-limited request to Tenderly API."""
        service_name = "tenderly"
        max_retries = 3
        
        # Check circuit breaker
        if not await rate_limit_manager.wait_if_needed(service_name):
            logger.debug(f"[Tenderly] Service unavailable: {rate_limit_manager.get_service_status(service_name)}")
            return {}
        
        for attempt in range(max_retries):
            try:
                # Check our internal rate limits before making request
                await self._check_rate_limit()
                
                async with self.session.request(method, url, timeout=20, **kwargs) as response:
                    # Update rate limit info from headers
                    self._update_rate_limits(response.headers)
                    
                    if response.status == 429:  # Rate limited
                        retry_after = int(response.headers.get("Retry-After", "60"))
                        rate_limit_manager.update_rate_limit(service_name, 0, 100, int(time.time()) + retry_after)
                        rate_limit_manager.record_failure(service_name, ErrorType.RATE_LIMIT)
                        
                        if attempt < max_retries - 1:
                            logger.info(f"[Tenderly] Rate limit hit. Waiting {retry_after}s...")
                            await asyncio.sleep(retry_after)
                            continue
                        else:
                            logger.warning(f"[Tenderly] Rate limit hit on final attempt")
                            break
                    
                    if response.status == 404:
                        logger.debug(f"[Tenderly] Resource not found: {url}")
                        rate_limit_manager.record_success(service_name)  # 404 is a successful response
                        return {}
                    
                    if response.status >= 500:
                        rate_limit_manager.record_failure(service_name, ErrorType.TRANSIENT)
                        wait_time = rate_limit_manager.calculate_backoff(attempt)
                        if attempt < max_retries - 1:
                            logger.debug(f"[Tenderly] Server error {response.status}. Retrying in {wait_time:.1f}s...")
                            await asyncio.sleep(wait_time)
                            continue
                    
                    response.raise_for_status()
                    rate_limit_manager.record_success(service_name)
                    return await response.json()
                    
            except Exception as e:
                error_type = GracefulErrorHandler.categorize_error(e)
                rate_limit_manager.record_failure(service_name, error_type)
                GracefulErrorHandler.log_error_appropriately(service_name, e, attempt, max_retries, url)
                
                if not GracefulErrorHandler.should_retry(error_type, attempt, max_retries):
                    if error_type in [ErrorType.AUTH, ErrorType.PERMANENT]:
                        raise  # Don't continue processing for auth/permanent errors
                    break
                
                if attempt < max_retries - 1:
                    wait_time = rate_limit_manager.calculate_backoff(attempt)
                    await asyncio.sleep(wait_time)
        
        raise Exception(f"[Tenderly] Failed after {max_retries} retries: {url}")

    async def get_transaction_contracts(self, tx_hash: str, chain_id: int) -> List[Dict]:
        """Fetch interacted contracts for a given transaction using Tenderly's API."""
        url = f"{self.base_url}/{chain_id}/tx/{tx_hash}/contracts"
        
        try:
            logger.debug(f"[Tenderly] Fetching contracts for tx {tx_hash} on chain {chain_id}")
            data = await self._make_request(url)
            
            contracts = []
            if isinstance(data, list):
                for contract in data:
                    if isinstance(contract, dict):
                        contract_info = {
                            'address': contract.get('address'),
                            'contract_name': contract.get('contract_name'),
                            'type': contract.get('type'),
                            'network_id': contract.get('network_id')
                        }
                        if contract_info['address']:  # Only add if we have an address
                            contracts.append(contract_info)
            
            logger.debug(f"[Tenderly] Found {len(contracts)} contracts for tx {tx_hash}")
            return contracts
            
        except Exception as e:
            logger.error(f"[Tenderly] Error fetching contracts for tx {tx_hash}: {e}")
            return []

    async def trace_transaction(self, tx_hash: str, chain_id: int) -> Dict:
        """Fetch full call trace for a transaction via Tenderly public API.

        Returns a summarized trace dict with: method, status, subcall_count,
        involved_contracts, gas_used, error (if any).
        """
        url = f"https://api.tenderly.co/api/v1/public-contract/{chain_id}/trace/{tx_hash}"

        try:
            logger.debug(f"[Tenderly] Tracing tx {tx_hash} on chain {chain_id}")
            async with self.session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=20),
                ssl=create_ssl_context()
            ) as response:
                if response.status == 404:
                    logger.debug(f"[Tenderly] Trace not found for tx {tx_hash}")
                    return {}
                if response.status != 200:
                    logger.debug(f"[Tenderly] Trace endpoint returned {response.status} for {tx_hash}")
                    return {}

                data = await response.json()

            # Summarize the trace for AI consumption
            return self._summarize_trace(data, tx_hash)

        except Exception as e:
            logger.debug(f"[Tenderly] Error tracing tx {tx_hash}: {e}")
            return {}

    async def get_transactions_range(
        self,
        address: str,
        chain_id: int,
        access_key: str,
        txcount_7d: int = 0,
        target_hashes: int = 15,
    ) -> list[str]:
        """Find recent direct transactions TO `address` using Tenderly Node RPC.

        Uses tenderly_getTransactionsRange to find top-level txs sent TO the
        contract in a recent block window. Block window is sized so that
        ~target_hashes txs are expected, based on txcount_7d activity.

        Returns a list of tx hashes (up to target_hashes).
        Requires TENDERLY_ACCESS_KEY — returns [] when not configured.
        """
        node_info = Config.CHAIN_TO_NODE.get(chain_id)
        if not node_info:
            logger.debug(f"[Tenderly] No node config for chain {chain_id}, skipping range search")
            return []

        network, block_time = node_info
        node_url = f"https://{network}.gateway.tenderly.co"
        node_headers = {"X-Access-Key": access_key, "Content-Type": "application/json"}

        async def _rpc(method: str, params: list):
            try:
                async with self.session.post(
                    node_url,
                    headers=node_headers,
                    json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
                    timeout=aiohttp.ClientTimeout(total=15),
                    ssl=create_ssl_context(),
                ) as resp:
                    if resp.status != 200:
                        logger.debug(f"[Tenderly] Node RPC {method} returned {resp.status}")
                        return None
                    return await resp.json()
            except Exception as e:
                logger.debug(f"[Tenderly] Node RPC {method} error: {e}")
                return None

        # 1. Get current block number
        bn_resp = await _rpc("eth_blockNumber", [])
        if not bn_resp or "result" not in bn_resp:
            return []
        current_block = int(bn_resp["result"], 16)

        # 2. Compute look-back window: enough blocks to expect target_hashes txs
        blocks_per_week = (7 * 24 * 3600) / block_time
        if txcount_7d > 0:
            tx_per_block = txcount_7d / blocks_per_week
            look_back = max(500, min(20000, int(target_hashes / tx_per_block * 3)))
        else:
            look_back = 5000  # default when no activity data

        from_block = max(0, current_block - look_back)
        logger.debug(
            f"[Tenderly] Range search {address} on {network}: "
            f"blocks {from_block}–{current_block} (look_back={look_back}, txcount_7d={txcount_7d})"
        )

        # 3. Call tenderly_getTransactionsRange — params: [from, to, fromBlock, toBlock]
        # Use zero address for 'from' to match any sender.
        resp = await _rpc(
            "tenderly_getTransactionsRange",
            [
                "0x0000000000000000000000000000000000000000",
                address,
                hex(from_block),
                "latest",
            ],
        )
        if not resp or "result" not in resp or not isinstance(resp["result"], list):
            logger.debug(f"[Tenderly] Range search returned no results for {address}")
            return []

        txs = resp["result"]
        hashes = [tx["hash"] for tx in txs if tx.get("hash")][:target_hashes]
        logger.info(f"[Tenderly] Range fallback found {len(hashes)} txs for {address} (from {len(txs)} total)")
        return hashes

    # External call types — real cross-contract calls
    _EXTERNAL_CALL_TYPES = {'CALL', 'STATICCALL', 'DELEGATECALL', 'CALLCODE', 'CREATE', 'CREATE2'}
    # JUMPDEST nodes carry Tenderly's decoded function_name + contract_name (internal dispatch)
    _DECODED_NODE_TYPES  = {'JUMPDEST'}
    # Selectors where the first ABI argument is a target address worth capturing
    _ADDR_FIRST_ARG_SELECTORS = {
        '0x095ea7b3',  # approve(address,uint256) → spender
        '0xa9059cbb',  # transfer(address,uint256) → recipient
    }

    def _summarize_trace(self, trace_data: Dict, tx_hash: str) -> Dict:
        """Extract protocol-level signals from a Tenderly trace for AI classification."""
        if not trace_data:
            return {}

        summary = {'tx_hash': tx_hash}

        call_trace = trace_data.get('call_trace')
        if not call_trace:
            return {}

        # Root-level info
        summary['gas_used'] = call_trace.get('gas_used')
        summary['error_message'] = call_trace.get('error_message') or call_trace.get('error')
        summary['status'] = 'error' if summary['error_message'] else 'success'

        # Who sent this transaction (the EOA or contract that initiated it)
        summary['caller'] = (call_trace.get('from') or '').lower()

        # Entry point: prefer decoded function_name, fall back to raw selector
        entry_fn    = call_trace.get('function_name') or ''
        entry_input = call_trace.get('input', '')
        summary['entry_method'] = entry_fn or (entry_input[:10] if entry_input else 'unknown')
        summary['call_type']    = call_trace.get('call_type', 'CALL')

        calls_seen: list[Dict] = []         # {contract, function, call_type, depth}
        contracts_seen: Dict[str, str] = {} # addr -> name

        def walk(node, depth=0):
            if not isinstance(node, dict) or depth > 10:
                return
            ct   = node.get('call_type', '')
            name = node.get('contract_name') or ''
            fn   = node.get('function_name') or ''
            addr = (node.get('to') or node.get('address') or '').lower()

            raw_input = node.get('input', '') or ''
            selector  = raw_input[:10] if len(raw_input) >= 10 else ''  # e.g. '0x3850c7bd'

            if ct in self._EXTERNAL_CALL_TYPES:
                if addr:
                    contracts_seen[addr] = name
                call_entry = {
                    'contract':  name or addr[:10] or '?',
                    'function':  fn,
                    'selector':  selector,
                    'call_type': ct,
                    'depth':     depth,
                }
                # Capture first address arg for high-signal selectors (e.g. approve spender)
                if selector in self._ADDR_FIRST_ARG_SELECTORS and len(raw_input) >= 74:
                    first_addr = '0x' + raw_input[34:74].lower()
                    if first_addr != '0x' + '0' * 40:
                        call_entry['_first_addr_arg'] = first_addr
                calls_seen.append(call_entry)
            elif ct in self._DECODED_NODE_TYPES and fn:
                # JUMPDEST with a decoded fn = Tenderly-decoded internal dispatch
                calls_seen.append({
                    'contract':  name or '?',
                    'function':  fn,
                    'selector':  selector,
                    'call_type': 'internal',
                    'depth':     depth,
                })

            for sub in (node.get('calls') or []):
                walk(sub, depth + 1)

        walk(call_trace)

        # Resolve first address args → human-readable key_arg (e.g. approve spender)
        for c in calls_seen:
            first_addr = c.pop('_first_addr_arg', '')
            if first_addr:
                resolved = contracts_seen.get(first_addr, '')
                c['key_arg'] = resolved if resolved else first_addr[:12]

        # Deduplicate (contract, function) pairs, preserving first-seen order
        seen_sigs: set = set()
        unique_calls: list[Dict] = []
        for c in calls_seen:
            sig = (c['contract'], c['function'])
            if sig not in seen_sigs:
                seen_sigs.add(sig)
                unique_calls.append(c)

        summary['protocol_calls'] = unique_calls[:25]
        summary['involved_contracts'] = [
            {'address': addr, 'name': name}
            for addr, name in contracts_seen.items()
        ][:15]

        return summary

class ContractDataProcessor_Async:
    def __init__(self, blockscout_api: BlockscoutAPI_Async, tenderly_api: TenderlyAPI_Async, supported_chains_map: Dict[str, int], chain_aliases_map: Dict[str, str], chain_explorer_config: Dict[int, Dict]):
        self.blockscout_api = blockscout_api
        self.tenderly_api = tenderly_api
        self.supported_chains_map = supported_chains_map
        self.chain_aliases_map = chain_aliases_map
        self.chain_explorer_config = chain_explorer_config # For validation
        
        # Keywords that indicate token-related contracts
        self.token_keywords = {
            'fiat', 'pool', 'pair', 'uniswap', 'usdc', 'usdt', 'dai', 'weth', 'wbtc', 
            'token', 'erc20', 'swap', 'liquidity', 'vault', 'farm', 'staking', 'lp'
        }
    
    def _contains_token_keywords(self, text: str) -> bool:
        """Check if text contains token-related keywords."""
        if not text:
            return False
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.token_keywords)

    def _calculate_personal_usage_percentage(self, transactions: List[Dict]) -> float:
        """
        Calculate the percentage of transactions coming from the same 'from' address.
        Returns the highest percentage for any single address.
        """
        if not transactions:
            return 0.0
        
        # Count transactions by 'from' address
        from_address_counts = {}
        valid_tx_count = 0
        
        for tx in transactions:
            from_addr = tx.get('from')
            if from_addr and isinstance(from_addr, str) and from_addr.startswith('0x'):
                from_addr_lower = from_addr.lower()
                from_address_counts[from_addr_lower] = from_address_counts.get(from_addr_lower, 0) + 1
                valid_tx_count += 1
        
        if valid_tx_count == 0:
            return 0.0
        
        # Find the highest count and calculate percentage
        max_count = max(from_address_counts.values()) if from_address_counts else 0
        personal_usage_percentage = (max_count / valid_tx_count) * 100
        
        logger.debug(f"Personal usage analysis: {max_count}/{valid_tx_count} transactions = {personal_usage_percentage:.1f}% from most frequent address")
        
        return round(personal_usage_percentage, 1)

    async def process_contract_data(self, contract_address: str, chain_name_str: str, fetch_logs: bool = True, tx_limit: int = 5, analyze_personal_usage: bool = False) -> Dict[str, Any]:
        """
        Fetches and processes blockchain data for a contract to extract name, tx methods, tokens, etc.
        Uses Tenderly as primary source and Blockscout as backup.
        
        Args:
            contract_address: The contract address to analyze
            chain_name_str: The blockchain name
            fetch_logs: Whether to fetch transaction logs
            tx_limit: Number of transactions to fetch (5 for basic, 20 for important)
            analyze_personal_usage: Whether to calculate personal usage percentage (only for important contracts)
            
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
            "blockscout_fetch_status": "No Transactions Found", # Keep field name for compatibility
            "transaction_summary_json": None,
            "last_activity_timestamp": None
        }
        
        # Only add personal_usage field if we're analyzing it
        if analyze_personal_usage:
            results["personal_usage"] = 0.0

        try:
            logger.debug(f"[ContractProcessor] Getting {tx_limit} transactions for {contract_address} on {final_chain_name} (ID: {chain_id})")
            
            # Primary: Try Tenderly first
            raw_transactions = []
            data_source = "None"
            
            logger.info(f"[ContractProcessor] Trying Tenderly API first for {contract_address}")
            try:
                # Get transactions from Tenderly using a mock transaction to get contracts
                # Note: Tenderly doesn't have a direct transaction list endpoint, so we'll need to adapt this
                # For now, let's try to get some recent transaction hashes and then analyze them
                
                # Since Tenderly doesn't have a direct "get transactions" endpoint, we'll fall back to Blockscout
                # but mark that we attempted Tenderly first
                logger.debug(f"[ContractProcessor] Tenderly doesn't provide transaction listing - falling back to Blockscout for transaction discovery")
                raise Exception("Tenderly transaction listing not available")
                
            except Exception as e:
                logger.info(f"[ContractProcessor] Tenderly primary failed for {contract_address}: {e}. Trying Blockscout backup...")
                
                # Backup: Use Blockscout for transaction discovery
                raw_transactions = await self.blockscout_api.get_transactions(contract_address, chain_id, limit=tx_limit)
                if raw_transactions:
                    data_source = "Blockscout"
                    logger.info(f"[ContractProcessor] Successfully got {len(raw_transactions)} transactions from Blockscout backup")
                else:
                    logger.warning(f"[ContractProcessor] Both Tenderly and Blockscout failed for {contract_address}")
            
            processed_transactions_with_logs = []
            if raw_transactions:
                results["blockscout_fetch_status"] = f"Partial Data ({data_source})" # Initial status if we have txs
                
                # Calculate personal usage percentage only if analyze_personal_usage is True
                if analyze_personal_usage:
                    personal_usage_percentage = self._calculate_personal_usage_percentage(raw_transactions)
                    results["personal_usage"] = personal_usage_percentage
                    logger.info(f"[ContractProcessor] Personal usage for {contract_address}: {personal_usage_percentage}%")
                
                # Now use Tenderly as primary for contract analysis on each transaction
                logger.info(f"[ContractProcessor] Using Tenderly as primary for contract analysis on {len(raw_transactions)} transactions")
                
                # Fetch contract interactions from Tenderly for each transaction (PRIMARY)
                tenderly_tasks = []
                for tx in raw_transactions:
                    if tx.get('hash'):
                        tenderly_tasks.append(self.tenderly_api.get_transaction_contracts(tx['hash'], chain_id))
                    else:
                        tenderly_tasks.append(asyncio.sleep(0, result=[])) # Placeholder
                
                tenderly_success = False
                all_tenderly_contracts = []
                
                if tenderly_tasks:
                    logger.debug(f"[ContractProcessor] Fetching Tenderly contract data for {len(tenderly_tasks)} transactions (PRIMARY).")
                    all_tenderly_contracts = await asyncio.gather(*tenderly_tasks, return_exceptions=True)
                    
                    # Check if we got useful data from Tenderly
                    for result in all_tenderly_contracts:
                        if isinstance(result, list) and len(result) > 0:
                            tenderly_success = True
                            break
                
                # Process transactions with Tenderly data or fall back to Blockscout logs
                for i, tx in enumerate(raw_transactions):
                    tx_copy = tx.copy() # Work with a copy
                    
                    # Add Tenderly contract data if available
                    if i < len(all_tenderly_contracts) and isinstance(all_tenderly_contracts[i], list):
                        tx_copy['tenderly_contracts'] = all_tenderly_contracts[i]
                    elif i < len(all_tenderly_contracts) and isinstance(all_tenderly_contracts[i], Exception):
                        logger.debug(f"[ContractProcessor] Tenderly error for tx {tx.get('hash')}: {all_tenderly_contracts[i]}")
                        tx_copy['tenderly_contracts'] = []
                    else:
                        tx_copy['tenderly_contracts'] = []
                    
                    # Fetch Blockscout logs as backup if needed
                    chain_conf = self.blockscout_api.chain_config.get(chain_id, {})
                    should_fetch_logs_for_chain = fetch_logs and not chain_conf.get("skip_logs", False)
                    
                    # PROTOTYPING: log fetching disabled to conserve Blockscout credits
                    # if should_fetch_logs_for_chain and (not tenderly_success or not tx_copy['tenderly_contracts']):
                    #     logger.debug(f"[ContractProcessor] Fetching Blockscout logs as backup for tx {tx.get('hash')}")
                    #     if tx.get('hash'):
                    #         try:
                    #             logs = await self.blockscout_api.get_transaction_logs(tx['hash'], chain_id)
                    #             tx_copy['logs'] = logs
                    #         except Exception as e:
                    #             logger.warning(f"[ContractProcessor] Error fetching Blockscout logs for tx {tx.get('hash')}: {e}")
                    #             tx_copy['logs'] = []
                    #     else:
                    #         tx_copy['logs'] = []
                    # else:
                    tx_copy['logs'] = []
                    
                    processed_transactions_with_logs.append(tx_copy)
           
            if not processed_transactions_with_logs:
                logger.info(f"[ContractProcessor] No transactions found for {contract_address} on {final_chain_name}.")
                # blockscout_fetch_status remains "No Transactions Found" as set by default
                return results 

            # ---- Process transactions for desired fields ----
            if tenderly_success:
                results["blockscout_fetch_status"] = f"Success (Tenderly Primary, {data_source} Discovery)"
            else:
                results["blockscout_fetch_status"] = f"Success ({data_source} Backup Only)"
            
            if processed_transactions_with_logs[0].get('timestamp'):
                results["last_activity_timestamp"] = processed_transactions_with_logs[0]['timestamp']

            # Extract contract name from transactions - check Tenderly data first, then Blockscout
            for tx in processed_transactions_with_logs:
                # Try Tenderly contract data first
                for tenderly_contract in tx.get('tenderly_contracts', []):
                    if isinstance(tenderly_contract, dict):
                        contract_name = tenderly_contract.get('contract_name')
                        contract_addr = tenderly_contract.get('address', '').lower()
                        if contract_name and contract_addr == contract_address.lower():
                            results["contract_name"] = contract_name
                            logger.info(f"[ContractProcessor] Found contract name '{contract_name}' from Tenderly for {contract_address}")
                            break
                
                if results["contract_name"]:
                    break
                
                # Fallback to Blockscout data
                if tx.get('to_contract_name') and tx.get('to', '').lower() == contract_address.lower():
                    results["contract_name"] = tx['to_contract_name']
                    logger.info(f"[ContractProcessor] Found contract name '{results['contract_name']}' from Blockscout for {contract_address}")
                    break
            
            tx_methods = set()
            for tx in processed_transactions_with_logs:
                if tx.get('method'): tx_methods.add(tx['method'])
                if tx.get('method_call'): tx_methods.add(tx['method_call'])
            if tx_methods: results["recent_tx_methods"] = ", ".join(sorted(list(tx_methods)))[:1024] # Airtable text limit

            involves_transfer_flag = False
            involved_tokens_set = set()
            
            # Check Tenderly data first for token information
            tenderly_found_tokens = False
            for tx in processed_transactions_with_logs:
                for tenderly_contract in tx.get('tenderly_contracts', []):
                    if isinstance(tenderly_contract, dict):
                        contract_name = tenderly_contract.get('contract_name', '')
                        contract_address_tenderly = tenderly_contract.get('address', '')
                        
                        # Check if contract name contains token-related keywords
                        if self._contains_token_keywords(contract_name):
                            involves_transfer_flag = True
                            tenderly_found_tokens = True
                        
                        # Add contract name if available, otherwise add address
                        if contract_name:
                            involved_tokens_set.add(contract_name)
                        elif contract_address_tenderly:
                            involved_tokens_set.add(contract_address_tenderly)
            
            # Fallback to Blockscout logs if Tenderly didn't find token info
            if not tenderly_found_tokens:
                logger.debug(f"[ContractProcessor] No token info from Tenderly, checking Blockscout logs for {contract_address}")
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
                    
                    # Include Tenderly contract data in summary
                    if "tenderly_contracts" in tx_sum_item and tx_sum_item["tenderly_contracts"]:
                        sanitized_tenderly = []
                        for contract in tx_sum_item["tenderly_contracts"][:3]:  # Limit to 3 contracts per tx
                            if isinstance(contract, dict):
                                sanitized_tenderly.append({
                                    "address": contract.get('address'),
                                    "contract_name": contract.get('contract_name'),
                                    "type": contract.get('type')
                                })
                        tx_sum_item["tenderly_contracts"] = sanitized_tenderly
                    
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
                    "transactions": sanitized_tx_summary,
                    "data_source_priority": "Tenderly Primary, Blockscout Backup"
                }
                
                # Only include personal usage in JSON if it was analyzed
                if analyze_personal_usage and "personal_usage" in results:
                    limited_tx_data_for_json["personal_usage_percentage"] = results["personal_usage"]
                
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
                                      contract_processor: ContractDataProcessor_Async,
                                      important_only: bool = False,
                                      github_only: bool = False
                                      ) -> Optional[Dict]:
    """
    Processes a single record:
    1. Fetches GitHub info.
    2. Fetches contract name and transaction data (methods, tokens, transfer flag).
    3. Optionally analyzes personal usage patterns if important_only is True.
    Returns a dictionary with data to update in Airtable, or None if processing fails.
    """
    record_id = record.get('id')
    fields = record.get('fields', {})
    contract_address = fields.get('address')
    origin_key_list = fields.get('origin_key') # It's a list of record IDs

    if not contract_address or not record_id:
        logger.warning(f"Skipping record due to missing address or ID: {record}")
        return None

    # Skip records that already have processed data
    contract_name = fields.get('contract_name')
    approve_status = fields.get('approve')
    github_found = fields.get('github_found')
    
    if contract_name or approve_status is True or github_found is True:
        logger.info(f"Skipping record {record_id} - already has processed data (contract_name: {bool(contract_name)}, approve: {approve_status}, github_found: {github_found})")
        return None

    # Resolve chain (simplified, assuming first origin_key is the one)
    chain_name_str = "base" # Default
    if origin_key_list and isinstance(origin_key_list, list) and len(origin_key_list) > 0:
        chain_ref_id = origin_key_list[0]
        chain_name_str = {}.get(chain_ref_id, "base")
    elif isinstance(origin_key_list, str): # Should not happen based on typical Airtable structure but good to handle
        chain_name_str = {}.get(origin_key_list, "base")

    mode_label = "important mode" if important_only else "basic mode"
    if github_only:
        mode_label = f"{mode_label}, github-only"
    logger.info(f"Processing record {record_id}: Address {contract_address} on chain {chain_name_str} ({mode_label})")
    
    results_to_update = {}

    async with semaphore: # Ensure only a limited number of these blocks run concurrently
        try:
            # 1. Fetch GitHub Info
            logger.debug(f"[{record_id}] Fetching GitHub info for {contract_address}...")
            github_found, repo_count = await github_api.search_contract_address(contract_address)
            results_to_update["github_found"] = github_found
            results_to_update["repo_count"] = repo_count
            logger.debug(f"[{record_id}] GitHub info: Found={github_found}, Count={repo_count}")

            # 2. Fetch Contract Name & Transaction Data (skip in GitHub-only mode)
            if not github_only:
                # Use different transaction limits and personal usage analysis based on mode
                if important_only:
                    logger.debug(f"[{record_id}] Processing contract data for {contract_address} on {chain_name_str} (important mode - 20 txs with personal usage analysis)...")
                    contract_data_results = await contract_processor.process_contract_data(
                        contract_address, chain_name_str, tx_limit=20, analyze_personal_usage=True
                    )
                else:
                    logger.debug(f"[{record_id}] Processing contract data for {contract_address} on {chain_name_str} (basic mode - 5 txs)...")
                    contract_data_results = await contract_processor.process_contract_data(
                        contract_address, chain_name_str, tx_limit=5, analyze_personal_usage=False
                    )
                
                results_to_update.update(contract_data_results) # Merge in all fields from contract_processor
                
                mode_info = f"Important: {important_only}, GH: {github_found}, Name: {results_to_update.get('contract_name')}, Status: {results_to_update.get('blockscout_fetch_status')}"
                if important_only and 'personal_usage' in results_to_update:
                    mode_info += f", Personal Usage: {results_to_update['personal_usage']}%"
            else:
                mode_info = f"Important: {important_only}, GH: {github_found}, On-chain: skipped"
            
            logger.info(f"[{record_id}] Processed {contract_address}. {mode_info}")
            return results_to_update
            
        except Exception as e:
            logger.error(f"Error processing record {record_id} ({contract_address}): {e}", exc_info=True)
            return None # Indicate failure for this record

# ===============================================
# MAIN EXECUTION
# ===============================================

def log_service_health_status():
    """Log the health status of all services"""
    services = ["airtable", "github", "tenderly"]
    # Add blockscout services for each chain
    for chain in ["ethereum", "optimism", "base", "arbitrum", "scroll"]:
        services.append(f"blockscout_{chain}")
    
    healthy_services = []
    degraded_services = []
    failed_services = []
    
    for service in services:
        status = rate_limit_manager.get_service_status(service)
        if "Healthy" in status:
            healthy_services.append(service)
        elif "Degraded" in status:
            degraded_services.append(f"{service} ({status})")
        elif "Circuit Open" in status:
            failed_services.append(f"{service} ({status})")
    
    if healthy_services:
        logger.info(f"Healthy services: {', '.join(healthy_services)}")
    if degraded_services:
        logger.warning(f"Degraded services: {', '.join(degraded_services)}")
    if failed_services:
        logger.error(f"Failed services: {', '.join(failed_services)}")

