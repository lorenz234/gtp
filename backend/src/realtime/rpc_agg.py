import logging
import time
import asyncio
import aiohttp
import redis.asyncio as aioredis
import os
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from src.db_connector import DbConnector

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("rt_backend")

# Constants
NATIVE_TRANSFER_GAS = 21000  # Standard gas for a native ETH transfer
ERC20_TRANSFER_GAS = 65000  # Standard gas for an ERC20 transfer
ETH_PRICE_UPDATE_INTERVAL = 300  # 5 minutes in seconds
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

# Redis constants
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)  # For AUTH if enabled
REDIS_STREAM_MAXLEN = 500

class BlockchainProcessor(ABC):
    """Abstract base class for blockchain processors."""
    
    @abstractmethod
    async def fetch_latest_block(self, client: Any, chain_name: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest block for this blockchain type."""
        pass
    
    @abstractmethod
    async def initialize_client(self, url: str) -> Any:
        """Initialize the client for this blockchain type."""
        pass
    
    @abstractmethod
    def supports_tx_costs(self) -> bool:
        """Whether this blockchain processor supports transaction cost calculations."""
        pass


class EVMProcessor(BlockchainProcessor):
    """Processor for EVM-based blockchains."""
    
    def __init__(self, backend: 'RtBackend'):
        self.backend = backend
    
    async def initialize_client(self, url: str) -> AsyncWeb3:
        """Initialize Web3 client for EVM chains."""
        w3 = AsyncWeb3(AsyncHTTPProvider(url, request_kwargs={"timeout": 5}))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return w3
    
    def supports_tx_costs(self) -> bool:
        return True
    
    async def fetch_latest_block(self, web3: AsyncWeb3, chain_name: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest block using web3.py for EVM chains."""
        try:
            block_number = await web3.eth.block_number
            block = await web3.eth.get_block(block_number, full_transactions=False)

            # Convert to dictionary
            block_dict = {
                "number": hex(block.number),
                "hash": block.hash.hex(),
                "transactions": [tx.hex() if isinstance(tx, bytes) else tx for tx in block.transactions],
                "timestamp": hex(block.timestamp),
                "gasUsed": hex(block.gasUsed),
                "gasLimit": hex(block.gasLimit),
                "baseFeePerGas": hex(block.baseFeePerGas) if hasattr(block, 'baseFeePerGas') else None,
                "size": hex(block.size) if hasattr(block, 'size') else None,
            }
            
            # Calculate transaction costs if base fee is available
            if hasattr(block, 'baseFeePerGas') and block.baseFeePerGas:
                base_fee_per_gas_gwei = block.baseFeePerGas / 1e9
                tx_cost_native = base_fee_per_gas_gwei * NATIVE_TRANSFER_GAS / 1e9
                tx_cost_erc20_transfer = base_fee_per_gas_gwei * ERC20_TRANSFER_GAS / 1e9
                
                await self.backend.update_eth_price()
                
                tx_cost_native_usd = tx_cost_native * self.backend.eth_price_usd if self.backend.eth_price_usd > 0 else None
                tx_cost_erc20_transfer_usd = tx_cost_erc20_transfer * self.backend.eth_price_usd if self.backend.eth_price_usd > 0 else None
                
                # Update chain data with cost info
                self.backend.chain_data[chain_name]["base_fee_gwei"] = base_fee_per_gas_gwei
                self.backend.chain_data[chain_name]["tx_cost_native"] = tx_cost_native
                self.backend.chain_data[chain_name]["tx_cost_erc20_transfer"] = tx_cost_erc20_transfer
                self.backend.chain_data[chain_name]["tx_cost_native_usd"] = tx_cost_native_usd
                self.backend.chain_data[chain_name]["tx_cost_erc20_transfer_usd"] = tx_cost_erc20_transfer_usd
            
            return block_dict
            
        except Exception as e:
            logger.error(f"Exception fetching EVM block from {chain_name}: {str(e)}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None


class StarknetProcessor(BlockchainProcessor):
    """Processor for Starknet blockchain."""
    
    def __init__(self, backend: 'RtBackend'):
        self.backend = backend
    
    async def initialize_client(self, url: str) -> str:
        """Return the URL as the 'client' for Starknet (we'll use backend's HTTP session)."""
        return url
    
    def supports_tx_costs(self) -> bool:
        return False  # Starknet has different gas model, skip tx costs for now
    
    async def fetch_latest_block(self, url: str, chain_name: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest block using Starknet JSON-RPC."""
        try:
            if not self.backend.http_session:
                logger.error("HTTP session not initialized")
                return None
            
            # Starknet JSON-RPC call to get latest block number
            payload = {
                "jsonrpc": "2.0",
                "method": "starknet_blockNumber",
                "params": [],
                "id": 1
            }
            
            async with self.backend.http_session.post(url, json=payload, timeout=10) as response:
                if response.status != 200:
                    logger.error(f"Failed to get Starknet block number from {chain_name}: HTTP {response.status}")
                    return None
                    
                result = await response.json()
                if "error" in result:
                    logger.error(f"Starknet RPC error for {chain_name}: {result['error']}")
                    return None
                    
                block_number = result.get("result")
                if block_number is None:
                    logger.error(f"No block number returned for {chain_name}")
                    return None
                    
                # Convert hex string to int if needed
                if isinstance(block_number, str):
                    block_number = int(block_number, 16) if block_number.startswith('0x') else int(block_number)
            
            # Get block details with transactions
            payload = {
                "jsonrpc": "2.0", 
                "method": "starknet_getBlockWithTxs",
                "params": [{"block_number": block_number}],
                "id": 2
            }
            
            async with self.backend.http_session.post(url, json=payload, timeout=10) as response:
                if response.status != 200:
                    logger.error(f"Failed to get Starknet block details from {chain_name}: HTTP {response.status}")
                    return None
                    
                result = await response.json()
                if "error" in result:
                    logger.error(f"Starknet block fetch error for {chain_name}: {result['error']}")
                    return None
                    
                block_data = result.get("result")
                if not block_data:
                    logger.error(f"No block data returned for {chain_name}")
                    return None
                
                # Extract transaction list - handle different possible formats
                transactions = []
                if "transactions" in block_data:
                    transactions = block_data["transactions"]
                elif "transaction_receipts" in block_data:
                    transactions = block_data["transaction_receipts"]
                
                # Get timestamp - try different field names
                timestamp = block_data.get("timestamp", 0)
                if timestamp == 0:
                    timestamp = block_data.get("block_timestamp", 0)
                    
                # Convert to our standard format
                block_dict = {
                    "number": hex(block_number),
                    "hash": str(block_data.get("block_hash", "0x0")),
                    "transactions": transactions,
                    "timestamp": hex(timestamp) if timestamp else "0x0",
                    "gasUsed": "N/A",  # Starknet doesn't use traditional gas
                    "gasLimit": "N/A",
                    "baseFeePerGas": None,
                    "size": None,
                }
                
                logger.debug(f"Fetched Starknet block {block_number} with {len(transactions)} transactions")
                return block_dict
                
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching Starknet block from {chain_name}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Exception fetching Starknet block from {chain_name}: {str(e)}")
            self.backend.chain_data[chain_name]["errors"] += 1
            return None


class RtBackend:
    """
    Real-time Backend for blockchain TPS monitoring.
    Handles connections to multiple blockchain types and calculates TPS.
    """

    def __init__(self):
        """Initialize RPC endpoints, clients, and Redis connection for each chain."""
        # init db
        self.db_connector = DbConnector()

        # RPC endpoints with their chain identifiers
        self.RPC_ENDPOINTS = self._initialize_rpc_endpoints()

        # Initialize blockchain processors
        self.processors = {
            "evm": EVMProcessor(self),
            "evm_custom_gas": EVMProcessor(self),  # EVM but skip tx costs
            "starknet": StarknetProcessor(self),
        }

        # Initialize clients for each endpoint (will be populated in initialize_async)
        self.blockchain_clients: Dict[str, Any] = {}

        # Initialize chain data storage
        self.chain_data: Dict[str, Dict[str, Any]] = {}
        self._initialize_chain_data()
        
        # Initialize ETH price tracking
        self.eth_price_usd: float = 0.0
        self.last_eth_price_update: Optional[float] = None
        self.eth_price_lock = asyncio.Lock()
        
        # Initialize HTTP session for API calls
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize Redis connection
        self.redis_client: Optional[aioredis.Redis] = None

    async def initialize_async(self):
        """Initialize components that require async operations."""
        self.http_session = aiohttp.ClientSession()
        
        # Initialize Redis connection
        try:
            # Configure Redis connection parameters
            redis_params = {
                "host": REDIS_HOST,
                "port": REDIS_PORT,
                "db": REDIS_DB,
                "decode_responses": True,
                "socket_keepalive": True,
                "socket_keepalive_options": {},
                "retry_on_timeout": True,
                "health_check_interval": 30,
            }
            
            # Add password if provided
            if REDIS_PASSWORD:
                redis_params["password"] = REDIS_PASSWORD
                
            # For GCP Memorystore, you might need connection pooling
            self.redis_client = aioredis.Redis(**redis_params)
            
            # Test the connection
            await self.redis_client.ping()
            logger.info(f"Connected to Redis successfully at {REDIS_HOST}:{REDIS_PORT}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {str(e)}")
            raise
        
        # Initialize blockchain clients
        await self._initialize_blockchain_clients()
        
        # Get initial ETH price
        await self.update_eth_price()

    async def close(self):
        """Clean up resources."""
        if self.http_session:
            await self.http_session.close()
        
        if self.redis_client:
            await self.redis_client.close()
            
        # Note: Starknet clients are just URLs, no cleanup needed
        # Only close actual HTTP clients (for EVM processors, Web3 handles this)

    def _initialize_rpc_endpoints(self) -> Dict[str, Dict[str, Any]]:
        """
        Initialize and return the RPC endpoint configurations.
        
        Returns:
            Dictionary with RPC endpoint configurations.
        """
        rpc_config = {
            "zksync_era": {"prep_script": "evm", "sleeper": 3},
            "mode": {"prep_script": "evm", "sleeper": 3},
            "base": {"prep_script": "evm", "sleeper": 3},
            "linea": {"prep_script": "evm", "sleeper": 3},
            "optimism": {"prep_script": "evm", "sleeper": 3},
            "ethereum": {"prep_script": "evm", "sleeper": 6},
            "blast": {"prep_script": "evm", "sleeper": 3},
            "scroll": {"prep_script": "evm", "sleeper": 3},
            "arbitrum": {"prep_script": "evm", "sleeper": 2},
            "unichain": {"prep_script": "evm", "sleeper": 3},
            "mantle": {"prep_script": "evm_custom_gas", "sleeper": 3},  # custom gas token
            "taiko": {"prep_script": "evm", "sleeper": 6},
            "manta": {"prep_script": "evm", "sleeper": 3},
            "redstone": {"prep_script": "evm", "sleeper": 3},
            "soneium": {"prep_script": "evm", "sleeper": 3},
            "celo": {"prep_script": "evm_custom_gas", "sleeper": 3},  # custom gas token
            "worldchain": {"prep_script": "evm", "sleeper": 3},
            "arbitrum_nova": {"prep_script": "evm", "sleeper": 3},
            "zircuit": {"prep_script": "evm", "sleeper": 3},
            "swell": {"prep_script": "evm", "sleeper": 3},
            "ink": {"prep_script": "evm", "sleeper": 3},
            # Non-EVM chains
            "starknet": {"prep_script": "starknet", "sleeper": 5},
        }

        for chain_name, config in rpc_config.items():
            url = self.db_connector.get_special_use_rpc(chain_name)

            if not url or url == "None" or url == "":
                logger.error(f"No RPC URL configured for {chain_name}, skipping initialization")
                continue
            rpc_config[chain_name]['url'] = url

        logger.info(f"Initialized a total of {len(rpc_config)} RPC endpoints")
        return rpc_config

    async def _initialize_blockchain_clients(self) -> None:
        """Initialize clients for all blockchain endpoints."""
        for chain_name, config in self.RPC_ENDPOINTS.items():
            try:
                prep_script = config["prep_script"]
                processor = self.processors.get(prep_script)
                
                if not processor:
                    logger.error(f"No processor found for prep_script: {prep_script} (chain: {chain_name})")
                    continue
                
                client = await processor.initialize_client(config["url"])
                self.blockchain_clients[chain_name] = client
                logger.info(f"Initialized {prep_script} client for {chain_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize client for {chain_name}: {str(e)}")
                # Continue with other chains even if one fails

    def _initialize_chain_data(self) -> None:
        """Initialize empty chain data structures for all endpoints."""
        for chain_name, config in self.RPC_ENDPOINTS.items():
            self.chain_data[chain_name] = {
                "last_block_number": None,
                "last_block_timestamp": None,
                "last_tx_count": 0,
                "tps": 0,
                "gas_used": 0,
                "blocks_processed": 0,
                "errors": 0,
                "last_updated": None,
                "tx_cost_native": None,
                "tx_cost_erc20_transfer": None,
                "tx_cost_native_usd": None,
                "tx_cost_erc20_transfer_usd": None,
                "block_history": [],
                "prep_script": config["prep_script"],
            }

    async def update_eth_price(self) -> None:
        """Update the ETH price from CoinGecko API."""
        current_time = time.time()
        
        async with self.eth_price_lock:
            if (self.last_eth_price_update is None or 
                current_time - self.last_eth_price_update >= ETH_PRICE_UPDATE_INTERVAL):
                
                logger.info("Updating ETH price from CoinGecko API")
                
                try:
                    if not self.http_session:
                        self.http_session = aiohttp.ClientSession()
                        
                    async with self.http_session.get(COINGECKO_API_URL) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'ethereum' in data and 'usd' in data['ethereum']:
                                self.eth_price_usd = float(data['ethereum']['usd'])
                                self.last_eth_price_update = current_time
                                logger.info(f"Updated ETH price: ${self.eth_price_usd:.2f}")
                            else:
                                logger.error("Invalid response format from CoinGecko API")
                        else:
                            logger.error(f"Failed to fetch ETH price: HTTP {response.status}")
                except Exception as e:
                    logger.error(f"Error updating ETH price: {str(e)}")

    async def publish_to_redis(self, chain_name: str, data: Dict[str, Any]) -> None:
        """Publish chain data to Redis stream."""
        if not self.redis_client:
            logger.error("Redis client not initialized")
            return
            
        try:
            stream_key = f"chain:{chain_name}"
            
            # Get prep script to determine what data to include
            prep_script = self.chain_data[chain_name]["prep_script"]
            
            # Base data that all chains have
            redis_data = {
                "timestamp": str(int(time.time() * 1000)),
                "block_number": str(data.get("block_number", 0)),
                "tps": str(data.get("tps", 0)),
                "tx_count": str(data.get("tx_count", 0)),
                "blocks_processed": str(data.get("blocks_processed", 0)),
                "errors": str(data.get("errors", 0)),
                "chain_type": prep_script,
            }
            
            # Add transaction cost data only for regular EVM chains (not custom gas)
            if prep_script == "evm":
                redis_data.update({
                    "gas_used": str(data.get("gas_used", 0)),
                    "base_fee_gwei": str(data.get("base_fee_gwei", 0)),
                    "tx_cost_native": str(data.get("tx_cost_native", 0)),
                    "tx_cost_erc20_transfer": str(data.get("tx_cost_erc20_transfer", 0)),
                    "tx_cost_native_usd": str(data.get("tx_cost_native_usd", 0)),
                    "tx_cost_erc20_transfer_usd": str(data.get("tx_cost_erc20_transfer_usd", 0)),
                })
            elif prep_script == "evm_custom_gas":
                # EVM with custom gas token - include gas data but not costs
                redis_data.update({
                    "gas_used": str(data.get("gas_used", 0)),
                    "base_fee_gwei": "N/A",
                    "tx_cost_native": "N/A",
                    "tx_cost_erc20_transfer": "N/A",
                    "tx_cost_native_usd": "N/A",
                    "tx_cost_erc20_transfer_usd": "N/A",
                })
            else:
                # Non-EVM chains (like Starknet) - minimal data
                redis_data.update({
                    "gas_used": "N/A",
                    "base_fee_gwei": "N/A",
                    "tx_cost_native": "N/A",
                    "tx_cost_erc20_transfer": "N/A",
                    "tx_cost_native_usd": "N/A",
                    "tx_cost_erc20_transfer_usd": "N/A",
                })
            
            await self.redis_client.xadd(
                stream_key,
                redis_data,
                maxlen=REDIS_STREAM_MAXLEN,
                approximate=True
            )
            
            logger.debug(f"Published {prep_script} data to Redis stream {stream_key}")
            
        except Exception as e:
            logger.error(f"Failed to publish to Redis stream for {chain_name}: {str(e)}")

    async def fetch_latest_block(self, chain_name: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest block for any blockchain type."""
        try:
            config = self.RPC_ENDPOINTS[chain_name]
            prep_script = config["prep_script"]
            
            processor = self.processors.get(prep_script)
            if not processor:
                logger.error(f"No processor found for {prep_script}")
                return None
            
            client = self.blockchain_clients.get(chain_name)
            if not client:
                logger.error(f"No client found for {chain_name}")
                return None
            
            return await processor.fetch_latest_block(client, chain_name)
            
        except Exception as e:
            logger.error(f"Exception fetching block from {chain_name}: {str(e)}")
            self.chain_data[chain_name]["errors"] += 1
            return None

    def calculate_tps(self, chain_name: str, current_block: Dict[str, Any]) -> float:
        """Calculate TPS for a chain based on the current block and last 3 blocks."""
        chain = self.chain_data[chain_name]
        
        current_block_number = int(current_block["number"], 16)
        current_timestamp = int(current_block["timestamp"], 16)
        tx_count = len(current_block["transactions"])
        gas_used = int(current_block["gasUsed"], 16) if current_block["gasUsed"] != "N/A" else 0
        
        # Check if this is a new block (prevent duplicates)
        if chain["last_block_number"] is not None and current_block_number <= chain["last_block_number"]:
            logger.debug(f"{chain_name}: Skipping duplicate/old block {current_block_number}")
            return chain["tps"]
        
        # Handle missed blocks with estimation
        blocks_missed = 0
        if chain["last_block_number"] is not None:
            blocks_missed = current_block_number - chain["last_block_number"] - 1
            if blocks_missed > 0:
                self._add_estimated_blocks(chain, blocks_missed, current_block_number, current_timestamp, tx_count)
                if blocks_missed > 5:
                    logger.warning(f"{chain_name}: Missed {blocks_missed} blocks between {chain['last_block_number']} and {current_block_number}")
        
        # Create block info for current block
        block_info = {
            "number": current_block_number,
            "timestamp": current_timestamp,
            "tx_count": tx_count,
            "gas_used": gas_used,
            "is_estimated": False
        }
        
        chain["block_history"].append(block_info)
        
        # Keep only last 3 blocks
        if len(chain["block_history"]) > 3:
            chain["block_history"] = chain["block_history"][-3:]
        
        # Calculate TPS based on available blocks
        tps = self._calculate_tps_from_history(chain["block_history"])
        
        # Update chain data
        self._update_chain_data(chain_name, current_block_number, current_timestamp, 
                                tx_count, tps, current_block)
        
        # Publish to Redis
        asyncio.create_task(self._publish_chain_update(chain_name, current_block_number, tx_count, gas_used, tps))
        
        return tps

    async def _publish_chain_update(self, chain_name: str, block_number: int, tx_count: int, gas_used: int, tps: float) -> None:
        """Publish chain update to Redis stream."""
        chain_data = self.chain_data[chain_name]
        
        publish_data = {
            "block_number": block_number,
            "tps": tps,
            "tx_count": tx_count,
            "gas_used": gas_used,
            "base_fee_gwei": chain_data.get("base_fee_gwei", 0),
            "tx_cost_native": chain_data.get("tx_cost_native", 0),
            "tx_cost_erc20_transfer": chain_data.get("tx_cost_erc20_transfer", 0),
            "tx_cost_native_usd": chain_data.get("tx_cost_native_usd", 0),
            "tx_cost_erc20_transfer_usd": chain_data.get("tx_cost_erc20_transfer_usd", 0),
            "blocks_processed": chain_data.get("blocks_processed", 0),
            "errors": chain_data.get("errors", 0)
        }
        
        await self.publish_to_redis(chain_name, publish_data)
        
    def _add_estimated_blocks(self, chain: Dict[str, Any], blocks_missed: int, 
                             current_block_number: int, current_timestamp: int, current_tx_count: int) -> None:
        """Add estimated blocks for missed blocks to maintain TPS calculation accuracy."""
        if len(chain["block_history"]) == 0:
            return
            
        last_block = chain["block_history"][-1]
        last_timestamp = last_block["timestamp"]
        last_tx_count = last_block["tx_count"]
        
        time_diff = current_timestamp - last_timestamp
        avg_block_time = time_diff / (blocks_missed + 1)
        avg_tx_per_block = (last_tx_count + current_tx_count) / 2
        avg_gas_per_block = (last_block["gas_used"] + 0) / 2
        
        for i in range(1, blocks_missed + 1):
            estimated_block = {
                "number": last_block["number"] + i,
                "timestamp": int(last_timestamp + (avg_block_time * i)),
                "tx_count": int(avg_tx_per_block),
                "gas_used": int(avg_gas_per_block),
                "is_estimated": True
            }
            
            chain["block_history"].append(estimated_block)
            logger.debug(f"{estimated_block['number']}: Estimated block with {estimated_block['tx_count']} tx")

    def _calculate_tps_from_history(self, block_history: List[Dict[str, Any]]) -> float:
        """Calculate TPS based on block history (up to last 3 blocks)."""
        if len(block_history) < 2:
            return 0
        
        if len(block_history) == 2:
            oldest_block = block_history[0]
            newest_block = block_history[1]
            
            time_diff = newest_block["timestamp"] - oldest_block["timestamp"]
            if time_diff <= 0:
                return 0
                
            total_tx = oldest_block["tx_count"] + newest_block["tx_count"]
            return total_tx / time_diff
            
        else:  # 3 blocks available
            oldest_block = block_history[0]
            newest_block = block_history[2]
            
            time_diff = newest_block["timestamp"] - oldest_block["timestamp"]
            if time_diff <= 0:
                return 0
                
            total_tx = sum(block["tx_count"] for block in block_history)
            
            estimated_blocks = sum(1 for block in block_history if block.get("is_estimated", False))
            if estimated_blocks > 0:
                logger.debug(f"TPS calculation using {estimated_blocks} estimated blocks out of {len(block_history)}")
            
            return total_tx / time_diff
        
    def _update_chain_data(self, chain_name: str, block_number: int, timestamp: int, 
                          tx_count: int, tps: float, current_block: Dict[str, Any]) -> None:
        """Update chain data with new block information."""
        chain = self.chain_data[chain_name]
        chain["last_block_number"] = block_number
        chain["last_block_timestamp"] = timestamp
        chain["last_tx_count"] = tx_count
        chain["blocks_processed"] += 1
        chain["tps"] = tps
        chain["last_updated"] = time.time()

    async def get_chain_streams_info(self) -> Dict[str, Any]:
        """Get information about all Redis streams for debugging purposes."""
        if not self.redis_client:
            return {}
            
        streams_info = {}
        
        for chain_name, config in self.RPC_ENDPOINTS.items():
            stream_key = f"chain:{chain_name}"
            
            try:
                stream_len = await self.redis_client.xlen(stream_key)
                
                latest_entry = None
                if stream_len > 0:
                    entries = await self.redis_client.xrevrange(stream_key, count=1)
                    if entries:
                        latest_entry = entries[0]
                
                streams_info[chain_name] = {
                    "stream_key": stream_key,
                    "length": stream_len,
                    "latest_entry": latest_entry,
                    "chain_type": config["prep_script"]
                }
                
            except Exception as e:
                logger.error(f"Error getting stream info for {chain_name}: {str(e)}")
                streams_info[chain_name] = {"error": str(e)}
                
        return streams_info

    async def process_chain(self, chain_name: str) -> None:
        """Process a single chain continuously."""
        logger.info(f"Starting processing for chain: {chain_name}")
        sleeper = self.RPC_ENDPOINTS[chain_name].get("sleeper", 3)
        
        while True:
            try:
                block = await self.fetch_latest_block(chain_name)
                
                if block:
                    tps = self.calculate_tps(chain_name, block)
                    
                    current_block_number = int(block['number'], 16)
                    last_logged_block = self.chain_data[chain_name].get('last_logged_block', None)
                    
                    if tps > 0 and current_block_number != last_logged_block:
                        chain_type = self.chain_data[chain_name]["prep_script"]
                        logger.info(f"{chain_name} ({chain_type}): Block {current_block_number}, TPS: {tps:.2f}, Tx: {len(block['transactions'])}")
                        self.chain_data[chain_name]["last_logged_block"] = current_block_number
                        
                await asyncio.sleep(sleeper)
                
            except Exception as e:
                logger.error(f"Error processing {chain_name}: {str(e)}")
                self.chain_data[chain_name]["errors"] += 1
                await asyncio.sleep(5)


async def main():
    """Main function to run the real-time blockchain TPS monitor."""
    logger.info("Starting Real-time Multi-Blockchain TPS Monitor")
    
    backend = RtBackend()
    
    try:
        await backend.initialize_async()
        
        # Check which clients were successfully initialized
        successful_chains = list(backend.blockchain_clients.keys())
        failed_chains = [name for name in backend.RPC_ENDPOINTS.keys() if name not in successful_chains]
        
        if failed_chains:
            logger.warning(f"Failed to initialize clients for: {', '.join(failed_chains)}")
        
        # Create tasks only for successfully initialized chains
        tasks = []
        for chain_name in successful_chains:
            task = asyncio.create_task(
                backend.process_chain(chain_name),
                name=f"process_{chain_name}"
            )
            tasks.append(task)
        
        if not tasks:
            logger.error("No chains successfully initialized! Exiting.")
            return
        
        logger.info(f"Started monitoring {len(tasks)} chains: {', '.join(successful_chains)}")
        
        # Print initial stream info
        streams_info = await backend.get_chain_streams_info()
        chain_types = {}
        for chain_name, info in streams_info.items():
            if chain_name in successful_chains:  # Only show successfully initialized chains
                chain_type = info.get("chain_type", "unknown")
                if chain_type not in chain_types:
                    chain_types[chain_type] = []
                chain_types[chain_type].append(chain_name)
        
        logger.info("Successfully initialized chains by type:")
        for chain_type, chains in chain_types.items():
            logger.info(f"  {chain_type}: {len(chains)} chains - {', '.join(chains)}")
        
        # Run all tasks concurrently
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
    finally:
        await backend.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        exit(1)