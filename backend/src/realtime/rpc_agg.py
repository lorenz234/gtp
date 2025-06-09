import logging
import time
import asyncio
import aiohttp
import redis.asyncio as aioredis
from typing import Dict, List, Optional, Any

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from src.db_connector import DbConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("rt_backend")

# Constants
NATIVE_TRANSFER_GAS = 21000  # Standard gas for a native ETH transfer
ER20_TRANSFER_GAS = 65000  # Standard gas for an ERC20 transfer
ETH_PRICE_UPDATE_INTERVAL = 300  # 5 minutes in seconds
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

# Redis constants
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_STREAM_MAXLEN = 500


class RtBackend:
    """
    Real-time Backend for blockchain TPS monitoring.
    Handles connections to blockchain RPC endpoints, calculates TPS, and publishes to Redis streams.
    """

    def __init__(self):
        """Initialize RPC endpoints, Web3 clients, and Redis connection for each chain."""
        # init db
        self.db_connector = DbConnector()

        # RPC endpoints with their chain identifiers
        self.RPC_ENDPOINTS = self._initialize_rpc_endpoints()

        # Initialize web3 clients for each endpoint
        self.web3_clients: Dict[str, AsyncWeb3] = {}
        self._initialize_web3_clients()

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
            self.redis_client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True
            )
            # Test the connection
            await self.redis_client.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
        
        # Get initial ETH price
        await self.update_eth_price()

    async def close(self):
        """Clean up resources."""
        if self.http_session:
            await self.http_session.close()
        
        if self.redis_client:
            await self.redis_client.close()

    def _initialize_rpc_endpoints(self) -> List[Dict[str, str]]:
        """
        Initialize and return the list of RPC endpoints.
        Separated for easier maintenance and updating endpoints.
        
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
            # "mantle": {"prep_script": "evm_custom_gas", "sleeper": 3},  # custom gas token
            "taiko": {"prep_script": "evm", "sleeper": 6},
            "manta": {"prep_script": "evm", "sleeper": 3},
            "redstone": {"prep_script": "evm", "sleeper": 3},
            "soneium": {"prep_script": "evm", "sleeper": 3},
            # "celo": {"prep_script": "evm_custom_gas", "sleeper": 3},  # custom gas token
            "worldchain": {"prep_script": "evm", "sleeper": 3},
            "arbitrum_nova": {"prep_script": "evm", "sleeper": 3},
            "zircuit": {"prep_script": "evm", "sleeper": 3},
            "swell": {"prep_script": "evm", "sleeper": 3},
            "ink": {"prep_script": "evm", "sleeper": 3}
        }

        for chain_name, config in rpc_config.items():
            # Set the URL based on the chain name
            rpc_config[chain_name]['url'] = self.db_connector.get_special_use_rpc(chain_name)

        logger.info(f"Initialized a total of {len(rpc_config)} RPC endpoints")
        
        return rpc_config

    def _initialize_web3_clients(self) -> None:
        """Initialize Web3 clients for all RPC endpoints."""
        for chain_name, config in self.RPC_ENDPOINTS.items():
            try:
                w3 = AsyncWeb3(AsyncHTTPProvider(
                    self.RPC_ENDPOINTS[chain_name]["url"], 
                    request_kwargs={"timeout": 5}
                ))
                # Add PoA middleware for all chains to handle both PoA and non-PoA chains
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                self.web3_clients[chain_name] = w3
                logger.info(f"Initialized web3 client for {chain_name}")
            except Exception as e:
                logger.error(f"Failed to initialize web3 client for {chain_name}: {str(e)}")

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
                "block_history": [],  # Store last 3 blocks for TPS calculation
            }

    async def update_eth_price(self) -> None:
        """
        Update the ETH price from CoinGecko API.
        This method is rate-limited to run at most once every ETH_PRICE_UPDATE_INTERVAL seconds.
        """
        current_time = time.time()
        
        # Check if we need to update the price
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
        """
        Publish chain data to Redis stream.
        
        Args:
            chain_name: Name of the chain
            data: Dictionary containing the data to publish
        """
        if not self.redis_client:
            logger.error("Redis client not initialized")
            return
            
        try:
            stream_key = f"chain:{chain_name}"
            
            # Prepare the data for Redis stream
            redis_data = {
                "timestamp": str(int(time.time() * 1000)),  # milliseconds
                "block_number": str(data.get("block_number", 0)),
                "tps": str(data.get("tps", 0)),
                "tx_count": str(data.get("tx_count", 0)),
                "gas_used": str(data.get("gas_used", 0)),
                "base_fee_gwei": str(data.get("base_fee_gwei", 0)),
                "tx_cost_native": str(data.get("tx_cost_native", 0)),
                "tx_cost_erc20_transfer": str(data.get("tx_cost_erc20_transfer", 0)),
                "tx_cost_native_usd": str(data.get("tx_cost_native_usd", 0)),
                "tx_cost_erc20_transfer_usd": str(data.get("tx_cost_erc20_transfer_usd", 0)),
                "blocks_processed": str(data.get("blocks_processed", 0)),
                "errors": str(data.get("errors", 0))
            }
            
            # Add to stream with maxlen
            await self.redis_client.xadd(
                stream_key,
                redis_data,
                maxlen=REDIS_STREAM_MAXLEN,
                approximate=True  # Use approximate trimming for better performance
            )
            
            logger.debug(f"Published data to Redis stream {stream_key}")
            
        except Exception as e:
            logger.error(f"Failed to publish to Redis stream for {chain_name}: {str(e)}")

    async def fetch_latest_block(self, web3: AsyncWeb3, chain_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest block using web3.py.
        
        Args:
            web3: AsyncWeb3 instance for the chain
            chain_name: Name of the chain being queried
            
        Returns:
            Dictionary with block details or None if fetching failed
        """
        try:
            # Get latest block number
            block_number = await web3.eth.block_number
            
            # Get full block details
            block = await web3.eth.get_block(block_number, full_transactions=False)

            base_fee_per_gas_gwei = block.baseFeePerGas / 1e9
            
            # Convert to dictionary and handle any non-serializable objects
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
            
            # Calculate cost for a standard ETH transfer in native currency units (ETH)
            tx_cost_native = base_fee_per_gas_gwei * NATIVE_TRANSFER_GAS / 1e9
            tx_cost_erc20_transfer = base_fee_per_gas_gwei * ER20_TRANSFER_GAS / 1e9
            
            # Update ETH price if needed
            await self.update_eth_price()
            
            # Calculate USD cost
            tx_cost_native_usd = tx_cost_native * self.eth_price_usd if self.eth_price_usd > 0 else None
            tx_cost_erc20_transfer_usd = tx_cost_erc20_transfer * self.eth_price_usd if self.eth_price_usd > 0 else None
            
            # Update chain data
            self.chain_data[chain_name]["base_fee_gwei"] = base_fee_per_gas_gwei
            self.chain_data[chain_name]["tx_cost_native"] = tx_cost_native
            self.chain_data[chain_name]["tx_cost_erc20_transfer"] = tx_cost_erc20_transfer
            self.chain_data[chain_name]["tx_cost_native_usd"] = tx_cost_native_usd
            self.chain_data[chain_name]["tx_cost_erc20_transfer_usd"] = tx_cost_erc20_transfer_usd
            
            return block_dict
            
        except Exception as e:
            logger.error(f"Exception fetching block from {chain_name}: {str(e)}")
            self.chain_data[chain_name]["errors"] += 1
            return None

    def calculate_tps(self, chain_name: str, current_block: Dict[str, Any]) -> float:
        """
        Calculate TPS for a chain based on the current block and last 3 blocks.
        Only processes new blocks (prevents duplicates) and estimates missed blocks.
        
        Args:
            chain_name: Name of the chain
            current_block: Dictionary with current block details
            
        Returns:
            Float representing the calculated transactions per second
        """
        chain = self.chain_data[chain_name]
        
        current_block_number = int(current_block["number"], 16)
        current_timestamp = int(current_block["timestamp"], 16)
        tx_count = len(current_block["transactions"])
        gas_used = int(current_block["gasUsed"], 16)
        
        # Check if this is a new block (prevent duplicates)
        if chain["last_block_number"] is not None and current_block_number <= chain["last_block_number"]:
            logger.debug(f"{chain_name}: Skipping duplicate/old block {current_block_number} (last processed: {chain['last_block_number']})")
            return chain["tps"]  # Return existing TPS, don't process duplicate
        
        # Handle missed blocks with estimation
        blocks_missed = 0
        if chain["last_block_number"] is not None:
            blocks_missed = current_block_number - chain["last_block_number"] - 1
            if blocks_missed > 0:
                # Add estimated blocks for missed ones
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
        
        # Add current block to history
        chain["block_history"].append(block_info)
        
        # Keep only last 3 blocks
        if len(chain["block_history"]) > 3:
            chain["block_history"] = chain["block_history"][-3:]
        
        # Calculate TPS based on available blocks (including estimated ones)
        tps = self._calculate_tps_from_history(chain["block_history"])
        
        # Update chain data with the new block information
        self._update_chain_data(chain_name, current_block_number, current_timestamp, 
                                tx_count, tps, current_block)
        
        # Publish to Redis after updating chain data (only for new blocks)
        asyncio.create_task(self._publish_chain_update(chain_name, current_block_number, tx_count, gas_used, tps))
        
        return tps

    async def _publish_chain_update(self, chain_name: str, block_number: int, tx_count: int, gas_used: int, tps: float) -> None:
        """
        Publish chain update to Redis stream.
        
        Args:
            chain_name: Name of the chain
            block_number: Current block number
            tx_count: Number of transactions in current block
            gas_used: Gas used in current block
            tps: Calculated TPS value
        """
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
        """
        Add estimated blocks for missed blocks to maintain TPS calculation accuracy.
        
        Args:
            chain: Chain data dictionary
            blocks_missed: Number of blocks that were missed
            current_block_number: Current block number
            current_timestamp: Current block timestamp  
            current_tx_count: Transaction count in current block
        """
        if len(chain["block_history"]) == 0:
            # No previous blocks to base estimation on, skip estimation
            return
            
        last_block = chain["block_history"][-1]
        last_timestamp = last_block["timestamp"]
        last_tx_count = last_block["tx_count"]
        
        # Calculate average values for estimation
        time_diff = current_timestamp - last_timestamp
        avg_block_time = time_diff / (blocks_missed + 1)  # +1 includes current block
        avg_tx_per_block = (last_tx_count + current_tx_count) / 2
        avg_gas_per_block = (last_block["gas_used"] + 0) / 2  # Estimate gas (we don't have current gas here yet)
        
        # Add estimated blocks
        for i in range(1, blocks_missed + 1):
            estimated_block_number = last_block["number"] + i
            estimated_timestamp = int(last_timestamp + (avg_block_time * i))
            estimated_tx_count = int(avg_tx_per_block)
            estimated_gas_used = int(avg_gas_per_block)
            
            estimated_block = {
                "number": estimated_block_number,
                "timestamp": estimated_timestamp,
                "tx_count": estimated_tx_count,
                "gas_used": estimated_gas_used,
                "is_estimated": True
            }
            
            chain["block_history"].append(estimated_block)
            logger.debug(f"{estimated_block_number}: Estimated block with {estimated_tx_count} tx")

    def _calculate_tps_from_history(self, block_history: List[Dict[str, Any]]) -> float:
        """
        Calculate TPS based on block history (up to last 3 blocks).
        Handles both real and estimated blocks.
        
        Args:
            block_history: List of block info dictionaries
            
        Returns:
            Float representing the calculated transactions per second
        """
        if len(block_history) < 2:
            return 0  # Need at least 2 blocks to calculate TPS
        
        # Use all available blocks (up to 3) for calculation
        if len(block_history) == 2:
            # Only 2 blocks available
            oldest_block = block_history[0]
            newest_block = block_history[1]
            
            time_diff = newest_block["timestamp"] - oldest_block["timestamp"]
            if time_diff <= 0:
                return 0
                
            # Sum transactions from both blocks
            total_tx = oldest_block["tx_count"] + newest_block["tx_count"]
            return total_tx / time_diff
            
        else:  # 3 blocks available
            # Use all 3 blocks for more accurate calculation
            oldest_block = block_history[0]
            newest_block = block_history[2]
            
            time_diff = newest_block["timestamp"] - oldest_block["timestamp"]
            if time_diff <= 0:
                return 0
                
            # Sum transactions from all 3 blocks
            total_tx = sum(block["tx_count"] for block in block_history)
            
            # Log if we're using estimated data
            estimated_blocks = sum(1 for block in block_history if block.get("is_estimated", False))
            if estimated_blocks > 0:
                logger.debug(f"TPS calculation using {estimated_blocks} estimated blocks out of {len(block_history)}")
            
            return total_tx / time_diff
        
    def _update_chain_data(self, chain_name: str, block_number: int, timestamp: int, 
                          tx_count: int, tps: float, current_block: Dict[str, Any]) -> None:
        """
        Update chain data with new block information.
        
        Args:
            chain_name: Name of the chain
            block_number: Current block number
            timestamp: Current block timestamp
            tx_count: Number of transactions in current block
            tps: Calculated TPS value
            current_block: Full block data
        """
        chain = self.chain_data[chain_name]
        chain["last_block_number"] = block_number
        chain["last_block_timestamp"] = timestamp
        chain["last_tx_count"] = tx_count
        chain["blocks_processed"] += 1
        chain["tps"] = tps
        chain["last_updated"] = time.time()

    async def get_chain_streams_info(self) -> Dict[str, Any]:
        """
        Get information about all Redis streams for debugging purposes.
        
        Returns:
            Dictionary with stream information for each chain
        """
        if not self.redis_client:
            return {}
            
        streams_info = {}
        
        for chain_name, config in self.RPC_ENDPOINTS.items():
            stream_key = f"chain:{chain_name}"
            
            try:
                # Get stream length
                stream_len = await self.redis_client.xlen(stream_key)
                
                # Get latest entry if stream exists
                latest_entry = None
                if stream_len > 0:
                    entries = await self.redis_client.xrevrange(stream_key, count=1)
                    if entries:
                        latest_entry = entries[0]
                
                streams_info[chain_name] = {
                    "stream_key": stream_key,
                    "length": stream_len,
                    "latest_entry": latest_entry
                }
                
            except Exception as e:
                logger.error(f"Error getting stream info for {chain_name}: {str(e)}")
                streams_info[chain_name] = {"error": str(e)}
                
        return streams_info


    async def process_chain(self, chain_name: str, web3_client: AsyncWeb3) -> None:
        """
        Process a single chain continuously.
        
        Args:
            backend: RtBackend instance
            chain_name: Name of the chain to process
            web3_client: Web3 client for the chain
        """
        logger.info(f"Starting processing for chain: {chain_name}")
        sleeper = self.RPC_ENDPOINTS[chain_name].get("sleeper", 3) # Default sleeper if not specified
        
        while True:
            try:
                # Fetch latest block
                block = await self.fetch_latest_block(web3_client, chain_name)
                
                if block:
                    # Calculate TPS (this automatically publishes to Redis for new blocks only)
                    tps = self.calculate_tps(chain_name, block)
                    
                    # Only log if we have a meaningful TPS and it's a new block
                    current_block_number = int(block['number'], 16)
                    last_logged_block = self.chain_data[chain_name].get('last_block_number', None)
                    
                    if tps > 0 and current_block_number != last_logged_block:
                        #logger.info(f"{chain_name}: Block {current_block_number}, TPS: {tps:.2f}, Tx: {len(block['transactions'])}")
                        self.chain_data[chain_name]["last_block_number"] = current_block_number
                        
                # Wait before next iteration
                await asyncio.sleep(sleeper)
                
            except Exception as e:
                logger.error(f"Error processing {chain_name}: {str(e)}")
                self.chain_data[chain_name]["errors"] += 1
                await asyncio.sleep(5)  # Wait longer on error