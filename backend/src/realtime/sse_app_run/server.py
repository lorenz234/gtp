import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set, Optional
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import weakref

import redis.asyncio as aioredis
from aiohttp import web
import aiohttp_cors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("redis_sse_server")


@dataclass
class ServerConfig:
    """Server configuration settings."""
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    server_host: str = "0.0.0.0"
    server_port: int = 8080
    update_interval: int = 1
    heartbeat_interval: int = 30
    max_history_events: int = 20
    cleanup_interval_ms: int = 60000  # 1 minute
    # New optimizations
    redis_pool_size: int = 10
    redis_socket_timeout: int = 5
    max_concurrent_clients: int = 1000
    client_buffer_size: int = 8192

    @classmethod
    def from_env(cls) -> 'ServerConfig':
        """Create config from environment variables."""
        import os
        return cls(
            redis_host=os.getenv("REDIS_HOST", cls.redis_host),
            redis_port=int(os.getenv("REDIS_PORT", cls.redis_port)),
            redis_db=int(os.getenv("REDIS_DB", cls.redis_db)),
            redis_password=os.getenv("REDIS_PASSWORD"),
            server_port=int(os.getenv("SERVER_PORT", cls.server_port)),
            redis_pool_size=int(os.getenv("REDIS_POOL_SIZE", cls.redis_pool_size)),
            max_concurrent_clients=int(os.getenv("MAX_CLIENTS", cls.max_concurrent_clients))
        )


@dataclass
class TPSRecord:
    """TPS record data structure."""
    value: float
    timestamp: str
    timestamp_ms: int
    chain_breakdown: Dict[str, float]
    total_chains: int
    active_chains: int
    is_ath: bool = False
    
    def to_redis_dict(self) -> Dict[str, str]:
        """Convert to Redis-storable format."""
        return {
            "tps": str(self.value),
            "timestamp": self.timestamp,
            "timestamp_ms": str(self.timestamp_ms),
            "chain_breakdown": json.dumps(self.chain_breakdown),
            "total_chains": str(self.total_chains),
            "active_chains": str(self.active_chains),
            "is_ath": str(self.is_ath)
        }


@dataclass
class ClientConnection:
    """Client connection tracking."""
    response: web.StreamResponse
    connected_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    ip_address: str = ""


class RedisKeys:
    """Redis key constants."""
    GLOBAL_TPS_ATH = "global:tps:ath"
    GLOBAL_TPS_24H = "global:tps:24h_high"
    TPS_HISTORY_24H = "global:tps:history_24h"
    ATH_HISTORY = "global:tps:ath_history"
    CHAIN_LIST = "chains:active"  # New: cache active chain list
    
    @staticmethod
    def chain_stream(chain_name: str) -> str:
        return f"chain:{chain_name}"


class RedisSSEServer:
    """SSE Server that reads blockchain data from Redis streams."""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.redis_pool: Optional[aioredis.ConnectionPool] = None
        self.redis_client: Optional[aioredis.Redis] = None
        
        # Use WeakSet for automatic cleanup of dead connections
        self.connected_clients: Dict[str, ClientConnection] = {}
        self.client_counter = 0
        
        # Cache for frequently accessed data
        self._chain_list_cache: Optional[List[str]] = None
        self._chain_list_cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=1)
        
        # Data storage
        self.latest_data: Dict[str, Any] = {}
        self.global_metrics: Dict[str, Any] = {}
        
        # TPS tracking
        self.tps_ath: float = 0.0
        self.tps_ath_timestamp: str = ""
        self.tps_24h_high: float = 0.0
        self.tps_24h_high_timestamp: str = ""
        
        # Background tasks
        self._tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
    async def initialize(self):
        """Initialize Redis connection pool and load existing TPS records."""
        try:
            # Create connection pool for better resource management
            self.redis_pool = aioredis.ConnectionPool(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=self.config.redis_db,
                password=self.config.redis_password,
                max_connections=self.config.redis_pool_size,
                socket_timeout=self.config.redis_socket_timeout,
                socket_keepalive=True,
                retry_on_timeout=True,
                health_check_interval=30,
            )
            
            self.redis_client = aioredis.Redis(
                connection_pool=self.redis_pool,
                decode_responses=True
            )
            
            await self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
            
            await self._load_tps_records()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {str(e)}")
            raise
    
    @asynccontextmanager
    async def redis_transaction(self):
        """Context manager for Redis transactions with automatic cleanup."""
        pipe = self.redis_client.pipeline()
        try:
            yield pipe
            await pipe.execute()
        except Exception as e:
            logger.error(f"Redis transaction failed: {str(e)}")
            raise
        finally:
            await pipe.reset()
    
    async def _load_tps_records(self):
        """Load existing TPS ATH and 24hr high from Redis using pipeline."""
        try:
            # Use pipeline for batch operations
            pipe = self.redis_client.pipeline()
            pipe.hgetall(RedisKeys.GLOBAL_TPS_ATH)
            pipe.hgetall(RedisKeys.GLOBAL_TPS_24H)
            results = await pipe.execute()
            
            ath_data, h24_data = results
            
            if ath_data:
                self.tps_ath = float(ath_data.get("value", 0))
                self.tps_ath_timestamp = ath_data.get("timestamp", "")
                logger.info(f"📈 Loaded TPS ATH: {self.tps_ath} (set at {self.tps_ath_timestamp})")
            
            if h24_data:
                self.tps_24h_high = float(h24_data.get("value", 0))
                self.tps_24h_high_timestamp = h24_data.get("timestamp", "")
                logger.info(f"📊 Loaded 24hr TPS High: {self.tps_24h_high} (set at {self.tps_24h_high_timestamp})")
            
            await self._cleanup_24h_history()
            
        except Exception as e:
            logger.error(f"Error loading TPS records: {str(e)}")
    
    async def _cleanup_24h_history(self):
        """Remove TPS history entries older than 24 hours."""
        try:
            cutoff_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
            async with self.redis_transaction() as pipe:
                pipe.zremrangebyscore(RedisKeys.TPS_HISTORY_24H, 0, cutoff_time)
                pipe.zremrangebyscore(RedisKeys.ATH_HISTORY, 0, cutoff_time - (7 * 24 * 60 * 60 * 1000))  # Keep ATH for 7 days
                results = await pipe.execute()
                
            removed_24h, removed_ath = results
            if removed_24h > 0 or removed_ath > 0:
                logger.debug(f"🧹 Cleaned up {removed_24h} old TPS history, {removed_ath} old ATH entries")
        except Exception as e:
            logger.error(f"Error cleaning up history: {str(e)}")
    
    def _extract_chain_breakdown(self, chain_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract TPS values for each active chain."""
        return {
            chain_name: data.get("tps", 0)
            for chain_name, data in chain_data.items()
            if isinstance(data, dict) and data.get("tps", 0) > 0
        }
    
    async def _store_tps_record(self, record: TPSRecord):
        """Store TPS record in Redis using pipeline."""
        try:
            history_entry_json = json.dumps(record.to_redis_dict())
            
            async with self.redis_transaction() as pipe:
                if record.is_ath:
                    pipe.zadd(RedisKeys.ATH_HISTORY, {history_entry_json: record.timestamp_ms})
                
                pipe.zadd(RedisKeys.TPS_HISTORY_24H, {history_entry_json: record.timestamp_ms})
                
        except Exception as e:
            logger.error(f"Error storing TPS record: {str(e)}")

    async def _update_tps_records(self, current_tps: float, timestamp: str, chain_data: Dict[str, Any]):
        """Update TPS ATH and 24hr high records if new highs are reached."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            chain_breakdown = self._extract_chain_breakdown(chain_data)
            is_new_ath = current_tps > self.tps_ath

            # Batch all Redis operations
            updates = []
            
            if is_new_ath:
                old_ath = self.tps_ath
                self.tps_ath = current_tps
                self.tps_ath_timestamp = timestamp
                
                updates.append(('hset', RedisKeys.GLOBAL_TPS_ATH, {
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                }))
                
                logger.info(f"🚀 NEW TPS ALL-TIME HIGH: {current_tps} TPS! (Previous: {old_ath})")

            # Check 24h high more efficiently
            if current_tps > self.tps_24h_high or self._is_24h_cache_stale():
                self.tps_24h_high = current_tps
                self.tps_24h_high_timestamp = timestamp
                
                updates.append(('hset', RedisKeys.GLOBAL_TPS_24H, {
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                }))
                
                logger.info(f"📊 NEW 24HR TPS HIGH: {current_tps} TPS!")

            # Execute all updates in a single pipeline
            if updates:
                async with self.redis_transaction() as pipe:
                    for operation, key, data in updates:
                        getattr(pipe, operation)(key, mapping=data)

            # Store record
            record = TPSRecord(
                value=current_tps,
                timestamp=timestamp,
                timestamp_ms=current_timestamp_ms,
                chain_breakdown=chain_breakdown,
                total_chains=len(chain_data),
                active_chains=len(chain_breakdown),
                is_ath=is_new_ath
            )
            await self._store_tps_record(record)

        except Exception as e:
            logger.error(f"Error updating TPS records: {str(e)}")
    
    def _is_24h_cache_stale(self) -> bool:
        """Check if 24h cache might be stale."""
        return not self.tps_24h_high_timestamp or (
            datetime.now() - datetime.fromisoformat(self.tps_24h_high_timestamp.replace('Z', '+00:00'))
        ).total_seconds() > 24 * 3600
            
    async def _get_all_chains(self) -> List[str]:
        """Get all chain names from Redis with caching."""
        now = datetime.now()
        
        # Use cache if valid
        if (self._chain_list_cache and self._chain_list_cache_time and 
            now - self._chain_list_cache_time < self._cache_ttl):
            return self._chain_list_cache
        
        try:
            # Try to get from Redis cache first
            cached_chains = await self.redis_client.smembers(RedisKeys.CHAIN_LIST)
            if cached_chains:
                self._chain_list_cache = list(cached_chains)
                self._chain_list_cache_time = now
                return self._chain_list_cache
            
            # Fallback to key scan
            keys = await self.redis_client.keys("chain:*")
            chains = [key.replace("chain:", "") for key in keys]
            
            # Update cache in Redis
            if chains:
                await self.redis_client.sadd(RedisKeys.CHAIN_LIST, *chains)
                await self.redis_client.expire(RedisKeys.CHAIN_LIST, 300)  # 5 min TTL
            
            self._chain_list_cache = chains
            self._chain_list_cache_time = now
            return chains
            
        except Exception as e:
            logger.error(f"Error getting chain keys: {str(e)}")
            return self._chain_list_cache or []
    
    async def _get_latest_chain_data(self, chain_name: str) -> Dict[str, Any]:
        """Get latest data for a specific chain from Redis stream."""
        try:
            stream_key = RedisKeys.chain_stream(chain_name)
            entries = await self.redis_client.xrevrange(stream_key, count=1)
            
            if not entries:
                return {
                    "chain_name": chain_name,
                    "tps": 0,
                    "error": "No data available"
                }
            
            entry_id, fields = entries[0]
            
            # Parse numeric fields safely
            def safe_float(value, default=0.0):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            def safe_int(value, default=0):
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            timestamp = safe_int(fields.get("timestamp", 0))
            
            return {
                "chain_name": chain_name,
                "tps": safe_float(fields.get("tps", 0)),
                "timestamp": timestamp,
                "tx_cost_erc20_transfer": safe_float(fields.get("tx_cost_erc20_transfer", 0)),
                "tx_cost_erc20_transfer_usd": safe_float(fields.get("tx_cost_erc20_transfer_usd", 0)),
                "last_updated": datetime.fromtimestamp(timestamp / 1000).isoformat() if timestamp else ""
            }
            
        except Exception as e:
            logger.error(f"Error getting data for {chain_name}: {str(e)}")
            return {
                "chain_name": chain_name,
                "tps": 0,
                "error": str(e)
            }
    
    async def _get_all_chain_data(self) -> Dict[str, Any]:
        """Get latest data for all chains concurrently."""
        chains = await self._get_all_chains()
        
        # Fetch all chain data concurrently
        tasks = [self._get_latest_chain_data(chain) for chain in chains]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        chain_data = {}
        for chain_name, result in zip(chains, results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching data for {chain_name}: {result}")
                chain_data[chain_name] = {"chain_name": chain_name, "tps": 0, "error": str(result)}
            else:
                chain_data[chain_name] = result
        
        return chain_data
    
    def _calculate_l2_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Calculate Layer 2 transaction cost metrics."""
        l2_data = [
            (data.get("tx_cost_erc20_transfer_usd", 0), data.get("tx_cost_erc20_transfer", 0), data.get("tps", 0))
            for name, data in chain_data.items()
            if (name != "ethereum" and data.get("tps", 0) > 0 and data.get("tx_cost_erc20_transfer_usd", 0) > 0)
        ]
        
        if not l2_data:
            return {"avg_cost_usd": None, "avg_cost_eth": None, "highest_cost_usd": None}
        
        total_weighted_usd = sum(cost_usd * tps for cost_usd, _, tps in l2_data)
        total_weighted_eth = sum(cost_eth * tps for _, cost_eth, tps in l2_data)
        total_tps = sum(tps for _, _, tps in l2_data)
        
        return {
            "avg_cost_usd": total_weighted_usd / total_tps if total_tps > 0 else None,
            "avg_cost_eth": total_weighted_eth / total_tps if total_tps > 0 else None,
            "highest_cost_usd": max(cost_usd for cost_usd, _, _ in l2_data)
        }
    
    async def _calculate_global_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate global metrics from chain data."""
        try:
            # Basic TPS metrics
            tps_values = [
                data.get("tps", 0) for data in chain_data.values() 
                if isinstance(data.get("tps"), (int, float))
            ]
            
            total_tps = sum(tps_values)
            highest_tps = max(tps_values, default=0)
            active_chains = sum(1 for tps in tps_values if tps > 0)
            
            # Ethereum metrics
            eth_data = chain_data.get("ethereum", {})
            ethereum_tx_cost_usd = eth_data.get("tx_cost_erc20_transfer_usd")
            ethereum_tx_cost_eth = eth_data.get("tx_cost_erc20_transfer")
            
            # L2 metrics
            l2_metrics = self._calculate_l2_metrics(chain_data)
            
            # Update TPS records
            current_timestamp = datetime.now().isoformat()
            if total_tps > 0:
                await self._update_tps_records(total_tps, current_timestamp, chain_data)
            
            return {
                "total_tps": round(total_tps, 1),
                "highest_tps": round(highest_tps, 1),
                "highest_l2_cost_usd": l2_metrics["highest_cost_usd"],
                "total_chains": len(chain_data),
                "active_chains": active_chains,
                "ethereum_tx_cost_usd": ethereum_tx_cost_usd,
                "ethereum_tx_cost_eth": ethereum_tx_cost_eth,
                "layer2s_tx_cost_usd": l2_metrics["avg_cost_usd"],
                "layer2s_tx_cost_eth": l2_metrics["avg_cost_eth"],
                "total_tps_ath": round(self.tps_ath, 1),
                "total_tps_ath_timestamp": self.tps_ath_timestamp,
                "total_tps_24h_high": round(self.tps_24h_high, 1),
                "total_tps_24h_high_timestamp": self.tps_24h_high_timestamp,
                "last_updated": current_timestamp
            }
            
        except Exception as e:
            logger.error(f"Error calculating global metrics: {str(e)}")
            return {
                "total_tps": 0,
                "total_chains": 0,
                "active_chains": 0,
                "total_tps_ath": round(self.tps_ath, 1),
                "total_tps_ath_timestamp": self.tps_ath_timestamp,
                "total_tps_24h_high": round(self.tps_24h_high, 1),
                "total_tps_24h_high_timestamp": self.tps_24h_high_timestamp,
                "error": str(e)
            }
    
    async def _update_data(self):
        """Fetch latest data from Redis and update internal state."""
        try:
            chain_data = await self._get_all_chain_data()
            global_metrics = await self._calculate_global_metrics(chain_data)
            
            self.latest_data = chain_data
            self.global_metrics = global_metrics
            
            logger.debug(f"Updated data for {len(chain_data)} chains, total TPS: {global_metrics.get('total_tps', 0)}")
            
        except Exception as e:
            logger.error(f"Error updating data: {str(e)}")
    
    async def _cleanup_stale_clients(self):
        """Remove clients that haven't responded to heartbeats."""
        now = datetime.now()
        stale_clients = []
        
        for client_id, client in self.connected_clients.items():
            if (now - client.last_heartbeat).total_seconds() > self.config.heartbeat_interval * 2:
                stale_clients.append(client_id)
        
        for client_id in stale_clients:
            self.connected_clients.pop(client_id, None)
            logger.info(f"Removed stale client {client_id}")
    
    async def _broadcast_to_clients(self):
        """Broadcast updated data to all connected SSE clients."""
        if not self.connected_clients:
            return
        
        try:
            message = json.dumps({
                "type": "update",
                "data": self.latest_data,
                "global_metrics": self.global_metrics,
                "timestamp": datetime.now().isoformat()
            })
            
            sse_message = f"data: {message}\n\n".encode('utf-8')
            disconnected_clients = []
            
            # Send to all clients concurrently
            tasks = []
            for client_id, client in self.connected_clients.items():
                tasks.append(self._send_to_client(client_id, client, sse_message))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect disconnected clients
            for (client_id, _), result in zip(self.connected_clients.items(), results):
                if isinstance(result, Exception):
                    disconnected_clients.append(client_id)
            
            # Remove disconnected clients
            for client_id in disconnected_clients:
                self.connected_clients.pop(client_id, None)
            
            if disconnected_clients:
                logger.info(f"Removed {len(disconnected_clients)} disconnected clients. {len(self.connected_clients)} remaining.")
                
        except Exception as e:
            logger.error(f"Error broadcasting to clients: {str(e)}")
    
    async def _send_to_client(self, client_id: str, client: ClientConnection, message: bytes):
        """Send message to a single client."""
        try:
            await client.response.write(message)
            await client.response.drain()
            client.last_heartbeat = datetime.now()
        except (ConnectionResetError, ConnectionAbortedError, OSError):
            raise Exception(f"Client {client_id} disconnected")
        except Exception as e:
            raise Exception(f"Error sending to client {client_id}: {str(e)}")
    
    def _create_task(self, coro, name: str):
        """Create and track background tasks."""
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task
    
    async def data_update_loop(self):
        """Background task that continuously updates data and broadcasts to clients."""
        logger.info(f"Starting data update loop with {self.config.update_interval}s interval")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    await self._update_data()
                    await self._broadcast_to_clients()
                    
                    # Periodic cleanup
                    if len(self.connected_clients) > 0:
                        await self._cleanup_stale_clients()
                        
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=self.config.update_interval
                    )
                except asyncio.TimeoutError:
                    continue  # Normal timeout, continue loop
                except Exception as e:
                    logger.error(f"Error in data update loop: {str(e)}")
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("Data update loop cancelled")
        finally:
            logger.info("Data update loop stopped")
    
    async def periodic_refresh_loop(self):
        """Periodic refresh of 24h high data."""
        try:
            while not self._shutdown_event.is_set():
                try:
                    await self._refresh_24h_high()
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=300)  # 5 minutes
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error in refresh loop: {e}")
                    await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("Refresh loop cancelled")
    
    async def _refresh_24h_high(self):
        """Refresh the cached 24h TPS high from the 24h history."""
        try:
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - 24 * 60 * 60 * 1000

            entries = await self.redis_client.zrangebyscore(
                RedisKeys.TPS_HISTORY_24H, min_ts, now_ms
            )

            max_tps = 0
            max_entry = None

            for raw in entries:
                try:
                    data = json.loads(raw)
                    tps = float(data.get("tps", 0))
                    if tps > max_tps:
                        max_tps = tps
                        max_entry = data
                except Exception:
                    continue

            if max_entry:
                await self.redis_client.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                    "value": str(max_tps),
                    "timestamp": max_entry["timestamp"],
                    "timestamp_ms": max_entry["timestamp_ms"]
                })
                self.tps_24h_high = max_tps
                self.tps_24h_high_timestamp = max_entry["timestamp"]
                logger.info(f"♻️ Refreshed 24h TPS high: {max_tps} TPS")

        except Exception as e:
            logger.error(f"Error refreshing 24h TPS high: {str(e)}")
   
    async def close(self):
        """Close Redis connection and cleanup resources."""
        logger.info("🧹 Shutting down server...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Cancel all background tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close client connections
        for client in self.connected_clients.values():
            try:
                await client.response.write(b"event: shutdown\ndata: Server shutting down\n\n")
                client.response.force_close()
            except Exception:
                pass
        
        self.connected_clients.clear()
        
        # Close Redis connections
        if self.redis_client:
            await self.redis_client.close()
        if self.redis_pool:
            await self.redis_pool.disconnect()
    
    async def sse_handler(self, request):
        """Handle SSE connection requests with connection limits."""
        # Check connection limit
        if len(self.connected_clients) >= self.config.max_concurrent_clients:
            return web.Response(status=503, text="Too many connections")
        
        response = web.StreamResponse()
        response.headers.update({
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        })
        
        client_ip = request.remote
        client_id = f"{client_ip}_{self.client_counter}"
        self.client_counter += 1
        
        logger.info(f"New SSE client connected: {client_id}")
        
        try:
            await response.prepare(request)
            
            # Create client connection tracking
            client = ClientConnection(
                response=response,
                ip_address=client_ip
            )
            self.connected_clients[client_id] = client
            
            logger.info(f"Client added. Total clients: {len(self.connected_clients)}")
            
            # Send initial data
            initial_message = json.dumps({
                "type": "initial",
                "data": self.latest_data,
                "global_metrics": self.global_metrics,
                "timestamp": datetime.now().isoformat()
            })
            
            await response.write(f"data: {initial_message}\n\n".encode('utf-8'))
            
            # Keep connection alive with heartbeats
            while not self._shutdown_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=self.config.heartbeat_interval
                    )
                except asyncio.TimeoutError:
                    try:
                        await response.write(b": heartbeat\n\n")
                        await response.drain()
                        client.last_heartbeat = datetime.now()
                    except Exception:
                        logger.info(f"Client {client_id} disconnected during heartbeat")
                        break
                    
        except (ConnectionResetError, ConnectionAbortedError):
            logger.info(f"Client {client_id} disconnected")
        except Exception as e:
            logger.error(f"Error in SSE handler for {client_id}: {str(e)}")
        finally:
            self.connected_clients.pop(client_id, None)
            logger.info(f"Client {client_id} removed. Total clients: {len(self.connected_clients)}")
        
        return response
    
    async def health_handler(self, request):
        """Health check endpoint with more detailed information."""
        redis_healthy = False
        try:
            await self.redis_client.ping()
            redis_healthy = True
        except Exception:
            pass
        
        return web.json_response({
            "status": "healthy" if redis_healthy else "degraded",
            "connected_clients": len(self.connected_clients),
            "total_chains": len(self.latest_data),
            "active_chains": self.global_metrics.get("active_chains", 0),
            "total_tps": self.global_metrics.get("total_tps", 0),
            "total_tps_ath": self.global_metrics.get("total_tps_ath", 0),
            "total_tps_24h_high": self.global_metrics.get("total_tps_24h_high", 0),
            "redis_connected": redis_healthy,
            "cache_stats": {
                "chain_list_cached": self._chain_list_cache is not None,
                "cache_age_seconds": (
                    (datetime.now() - self._chain_list_cache_time).total_seconds()
                    if self._chain_list_cache_time else None
                )
            },
            "timestamp": datetime.now().isoformat()
        })
    
    async def api_data_handler(self, request):
        """REST API endpoint to get current data."""
        return web.json_response({
            "data": self.latest_data,
            "global_metrics": self.global_metrics,
            "timestamp": datetime.now().isoformat()
        })
    
    async def metrics_handler(self, request):
        """Prometheus-style metrics endpoint."""
        metrics = []
        
        # Add TPS metrics
        for chain_name, data in self.latest_data.items():
            if isinstance(data, dict) and "tps" in data:
                tps = data.get("tps", 0)
                metrics.append(f'blockchain_tps{{chain="{chain_name}"}} {tps}')
        
        # Add global metrics
        global_metrics = self.global_metrics
        metrics.extend([
            f'blockchain_total_tps {global_metrics.get("total_tps", 0)}',
            f'blockchain_total_chains {global_metrics.get("total_chains", 0)}',
            f'blockchain_active_chains {global_metrics.get("active_chains", 0)}',
            f'blockchain_tps_ath {global_metrics.get("total_tps_ath", 0)}',
            f'blockchain_tps_24h_high {global_metrics.get("total_tps_24h_high", 0)}',
            f'sse_connected_clients {len(self.connected_clients)}'
        ])
        
        return web.Response(text='\n'.join(metrics), content_type='text/plain')


async def create_app(server: RedisSSEServer):
    """Create and configure the aiohttp application."""
    app = web.Application()
    
    # Configure CORS
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
    })
    
    # Add routes
    routes = [
        web.get("/events", server.sse_handler),
        web.get("/health", server.health_handler),
        web.get("/api/data", server.api_data_handler),
        web.get("/metrics", server.metrics_handler),  # New metrics endpoint
    ]
    
    for route in routes:
        cors.add(app.router.add_route(route.method, route.path, route.handler))
    
    return app


async def graceful_shutdown(server: RedisSSEServer, runner: web.AppRunner):
    """Handle graceful shutdown."""
    logger.info("🛑 Received shutdown signal")
    
    try:
        # Stop accepting new connections
        await runner.cleanup()
        
        # Close server resources
        await server.close()
        
        logger.info("✅ Shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


async def main():
    """Main function to start the SSE server."""
    config = ServerConfig.from_env()
    server = RedisSSEServer(config)
    
    try:
        await server.initialize()
        app = await create_app(server)
        
        # Start background tasks
        server._create_task(server.data_update_loop(), "data_update_loop")
        server._create_task(server.periodic_refresh_loop(), "refresh_loop")
        
        # Start the web server
        logger.info(f"🚀 Starting SSE server on {config.server_host}:{config.server_port}")
        logger.info(f"📡 SSE endpoint: http://{config.server_host}:{config.server_port}/events")
        logger.info(f"🏥 Health check: http://{config.server_host}:{config.server_port}/health")
        logger.info(f"📈 API endpoint: http://{config.server_host}:{config.server_port}/api/data")
        logger.info(f"📊 Metrics endpoint: http://{config.server_host}:{config.server_port}/metrics")
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, config.server_host, config.server_port)
        await site.start()
        
        try:
            # Set up signal handlers for graceful shutdown
            import signal
            
            def signal_handler():
                logger.info("Received interrupt signal")
                server._shutdown_event.set()
            
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda s, f: signal_handler())
            
            # Wait for shutdown signal
            await server._shutdown_event.wait()
            
        finally:
            await graceful_shutdown(server, runner)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Server shutdown requested by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        exit(1)