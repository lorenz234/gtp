import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set, Optional
from dataclasses import dataclass
import requests
import os

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

def send_discord_message(message, webhook_url=None):
    data = {"content": message}
    if webhook_url is None:
        webhook_url = os.getenv('DISCORD_COMMS')
    response = requests.post(webhook_url, json=data)

    # Check the response status code
    if response.status_code == 204:
        print("Message sent successfully")
    else:
        print(f"Error sending message: {response.text}")

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
    max_connections: int = 500  # Connection limit

    @classmethod
    def from_env(cls) -> 'ServerConfig':
        """Create config from environment variables."""
        return cls(
            redis_host=os.getenv("REDIS_HOST", cls.redis_host),
            redis_port=int(os.getenv("REDIS_PORT", cls.redis_port)),
            redis_db=int(os.getenv("REDIS_DB", cls.redis_db)),
            redis_password=os.getenv("REDIS_PASSWORD"),
            server_port=int(os.getenv("SERVER_PORT", cls.server_port)),
            max_connections=int(os.getenv("MAX_CONNECTIONS", cls.max_connections))
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

class RedisKeys:
    """Redis key constants."""
    GLOBAL_TPS_ATH = "global:tps:ath"
    GLOBAL_TPS_24H = "global:tps:24h_high"
    TPS_HISTORY_24H = "global:tps:history_24h"
    ATH_HISTORY = "global:tps:ath_history"
    
    @staticmethod
    def chain_stream(chain_name: str) -> str:
        return f"chain:{chain_name}"

class RedisSSEServer:
    """SSE Server that reads blockchain data from Redis streams."""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.redis_client: Optional[aioredis.Redis] = None
        self.connected_clients: Set[web.StreamResponse] = set()
        self.latest_data: Dict[str, Any] = {}
        self.global_metrics: Dict[str, Any] = {}
        
        # TPS tracking
        self.tps_ath: float = 0.0
        self.tps_ath_timestamp: str = ""
        self.tps_24h_high: float = 0.0
        self.tps_24h_high_timestamp: str = ""
        
        # Chain caching (Optimization #6)
        self._chain_cache: Optional[List[str]] = None
        self._chain_cache_time: Optional[datetime] = None
        self._cache_ttl = 60  # 1 minute

    # Safe parsing helpers (Optimization #4)
    def _safe_float(self, value, default=0.0):
        """Safely convert to float with fallback."""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value, default=0):
        """Safely convert to int with fallback."""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default
        
    async def initialize(self):
        """Initialize Redis connection and load existing TPS records."""
        try:
            redis_params = {
                "host": self.config.redis_host,
                "port": self.config.redis_port,
                "db": self.config.redis_db,
                "decode_responses": True,
                "socket_keepalive": True,
                "retry_on_timeout": True,
                "health_check_interval": 30,
            }
            
            if self.config.redis_password:
                redis_params["password"] = self.config.redis_password
                
            self.redis_client = aioredis.Redis(**redis_params)
            await self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
            
            await self._load_tps_records()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {str(e)}")
            raise
    
    async def _load_tps_records(self):
        """Load existing TPS ATH and 24hr high from Redis using pipeline (Optimization #1)."""
        try:
            # Use pipeline for batch operations
            pipe = self.redis_client.pipeline()
            pipe.hgetall(RedisKeys.GLOBAL_TPS_ATH)
            pipe.hgetall(RedisKeys.GLOBAL_TPS_24H)
            results = await pipe.execute()
            
            ath_data, h24_data = results
            
            if ath_data:
                self.tps_ath = self._safe_float(ath_data.get("value", 0))
                self.tps_ath_timestamp = ath_data.get("timestamp", "")
                logger.info(f"📈 Loaded TPS ATH: {self.tps_ath} (set at {self.tps_ath_timestamp})")
            
            if h24_data:
                self.tps_24h_high = self._safe_float(h24_data.get("value", 0))
                self.tps_24h_high_timestamp = h24_data.get("timestamp", "")
                logger.info(f"📊 Loaded 24hr TPS High: {self.tps_24h_high} (set at {self.tps_24h_high_timestamp})")
            
            await self._cleanup_24h_history()
            
        except Exception as e:
            logger.error(f"Error loading TPS records: {str(e)}")
    
    async def _cleanup_24h_history(self):
        """Remove TPS history entries older than 24 hours."""
        try:
            cutoff_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
            removed = await self.redis_client.zremrangebyscore(
                RedisKeys.TPS_HISTORY_24H, 0, cutoff_time
            )
            if removed > 0:
                logger.debug(f"🧹 Cleaned up {removed} old TPS history entries")
        except Exception as e:
            logger.error(f"Error cleaning up 24hr history: {str(e)}")

    async def _periodic_maintenance(self):
        """Combined maintenance: refresh 24h high and cleanup old data."""
        try:
            # Batch both operations in a single pipeline for efficiency
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - 24 * 60 * 60 * 1000
            cutoff_time = min_ts  # Same cutoff for both operations

            # Get 24h entries for refresh
            entries = await self.redis_client.zrangebyscore(
                RedisKeys.TPS_HISTORY_24H, min_ts, now_ms
            )

            # Find max TPS from valid entries
            max_tps = 0
            max_entry = None

            for raw in entries:
                try:
                    data = json.loads(raw)
                    tps = self._safe_float(data.get("tps", 0))
                    if tps > max_tps:
                        max_tps = tps
                        max_entry = data
                except Exception:
                    continue

            # Batch all Redis operations
            pipe = self.redis_client.pipeline()
            operations = []

            # Update 24h high if we found a max entry
            if max_entry:
                pipe.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                    "value": str(max_tps),
                    "timestamp": max_entry["timestamp"],
                    "timestamp_ms": max_entry["timestamp_ms"]
                })
                operations.append("24h refresh")

            # Cleanup old entries (older than 24h) - ONLY TPS history, NOT ATH
            pipe.zremrangebyscore(RedisKeys.TPS_HISTORY_24H, 0, cutoff_time)
            operations.append("24h cleanup")

            # Execute all operations at once
            results = await pipe.execute()
            
            # Update in-memory values if 24h high was refreshed
            if max_entry:
                self.tps_24h_high = max_tps
                self.tps_24h_high_timestamp = max_entry["timestamp"]
                logger.info(f"♻️ Refreshed 24h TPS high: {max_tps} TPS")

            # Log cleanup results
            if len(results) >= 2:
                removed_24h = results[-1]
                if removed_24h > 0:
                    logger.debug(f"🧹 Cleaned up {removed_24h} old TPS entries")

        except Exception as e:
            logger.error(f"Error in periodic maintenance: {str(e)}")
    
    def _extract_chain_breakdown(self, chain_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract TPS values for each active chain."""
        breakdown = {}
        for chain_name, data in chain_data.items():
            if isinstance(data, dict) and "tps" in data:
                tps_value = self._safe_float(data.get("tps", 0))  # OPTIMIZED: Safe parsing
                if tps_value > 0:
                    breakdown[chain_name] = tps_value
        return breakdown
    
    async def _store_tps_record(self, record: TPSRecord):
        """Store TPS record in Redis using pipeline (Optimization #1)."""
        try:
            history_entry = {
                "tps": str(record.value),
                "timestamp": record.timestamp,
                "timestamp_ms": str(record.timestamp_ms),
                "chain_breakdown": json.dumps(record.chain_breakdown),
                "total_chains": str(record.total_chains),
                "active_chains": str(record.active_chains),
                "is_ath": str(record.is_ath)
            }
            
            # Batch operations in pipeline
            pipe = self.redis_client.pipeline()
            
            if record.is_ath:
                pipe.zadd(
                    RedisKeys.ATH_HISTORY,
                    {json.dumps(history_entry): record.timestamp_ms}
                )
            
            pipe.zadd(
                RedisKeys.TPS_HISTORY_24H,
                {json.dumps(history_entry): record.timestamp_ms}
            )
            
            await pipe.execute()
            
        except Exception as e:
            logger.error(f"Error storing TPS record: {str(e)}")

    async def _update_tps_records(self, current_tps: float, timestamp: str, chain_data: Dict[str, Any]):
        """Update TPS ATH and 24hr high records if new highs are reached (Optimization #1)."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            chain_breakdown = self._extract_chain_breakdown(chain_data)

            is_new_ath = current_tps > self.tps_ath

            # Batch all Redis operations in one pipeline
            pipe = self.redis_client.pipeline()
            redis_operations = []

            if is_new_ath:
                old_ath = self.tps_ath
                self.tps_ath = current_tps
                self.tps_ath_timestamp = timestamp

                pipe.hset(RedisKeys.GLOBAL_TPS_ATH, mapping={
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                })
                redis_operations.append("ATH update")

                logger.info(f"🚀 NEW TPS ALL-TIME HIGH: {current_tps} TPS! (Previous: {old_ath})")
                
                ## send msg to Discord with ATH info
                ath_message = f"""
                🚀 NEW TPS ALL-TIME HIGH: {current_tps} TPS! (Previous: {old_ath})
                \nTimestamp: {timestamp}\nActive Chains: {len(chain_breakdown)}\nChain Breakdown: {json.dumps(chain_breakdown, indent=2)}
                """
                send_discord_message(ath_message)
                

            # Check if current TPS is the new 24h high or if cached 24h high is expired
            cached_24h_data = await self.redis_client.hgetall(RedisKeys.GLOBAL_TPS_24H)

            last_tps = self._safe_float(cached_24h_data.get("value", "0")) if cached_24h_data else 0
            last_ts_ms = self._safe_int(cached_24h_data.get("timestamp_ms", "0")) if cached_24h_data else 0

            if current_timestamp_ms - last_ts_ms > 24 * 60 * 60 * 1000:
                last_tps = 0  # Cache is outdated

            if current_tps > last_tps:
                self.tps_24h_high = current_tps
                self.tps_24h_high_timestamp = timestamp
                pipe.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                })
                redis_operations.append("24h high update")
                logger.info(f"📊 NEW 24HR TPS HIGH: {current_tps} TPS!")
                
                ath_message = f"""
                🚀 NEW TPS 24hr HIGH: {current_tps} TPS! (Previous: {self.tps_24h_high})
                \nTimestamp: {timestamp}\nActive Chains: {len(chain_breakdown)}\nChain Breakdown: {json.dumps(chain_breakdown, indent=2)}
                """
                send_discord_message(ath_message)
            else:
                self.tps_24h_high = last_tps
                self.tps_24h_high_timestamp = cached_24h_data.get("timestamp", timestamp)

            # Execute all batched operations
            if redis_operations:
                await pipe.execute()

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

            record_type = "ATH" if is_new_ath else "New global TPS"
            #logger.info(f"💾 Stored {record_type} history: {current_tps} TPS with {len(chain_breakdown)} chains")

            # Periodic cleanup (reduced frequency since we have dedicated maintenance loop)
            if current_timestamp_ms % (self.config.cleanup_interval_ms * 5) == 0:  # Every 5 minutes instead of 1
                await self._cleanup_24h_history()

        except Exception as e:
            logger.error(f"Error updating TPS records: {str(e)}")
            
    async def periodic_maintenance_loop(self):
        """Combined maintenance loop: refresh 24h high and cleanup old data every 5 minutes."""
        logger.info("Starting periodic maintenance loop (every 5 minutes)")
        while True:
            try:
                await self._periodic_maintenance()
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
            await asyncio.sleep(300)  # Every 5 minutes
   
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
    
    async def _get_all_chains(self) -> List[str]:
        """Get all chain names from Redis with caching (Optimization #6)."""
        now = datetime.now()
        
        # Use cache if valid
        if (self._chain_cache and self._chain_cache_time and 
            (now - self._chain_cache_time).total_seconds() < self._cache_ttl):
            return self._chain_cache
        
        try:
            chains = []
            cursor = 0
            
            while True:
                cursor, keys = await self.redis_client.scan(
                    cursor=cursor, 
                    match="chain:*", 
                    count=100
                )
                
                for key in keys:
                    if isinstance(key, bytes):
                        key = key.decode('utf-8')
                    chain_name = key.replace("chain:", "")
                    chains.append(chain_name)
                
                if cursor == 0:
                    break
            
            # Update cache
            self._chain_cache = chains
            self._chain_cache_time = now
            return chains
            
        except Exception as e:
            logger.error(f"Error getting chain keys: {str(e)}")
            return self._chain_cache or []  # Return cached version on error
    
    async def _get_latest_chain_data(self, chain_name: str) -> Dict[str, Any]:
        """Get latest data for a specific chain from Redis stream with safe parsing (Optimization #4)."""
        try:
            stream_key = RedisKeys.chain_stream(chain_name)
            entries = await self.redis_client.xrevrange(stream_key, count=1)
            
            if not entries:
                return {
                    "chain_name": chain_name,
                    "display_name": chain_name,
                    "tps": 0,
                    "error": "No data available"
                }
            
            entry_id, fields = entries[0]
            
            # Safe parsing for all numeric fields
            timestamp = self._safe_int(fields.get("timestamp", 0))
            
            return {
                "chain_name": chain_name,
                "display_name": fields.get("display_name", chain_name),
                "tps": self._safe_float(fields.get("tps", 0)),
                "timestamp": timestamp,
                "tx_cost_erc20_transfer": self._safe_float(fields.get("tx_cost_erc20_transfer", 0)),
                "tx_cost_erc20_transfer_usd": self._safe_float(fields.get("tx_cost_erc20_transfer_usd", 0)),
                "last_updated": datetime.fromtimestamp(timestamp / 1000).isoformat() if timestamp else ""
            }
            
        except Exception as e:
            logger.error(f"Error getting data for {chain_name}: {str(e)}")
            return {
                "chain_name": chain_name,
                "display_name": chain_name,
                "tps": 0,
                "error": str(e)
            }
    
    async def _get_all_chain_data(self) -> Dict[str, Any]:
        """Get latest data for all chains concurrently (Optimization #2)."""
        chains = await self._get_all_chains()
        
        # Fetch all chain data concurrently instead of sequentially
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
        """Calculate Layer 2 transaction cost metrics with safe parsing (Optimization #4)."""
        l2_data = []
        for name, data in chain_data.items():
            if name != "ethereum" and isinstance(data, dict):
                tps = self._safe_float(data.get("tps", 0))
                cost_usd = self._safe_float(data.get("tx_cost_erc20_transfer_usd", 0))
                cost_eth = self._safe_float(data.get("tx_cost_erc20_transfer", 0))
                
                if tps > 0 and cost_usd > 0:
                    l2_data.append((cost_usd, cost_eth, tps))
        
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
        """Calculate global metrics from chain data with safe parsing (Optimization #4)."""
        try:
            # Safe TPS metrics calculation
            tps_values = []
            for data in chain_data.values():
                if isinstance(data, dict):
                    tps = self._safe_float(data.get("tps", 0))
                    tps_values.append(tps)
            
            total_tps = sum(tps_values)
            highest_tps = max(tps_values, default=0)
            active_chains = sum(1 for tps in tps_values if tps > 0)
            
            # Ethereum metrics with safe parsing
            eth_data = chain_data.get("ethereum", {})
            ethereum_tx_cost_usd = self._safe_float(eth_data.get("tx_cost_erc20_transfer_usd"))
            ethereum_tx_cost_eth = self._safe_float(eth_data.get("tx_cost_erc20_transfer"))
            
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
    
    async def _broadcast_to_clients(self):
        """Broadcast updated data to all connected SSE clients with improved error handling (Optimization #3)."""
        if not self.connected_clients:
            return
        
        try:
            message = json.dumps({
                "type": "update",
                "data": self.latest_data,
                "global_metrics": self.global_metrics,
                "timestamp": datetime.now().isoformat()
            })
            
            sse_message = f"data: {message}\n\n"
            disconnected_clients = set()
            
            for client in self.connected_clients:
                try:
                    await client.write(sse_message.encode('utf-8'))
                except (ConnectionResetError, ConnectionAbortedError, OSError):  # OPTIMIZED: Added OSError
                    disconnected_clients.add(client)
                except Exception as e:
                    logger.warning(f"Error sending to client: {str(e)}")
                    disconnected_clients.add(client)
            
            # CRITICAL: Actually remove the dead connections to prevent memory leaks
            if disconnected_clients:
                self.connected_clients.difference_update(disconnected_clients)
                logger.info(f"Removed {len(disconnected_clients)} disconnected clients. {len(self.connected_clients)} remaining.")
                
        except Exception as e:
            logger.error(f"Error broadcasting to clients: {str(e)}")
    
    async def data_update_loop(self):
        """Background task that continuously updates data and broadcasts to clients."""
        logger.info(f"Starting data update loop with {self.config.update_interval}s interval")
        
        while True:
            try:
                await self._update_data()
                await self._broadcast_to_clients()
                await asyncio.sleep(self.config.update_interval)
            except Exception as e:
                logger.error(f"Error in data update loop: {str(e)}")
                await asyncio.sleep(5)
    
    async def sse_handler(self, request):
        """Handle SSE connection requests with connection limit (Optimization #5)."""
        # Check connection limit to prevent DoS
        if len(self.connected_clients) >= self.config.max_connections:
            logger.warning(f"Connection limit reached ({self.config.max_connections}), rejecting new connection")
            return web.Response(status=503, text="Too many connections")
        
        response = web.StreamResponse()
        response.headers.update({
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        })
        
        client_ip = request.remote
        logger.info(f"New SSE client connected from {client_ip}")
        
        try:
            await response.prepare(request)
            self.connected_clients.add(response)
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
            while True:
                await asyncio.sleep(self.config.heartbeat_interval)
                try:
                    await response.write(b": heartbeat\n\n")
                except Exception:
                    logger.info(f"Client {client_ip} disconnected during heartbeat")
                    break
                    
        except (ConnectionResetError, ConnectionAbortedError):
            logger.info(f"Client {client_ip} disconnected")
        except Exception as e:
            logger.error(f"Error in SSE handler: {str(e)}")
        finally:
            self.connected_clients.discard(response)
            logger.info(f"Client removed. Total clients: {len(self.connected_clients)}")
        
        return response
    
    async def health_handler(self, request):
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy",
            "connected_clients": len(self.connected_clients),
            "max_connections": self.config.max_connections,  # NEW: Show limit
            "total_chains": len(self.latest_data),
            "active_chains": self.global_metrics.get("active_chains", 0),
            "total_tps": self.global_metrics.get("total_tps", 0),
            "total_tps_ath": self.global_metrics.get("total_tps_ath", 0),
            "total_tps_24h_high": self.global_metrics.get("total_tps_24h_high", 0),
            "redis_connected": self.redis_client is not None,
            "chain_cache_active": self._chain_cache is not None,  # NEW: Cache status
            "timestamp": datetime.now().isoformat()
        })
    
    async def api_data_handler(self, request):
        """REST API endpoint to get current data."""
        return web.json_response({
            "data": self.latest_data,
            "global_metrics": self.global_metrics,
            "timestamp": datetime.now().isoformat()
        })
    
    async def history_handler(self, request):
        """REST API endpoint to get TPS history for charts."""
        try:
            # Get query parameters
            limit = int(request.query.get('limit', 40))  # Default to 40 events
            hours = int(request.query.get('hours', 24))   # Default to last 24 hours
            
            # Calculate time range
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - (hours * 60 * 60 * 1000)
            
            # Get TPS history from Redis (sorted by timestamp, most recent first)
            raw_entries = await self.redis_client.zrevrangebyscore(
                RedisKeys.TPS_HISTORY_24H, 
                now_ms, 
                min_ts, 
                start=0, 
                num=limit
            )
            
            # Parse and format the history data
            history = []
            for raw_entry in raw_entries:
                try:
                    data = json.loads(raw_entry)
                    
                    # # Get historical chain data for this timestamp to extract tx costs
                    # entry_timestamp_ms = self._safe_int(data.get("timestamp_ms", 0))
                    # eth_tx_cost_usd = None
                    # eth_tx_cost_eth = None
                    # l2_avg_cost_usd = None
                    # l2_avg_cost_eth = None
                    # l2_highest_cost_usd = None
                    
                    # # Fetch chain data around this timestamp to get tx costs
                    # if entry_timestamp_ms > 0:
                    #     historical_chain_data = await self._get_historical_chain_data_at_timestamp(entry_timestamp_ms)
                        
                    #     # Extract Ethereum costs
                    #     eth_data = historical_chain_data.get("ethereum", {})
                    #     if eth_data:
                    #         eth_tx_cost_usd = self._safe_float(eth_data.get("tx_cost_erc20_transfer_usd"))
                    #         eth_tx_cost_eth = self._safe_float(eth_data.get("tx_cost_erc20_transfer"))
                        
                    #     # Calculate L2 costs
                    #     l2_metrics = self._calculate_l2_metrics(historical_chain_data)
                    #     l2_avg_cost_usd = l2_metrics.get("avg_cost_usd")
                    #     l2_avg_cost_eth = l2_metrics.get("avg_cost_eth")
                    #     l2_highest_cost_usd = l2_metrics.get("highest_cost_usd")
                    
                    # Format the history entry
                    entry = {
                        "tps": self._safe_float(data.get("tps", 0)),
                        "timestamp": data.get("timestamp", ""),
                        #"timestamp_ms": entry_timestamp_ms,
                        "total_chains": self._safe_int(data.get("total_chains", 0)),
                        "active_chains": self._safe_int(data.get("active_chains", 0)),
                        "is_ath": data.get("is_ath", "false").lower() == "true",
                        #"ethereum_tx_cost_usd": eth_tx_cost_usd,
                        #"ethereum_tx_cost_eth": eth_tx_cost_eth,
                        #"layer2s_avg_cost_usd": l2_avg_cost_usd,
                        #"layer2s_avg_cost_eth": l2_avg_cost_eth,
                        #"layer2s_highest_cost_usd": l2_highest_cost_usd
                    }
                    
                    history.append(entry)
                    
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse history entry: {e}")
                    continue
            
            # Calculate some summary statistics
            if history:
                tps_values = [entry["tps"] for entry in history]
                # eth_cost_usd_values = [entry["ethereum_tx_cost_usd"] for entry in history if entry["ethereum_tx_cost_usd"] is not None]
                # l2_cost_usd_values = [entry["layer2s_avg_cost_usd"] for entry in history if entry["layer2s_avg_cost_usd"] is not None]
                
                avg_tps = sum(tps_values) / len(tps_values)
                max_tps = max(tps_values)
                min_tps = min(tps_values)
                
                # # Transaction cost statistics
                # avg_eth_cost_usd = sum(eth_cost_usd_values) / len(eth_cost_usd_values) if eth_cost_usd_values else None
                # max_eth_cost_usd = max(eth_cost_usd_values) if eth_cost_usd_values else None
                # min_eth_cost_usd = min(eth_cost_usd_values) if eth_cost_usd_values else None
                
                # avg_l2_cost_usd = sum(l2_cost_usd_values) / len(l2_cost_usd_values) if l2_cost_usd_values else None
                # max_l2_cost_usd = max(l2_cost_usd_values) if l2_cost_usd_values else None
                # min_l2_cost_usd = min(l2_cost_usd_values) if l2_cost_usd_values else None
            else:
                avg_tps = max_tps = min_tps = 0
                # avg_eth_cost_usd = max_eth_cost_usd = min_eth_cost_usd = None
                # avg_l2_cost_usd = max_l2_cost_usd = min_l2_cost_usd = None
            
            response_data = {
                "history": history,
                "summary": {
                    "total_events": len(history),
                    "time_range_hours": hours,
                    "avg_tps": round(avg_tps, 2),
                    "max_tps": round(max_tps, 2),
                    "min_tps": round(min_tps, 2),
                    "current_ath": round(self.tps_ath, 1),
                    "current_24h_high": round(self.tps_24h_high, 1),
                    # "ethereum_tx_costs": {
                    #     "avg_usd": round(avg_eth_cost_usd, 4) if avg_eth_cost_usd else None,
                    #     "max_usd": round(max_eth_cost_usd, 4) if max_eth_cost_usd else None,
                    #     "min_usd": round(min_eth_cost_usd, 4) if min_eth_cost_usd else None
                    # },
                    # "layer2s_tx_costs": {
                    #     "avg_usd": round(avg_l2_cost_usd, 4) if avg_l2_cost_usd else None,
                    #     "max_usd": round(max_l2_cost_usd, 4) if max_l2_cost_usd else None,
                    #     "min_usd": round(min_l2_cost_usd, 4) if min_l2_cost_usd else None
                    # }
                },
                "timestamp": datetime.now().isoformat()
            }
            
            logger.debug(f"Served history: {len(history)} events")
            return web.json_response(response_data)
            
        except ValueError as e:
            logger.warning(f"Invalid query parameters in history request: {e}")
            return web.json_response({
                "error": "Invalid query parameters",
                "message": str(e)
            }, status=400)
            
        except Exception as e:
            logger.error(f"Error in history handler: {str(e)}")
            return web.json_response({
                "error": "Internal server error",
                "message": "Failed to retrieve history data"
            }, status=500)
    
    # async def _get_historical_chain_data_at_timestamp(self, timestamp_ms: int) -> Dict[str, Any]:
    #     """Get chain data closest to a specific timestamp from Redis streams."""
    #     try:
    #         chains = await self._get_all_chains()
    #         chain_data = {}
            
    #         # For each chain, get data closest to the target timestamp
    #         for chain_name in chains:
    #             try:
    #                 stream_key = RedisKeys.chain_stream(chain_name)
                    
    #                 # Get entries around the target timestamp (within 5 minutes)
    #                 time_window = 5 * 60 * 1000  # 5 minutes in ms
    #                 min_time = timestamp_ms - time_window
    #                 max_time = timestamp_ms + time_window
                    
    #                 # Convert to Redis stream timestamp format (milliseconds-0)
    #                 min_stream_id = f"{min_time}-0"
    #                 max_stream_id = f"{max_time}-0"
                    
    #                 entries = await self.redis_client.xrange(stream_key, min_stream_id, max_stream_id, count=10)
                    
    #                 if entries:
    #                     # Find the entry closest to target timestamp
    #                     closest_entry = None
    #                     closest_diff = float('inf')
                        
    #                     for entry_id, fields in entries:
    #                         entry_timestamp = self._safe_int(fields.get("timestamp", 0))
    #                         time_diff = abs(entry_timestamp - timestamp_ms)
                            
    #                         if time_diff < closest_diff:
    #                             closest_diff = time_diff
    #                             closest_entry = fields
                        
    #                     if closest_entry:
    #                         chain_data[chain_name] = {
    #                             "chain_name": chain_name,
    #                             "display_name": closest_entry.get("display_name", chain_name),
    #                             "tps": self._safe_float(closest_entry.get("tps", 0)),
    #                             "tx_cost_erc20_transfer": self._safe_float(closest_entry.get("tx_cost_erc20_transfer", 0)),
    #                             "tx_cost_erc20_transfer_usd": self._safe_float(closest_entry.get("tx_cost_erc20_transfer_usd", 0)),
    #                             "timestamp": self._safe_int(closest_entry.get("timestamp", 0))
    #                         }
                    
    #             except Exception as e:
    #                 logger.debug(f"Error getting historical data for {chain_name}: {e}")
    #                 continue
            
    #         return chain_data
            
    #     except Exception as e:
    #         logger.error(f"Error getting historical chain data: {e}")
    #         return {}
        
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
        web.get("/api/history", server.history_handler),
    ]

    for route in routes:
        cors.add(app.router.add_route(route.method, route.path, route.handler))
    
    return app

async def main():
    """Main function to start the SSE server."""
    config = ServerConfig.from_env()
    server = RedisSSEServer(config)
    
    try:
        await server.initialize()
        app = await create_app(server)
        
        # Start background tasks
        update_task = asyncio.create_task(server.data_update_loop())
        maintenance_task = asyncio.create_task(server.periodic_maintenance_loop())  # Combined task
        
        # Start the web server
        logger.info(f"🚀 Starting SSE server on {config.server_host}:{config.server_port}")
        logger.info(f"📡 SSE endpoint: http://{config.server_host}:{config.server_port}/events")
        logger.info(f"🏥 Health check: http://{config.server_host}:{config.server_port}/health")
        logger.info(f"📈 API endpoint: http://{config.server_host}:{config.server_port}/api/data")
        logger.info(f"📊 History endpoint: http://{config.server_host}:{config.server_port}/api/history")
        logger.info(f"🔒 Max connections: {config.max_connections}")
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, config.server_host, config.server_port)
        await site.start()

        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("🛑 Received shutdown signal")
        finally:
            logger.info("🧹 Cleaning up...")
            update_task.cancel()
            maintenance_task.cancel()
            await server.close()
            await runner.cleanup()
            logger.info("✅ Shutdown complete")
            
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