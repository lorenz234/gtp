import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set, Optional
from dataclasses import dataclass, field
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
    try:
        response = requests.post(webhook_url, json=data, timeout=10)
        # Check the response status code
        if response.status_code == 204:
            print("Message sent successfully")
        else:
            print(f"Error sending message: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Discord message: {e}")


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
    cleanup_interval_ms: int = 60000
    max_connections: int = 500

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
    chain_breakdown: Dict[str, float] = field(default_factory=dict)
    total_chains: int = 0
    active_chains: int = 0
    is_ath: bool = False
    chain_name: Optional[str] = None

class RedisKeys:
    """Redis key constants."""
    # Global keys
    GLOBAL_TPS_ATH = "global:tps:ath"
    GLOBAL_TPS_24H = "global:tps:24h_high"
    TPS_HISTORY_24H = "global:tps:history_24h"
    ATH_HISTORY = "global:tps:ath_history"

    @staticmethod
    def chain_stream(chain_name: str) -> str:
        return f"chain:{chain_name}"

    # Chain-specific metric keys
    @staticmethod
    def chain_tps_ath(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:ath"

    @staticmethod
    def chain_tps_24h(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:24h_high"
    
    @staticmethod
    def chain_tps_history_24h(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:history_24h"
    
    @staticmethod
    def chain_ath_history(chain_name: str) -> str:
        return f"chain:{chain_name}:tps:ath_history"


class RedisSSEServer:
    """SSE Server that reads blockchain data from Redis streams."""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.redis_client: Optional[aioredis.Redis] = None
        self.connected_clients: Set[web.StreamResponse] = set()
        self.latest_data: Dict[str, Any] = {}
        self.global_metrics: Dict[str, Any] = {}

        # Client manager for chain-specific SSE streams
        self.chain_event_clients: Dict[str, Set[web.StreamResponse]] = {}
        
        # Global TPS tracking
        self.tps_ath: float = 0.0
        self.tps_ath_timestamp: str = ""
        self.tps_24h_high: float = 0.0
        self.tps_24h_high_timestamp: str = ""
        
        # Per-chain TPS tracking
        self.chain_metrics: Dict[str, Dict[str, Any]] = {}
        
        self._chain_cache: Optional[List[str]] = None
        self._chain_cache_time: Optional[datetime] = None
        self._cache_ttl = 60

    def _safe_float(self, value, default=0.0):
        try: return float(value) if value is not None else default
        except (ValueError, TypeError): return default

    def _safe_int(self, value, default=0):
        try: return int(value) if value is not None else default
        except (ValueError, TypeError): return default
        
    async def initialize(self):
        """Initialize Redis connection and load existing TPS records."""
        try:
            redis_params = {
                "host": self.config.redis_host,
                "port": self.config.redis_port,
                "db": self.config.redis_db,
                "decode_responses": True, "socket_keepalive": True,
                "retry_on_timeout": True, "health_check_interval": 30,
            }
            if self.config.redis_password:
                redis_params["password"] = self.config.redis_password
                
            self.redis_client = aioredis.Redis(**redis_params)
            await self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
            
            await self._load_global_tps_records()
            await self._load_all_chain_metrics()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {str(e)}")
            raise
    
    async def _load_global_tps_records(self):
        """Load existing GLOBAL TPS ATH and 24hr high from Redis."""
        try:
            pipe = self.redis_client.pipeline()
            pipe.hgetall(RedisKeys.GLOBAL_TPS_ATH)
            pipe.hgetall(RedisKeys.GLOBAL_TPS_24H)
            results = await pipe.execute()
            
            ath_data, h24_data = results
            
            if ath_data:
                self.tps_ath = self._safe_float(ath_data.get("value", 0))
                self.tps_ath_timestamp = ath_data.get("timestamp", "")
                logger.info(f"📈 Loaded Global TPS ATH: {self.tps_ath}")
            
            if h24_data:
                self.tps_24h_high = self._safe_float(h24_data.get("value", 0))
                self.tps_24h_high_timestamp = h24_data.get("timestamp", "")
                logger.info(f"📊 Loaded Global 24hr TPS High: {self.tps_24h_high}")
            
            await self._cleanup_24h_history()
            
        except Exception as e:
            logger.error(f"Error loading global TPS records: {str(e)}")

    async def _load_all_chain_metrics(self):
        """Load ATH and 24h high for all chains from Redis concurrently."""
        logger.info("Loading historical metrics for all chains...")
        chains = await self._get_all_chains()
        
        async def fetch_metrics(chain_name):
            try:
                pipe = self.redis_client.pipeline()
                pipe.hgetall(RedisKeys.chain_tps_ath(chain_name))
                pipe.hgetall(RedisKeys.chain_tps_24h(chain_name))
                ath_data, h24_data = await pipe.execute()
                
                self.chain_metrics[chain_name] = {
                    "ath": self._safe_float(ath_data.get("value", 0)) if ath_data else 0,
                    "ath_timestamp": ath_data.get("timestamp", "") if ath_data else "",
                    "24h_high": self._safe_float(h24_data.get("value", 0)) if h24_data else 0,
                    "24h_high_timestamp": h24_data.get("timestamp", "") if h24_data else ""
                }
            except Exception as e:
                logger.warning(f"Could not load metrics for chain {chain_name}: {e}")

        tasks = [fetch_metrics(chain) for chain in chains]
        await asyncio.gather(*tasks)
        logger.info(f"✅ Loaded metrics for {len(self.chain_metrics)} chains.")
    
    async def _cleanup_24h_history(self):
        """Remove TPS history entries older than 24 hours for global and all chains."""
        try:
            cutoff_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
            
            pipe = self.redis_client.pipeline()
            # Global cleanup
            pipe.zremrangebyscore(RedisKeys.TPS_HISTORY_24H, 0, cutoff_time)
            
            # Per-chain cleanup
            chains = await self._get_all_chains()
            for chain_name in chains:
                pipe.zremrangebyscore(RedisKeys.chain_tps_history_24h(chain_name), 0, cutoff_time)

            results = await pipe.execute()
            removed_global = results[0]
            if removed_global > 0:
                logger.debug(f"🧹 Cleaned up {removed_global} old GLOBAL TPS history entries")

        except Exception as e:
            logger.error(f"Error cleaning up 24hr history: {str(e)}")

    async def _periodic_maintenance(self):
        """Combined maintenance: refresh 24h high and cleanup old data."""
        try:
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - 24 * 60 * 60 * 1000
            
            # Refresh Global 24h High
            entries = await self.redis_client.zrangebyscore(
                RedisKeys.TPS_HISTORY_24H, min_ts, now_ms
            )
            max_tps = 0
            max_entry = None
            for raw in entries:
                try:
                    data = json.loads(raw)
                    tps = self._safe_float(data.get("tps", 0))
                    if tps > max_tps:
                        max_tps = tps
                        max_entry = data
                except Exception: continue

            if max_entry:
                await self.redis_client.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                    "value": str(max_tps), "timestamp": max_entry["timestamp"],
                    "timestamp_ms": max_entry["timestamp_ms"]
                })
                self.tps_24h_high = max_tps
                self.tps_24h_high_timestamp = max_entry["timestamp"]

            # Cleanup
            await self._cleanup_24h_history()
            
            logger.info("♻️ Periodic maintenance complete (24h high refresh & history cleanup).")

        except Exception as e:
            logger.error(f"Error in periodic maintenance: {str(e)}")
    
    def _extract_chain_breakdown(self, chain_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract TPS values for each active chain."""
        breakdown = {}
        for chain_name, data in chain_data.items():
            if isinstance(data, dict) and "tps" in data:
                tps_value = self._safe_float(data.get("tps", 0))
                if tps_value > 0:
                    breakdown[chain_name] = tps_value
        return breakdown
    
    async def _store_global_tps_record(self, record: TPSRecord):
        """Store a GLOBAL TPS record in Redis."""
        try:
            history_entry = {
                "tps": record.value, "timestamp": record.timestamp,
                "timestamp_ms": record.timestamp_ms,
                "chain_breakdown": record.chain_breakdown,
                "total_chains": record.total_chains, "active_chains": record.active_chains,
                "is_ath": record.is_ath
            }
            pipe = self.redis_client.pipeline()
            entry_json = json.dumps(history_entry)
            if record.is_ath:
                pipe.zadd(RedisKeys.ATH_HISTORY, {entry_json: record.timestamp_ms})
            pipe.zadd(RedisKeys.TPS_HISTORY_24H, {entry_json: record.timestamp_ms})
            await pipe.execute()
        except Exception as e:
            logger.error(f"Error storing global TPS record: {str(e)}")
    
    async def _store_chain_tps_record(self, record: TPSRecord):
        """Store a CHAIN-SPECIFIC TPS record in Redis."""
        if not record.chain_name:
            return
        try:
            history_entry = {
                "tps": record.value, "timestamp": record.timestamp,
                "timestamp_ms": record.timestamp_ms, "is_ath": record.is_ath
            }
            pipe = self.redis_client.pipeline()
            key_history = RedisKeys.chain_tps_history_24h(record.chain_name)
            entry_json = json.dumps(history_entry)
            
            if record.is_ath:
                key_ath_history = RedisKeys.chain_ath_history(record.chain_name)
                pipe.zadd(key_ath_history, {entry_json: record.timestamp_ms})

            pipe.zadd(key_history, {entry_json: record.timestamp_ms})
            await pipe.execute()
        except Exception as e:
            logger.error(f"Error storing TPS record for chain {record.chain_name}: {str(e)}")

    async def _update_all_chain_records(self, chain_data: Dict[str, Any]):
        """Update ATH and 24h high records for each individual chain."""
        now = datetime.now()
        timestamp = now.isoformat()
        timestamp_ms = int(now.timestamp() * 1000)

        for chain_name, data in chain_data.items():
            current_tps = self._safe_float(data.get("tps", 0))
            if current_tps <= 0:
                continue

            metrics = self.chain_metrics.setdefault(chain_name, {
                "ath": 0, "ath_timestamp": "", "24h_high": 0, "24h_high_timestamp": ""
            })
            
            is_new_ath = current_tps > metrics.get("ath", 0)
            
            is_new_24h_high = False
            last_24h_ts_str = metrics.get("24h_high_timestamp", "")
            if last_24h_ts_str:
                try:
                    last_24h_dt = datetime.fromisoformat(last_24h_ts_str)
                    if (now - last_24h_dt) > timedelta(hours=24):
                       is_new_24h_high = True
                       metrics["24h_high"] = 0
                except ValueError: is_new_24h_high = True
            else: is_new_24h_high = True

            if not is_new_24h_high and current_tps > metrics.get("24h_high", 0):
                is_new_24h_high = True
            
            pipe = self.redis_client.pipeline()
            
            if is_new_ath:
                metrics["ath"], metrics["ath_timestamp"] = current_tps, timestamp
                pipe.hset(RedisKeys.chain_tps_ath(chain_name), mapping={"value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(timestamp_ms)})
                logger.info(f"🚀 NEW CHAIN ATH for {chain_name}: {current_tps} TPS!")

            if is_new_24h_high:
                metrics["24h_high"], metrics["24h_high_timestamp"] = current_tps, timestamp
                pipe.hset(RedisKeys.chain_tps_24h(chain_name), mapping={"value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(timestamp_ms)})

            if pipe: await pipe.execute()
            
            record = TPSRecord(chain_name=chain_name, value=current_tps, timestamp=timestamp, timestamp_ms=timestamp_ms, is_ath=is_new_ath)
            await self._store_chain_tps_record(record)

    async def _update_global_tps_records(self, current_tps: float, timestamp: str, chain_data: Dict[str, Any]):
        """Update GLOBAL TPS ATH and 24hr high records."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            chain_breakdown = self._extract_chain_breakdown(chain_data)
            is_new_ath = current_tps > self.tps_ath

            pipe = self.redis_client.pipeline()

            if is_new_ath:
                old_ath = self.tps_ath
                self.tps_ath, self.tps_ath_timestamp = current_tps, timestamp
                pipe.hset(RedisKeys.GLOBAL_TPS_ATH, mapping={"value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(current_timestamp_ms)})
                ath_message = f"""🚀 NEW GLOBAL TPS ALL-TIME HIGH: {current_tps:.2f} TPS! (Previous: {old_ath:.2f})"""
                send_discord_message(ath_message)

            if current_tps > self.tps_24h_high:
                self.tps_24h_high, self.tps_24h_high_timestamp = current_tps, timestamp
                pipe.hset(RedisKeys.GLOBAL_TPS_24H, mapping={"value": str(current_tps), "timestamp": timestamp, "timestamp_ms": str(current_timestamp_ms)})
            
            if pipe: await pipe.execute()

            record = TPSRecord(value=current_tps, timestamp=timestamp, timestamp_ms=current_timestamp_ms, chain_breakdown=chain_breakdown, total_chains=len(chain_data), active_chains=len(chain_breakdown), is_ath=is_new_ath)
            await self._store_global_tps_record(record)

        except Exception as e:
            logger.error(f"Error updating global TPS records: {str(e)}")

    async def _calculate_global_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate global metrics from chain data."""
        try:
            await self._update_all_chain_records(chain_data)
            tps_values = [self._safe_float(data.get("tps", 0)) for data in chain_data.values() if isinstance(data, dict)]
            total_tps = sum(tps_values)
            
            current_timestamp = datetime.now().isoformat()
            if total_tps > 0:
                await self._update_global_tps_records(total_tps, current_timestamp, chain_data)
            
            eth_data = chain_data.get("ethereum", {})
            l2_metrics = self._calculate_l2_metrics(chain_data)
            
            return {
                "total_tps": round(total_tps, 1),
                "highest_tps": round(max(tps_values, default=0), 1),
                "highest_l2_cost_usd": l2_metrics["highest_cost_usd"],
                "total_chains": len(chain_data),
                "active_chains": sum(1 for tps in tps_values if tps > 0),
                "ethereum_tx_cost_usd": self._safe_float(eth_data.get("tx_cost_erc20_transfer_usd")),
                "ethereum_tx_cost_eth": self._safe_float(eth_data.get("tx_cost_erc20_transfer")),
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
            return {"error": str(e)}
    
    async def periodic_maintenance_loop(self):
        """Combined maintenance loop: refresh 24h high and cleanup old data every 5 minutes."""
        logger.info("Starting periodic maintenance loop (every 1 minute)")
        while True:
            await asyncio.sleep(60)
            try:
                await self._periodic_maintenance()
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
   
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
    
    async def _get_all_chains(self) -> List[str]:
        """Get all chain names from Redis with caching."""
        now = datetime.now()
        if (self._chain_cache and self._chain_cache_time and (now - self._chain_cache_time).total_seconds() < self._cache_ttl):
            return self._chain_cache
        try:
            chains = set()
            cursor = '0'
            while cursor != 0:
                cursor, keys = await self.redis_client.scan(cursor=cursor, match="chain:*", count=1000)
                for key in keys:
                    if ":tps:" in key: continue
                    chains.add(key.split(':')[1])
            self._chain_cache, self._chain_cache_time = sorted(list(chains)), now
            return self._chain_cache
        except Exception as e:
            logger.error(f"Error getting chain keys: {str(e)}")
            return self._chain_cache or []
    
    async def _get_latest_chain_data(self, chain_name: str) -> Dict[str, Any]:
        """Get latest data for a specific chain from Redis stream."""
        try:
            entries = await self.redis_client.xrevrange(RedisKeys.chain_stream(chain_name), count=1)
            if not entries: return {"chain_name": chain_name, "tps": 0, "error": "No data available"}
            _id, fields = entries[0]
            ts = self._safe_int(fields.get("timestamp", 0))
            return {**fields, "tps": self._safe_float(fields.get("tps", 0)), "last_updated": datetime.fromtimestamp(ts/1000).isoformat() if ts else ""}
        except Exception as e:
            return {"chain_name": chain_name, "tps": 0, "error": str(e)}

    async def _get_all_chain_data(self) -> Dict[str, Any]:
        """Get latest data for all chains concurrently."""
        chains = await self._get_all_chains()
        results = await asyncio.gather(*[self._get_latest_chain_data(c) for c in chains])
        return {c: r for c, r in zip(chains, results) if not isinstance(r, Exception)}
    
    def _calculate_l2_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Calculate Layer 2 transaction cost metrics."""
        l2_data = []
        for name, data in chain_data.items():
            if name != "ethereum" and isinstance(data, dict):
                tps = self._safe_float(data.get("tps", 0))
                cost_usd = self._safe_float(data.get("tx_cost_erc20_transfer_usd", 0))
                cost_eth = self._safe_float(data.get("tx_cost_erc20_transfer", 0))
                if tps > 0 and cost_usd > 0:
                    l2_data.append((cost_usd, cost_eth, tps))
        
        if not l2_data: return {"avg_cost_usd": None, "avg_cost_eth": None, "highest_cost_usd": None}
        
        total_weighted_usd = sum(cost_usd * tps for cost_usd, _, tps in l2_data)
        total_weighted_eth = sum(cost_eth * tps for _, cost_eth, tps in l2_data)
        total_tps = sum(tps for _, _, tps in l2_data)
        
        return {
            "avg_cost_usd": total_weighted_usd / total_tps if total_tps > 0 else None,
            "avg_cost_eth": total_weighted_eth / total_tps if total_tps > 0 else None,
            "highest_cost_usd": max(cost_usd for cost_usd, _, _ in l2_data)
        }

    async def _update_data(self):
        """Fetch latest data from Redis and update internal state."""
        try:
            chain_data = await self._get_all_chain_data()
            global_metrics = await self._calculate_global_metrics(chain_data)
            self.latest_data, self.global_metrics = chain_data, global_metrics
        except Exception as e: logger.error(f"Error updating data: {str(e)}")
    
    async def _broadcast_to_clients(self):
        """Broadcast updated data to all connected global SSE clients."""
        if not self.connected_clients: return
        msg = f"data: {json.dumps({'type':'update', 'data':self.latest_data, 'global_metrics':self.global_metrics})}\n\n"
        dead = {c for c in self.connected_clients if not await self._send_to_client(c, msg)}
        if dead: self.connected_clients.difference_update(dead)

    # ### NEW ### - Broadcast function for chain-specific SSE streams
    async def _broadcast_to_chain_clients(self):
        if not self.chain_event_clients: return
        dead = {}
        for chain, clients in self.chain_event_clients.items():
            if not clients or chain not in self.latest_data: continue
            payload = {'type':'update', 'data':self.latest_data.get(chain), 'metrics':self.chain_metrics.get(chain)}
            msg = f"data: {json.dumps(payload)}\n\n"
            for c in clients:
                if not await self._send_to_client(c, msg): dead.setdefault(chain, set()).add(c)
        for chain, clients in dead.items():
            self.chain_event_clients[chain].difference_update(clients)
            if not self.chain_event_clients[chain]: del self.chain_event_clients[chain]

    async def _send_to_client(self, client, message):
        try:
            await client.write(message.encode('utf-8')); return True
        except Exception: return False
    
    async def data_update_loop(self):
        """Background task that continuously updates data and broadcasts to clients."""
        logger.info(f"Starting data update loop with {self.config.update_interval}s interval")
        while True:
            try:
                await self._update_data()
                # ### MODIFIED ### - Added new broadcast task
                await asyncio.gather(
                    self._broadcast_to_clients(),
                    self._broadcast_to_chain_clients()
                )
                await asyncio.sleep(self.config.update_interval)
            except Exception as e:
                logger.error(f"Error in data update loop: {str(e)}")
                await asyncio.sleep(5)
    
    async def sse_handler(self, request):
        """Handle global SSE connection requests."""
        if len(self.connected_clients) >= self.config.max_connections: return web.Response(status=503)
        resp = web.StreamResponse(headers={'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive'})
        await resp.prepare(request)
        self.connected_clients.add(resp)
        try:
            initial_msg = f"data: {json.dumps({'type':'initial', 'data':self.latest_data, 'global_metrics':self.global_metrics})}\n\n"
            if not await self._send_to_client(resp, initial_msg): return resp
            while True:
                await asyncio.sleep(self.config.heartbeat_interval)
                if not await self._send_to_client(resp, ": heartbeat\n\n"): break
        finally: self.connected_clients.discard(resp)
        return resp
    
    # ### NEW ### - SSE Handler for chain-specific streams
    async def chain_sse_handler(self, request):
        chain = request.match_info.get('chain_name')
        if not chain or chain not in self.latest_data: return web.Response(status=404, text="Chain not found")
        
        clients = self.chain_event_clients.setdefault(chain, set())
        resp = web.StreamResponse(headers={'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive'})
        await resp.prepare(request)
        clients.add(resp)
        logger.info(f"New SSE client for chain '{chain}'. Total for chain: {len(clients)}")

        try:
            initial_payload = {'type':'initial', 'data':self.latest_data.get(chain), 'metrics':self.chain_metrics.get(chain)}
            initial_msg = f"data: {json.dumps(initial_payload)}\n\n"
            if not await self._send_to_client(resp, initial_msg): return resp
            
            while True:
                await asyncio.sleep(self.config.heartbeat_interval)
                if not await self._send_to_client(resp, ": heartbeat\n\n"): break
        finally:
            clients.discard(resp)
            if not clients: del self.chain_event_clients[chain]
            logger.info(f"SSE client for chain '{chain}' disconnected.")
        return resp
    
    async def health_handler(self, request):
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy", 
            "global_sse_clients": len(self.connected_clients),
            "chain_sse_listeners": {k:len(v) for k,v in self.chain_event_clients.items()},
            "max_connections": self.config.max_connections, 
            "total_chains": len(self.latest_data),
            "active_chains": self.global_metrics.get("active_chains", 0),
            "total_tps": self.global_metrics.get("total_tps", 0),
            "total_tps_ath": self.global_metrics.get("total_tps_ath", 0),
            "timestamp": datetime.now().isoformat()
        })
    
    async def api_data_handler(self, request):
        """REST API endpoint to get current global data."""
        return web.json_response({"data": self.latest_data, "global_metrics": self.global_metrics})
    
    async def history_handler(self, request):
        """REST API endpoint to get global TPS history."""
        try:
            limit = int(request.query.get('limit', 100))
            hours = int(request.query.get('hours', 24))
            now_ms = int(datetime.now().timestamp() * 1000)
            min_ts = now_ms - (hours * 60 * 60 * 1000)
            
            raw_entries = await self.redis_client.zrevrangebyscore(
                RedisKeys.TPS_HISTORY_24H, now_ms, min_ts, start=0, num=limit
            )
            history = [json.loads(raw_entry) for raw_entry in raw_entries]
            
            avg_tps = max_tps = min_tps = 0
            if history:
                tps_values = [h['tps'] for h in history]
                if tps_values:
                    avg_tps = sum(tps_values) / len(tps_values)
                    max_tps = max(tps_values)
                    min_tps = min(tps_values)

            return web.json_response({
                "history": history,
                "summary": {
                    "total_events": len(history), "time_range_hours": hours,
                    "avg_tps": round(avg_tps, 2), "max_tps": round(max_tps, 2),
                    "min_tps": round(min_tps, 2), "current_ath": round(self.tps_ath, 1),
                    "current_24h_high": round(self.tps_24h_high, 1),
                }, "timestamp": datetime.now().isoformat()
            })
        except ValueError as e:
            return web.json_response({"error": "Invalid query parameters", "message": str(e)}, status=400)
        except Exception as e:
            logger.error(f"Error in global history handler: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)
    async def chain_data_handler(self, request):
        """REST API endpoint to get current data for a single chain."""
        chain = request.match_info.get('chain_name')
        if not chain or chain not in self.latest_data: return web.json_response({"error": "Not found"}, status=404)
        return web.json_response({"data": self.latest_data.get(chain), "metrics": self.chain_metrics.get(chain)})

    # ### MODIFIED ### - Renamed from chain_events_handler
    async def chain_ath_history_handler(self, request):
        """REST API endpoint to get a static list of ATH events for a single chain."""
        chain = request.match_info.get('chain_name')
        if not chain: return web.json_response({"error": "Chain name not provided"}, status=400)
        try:
            limit = int(request.query.get('limit', 50))
            raw = await self.redis_client.zrevrange(RedisKeys.chain_ath_history(chain), 0, limit - 1)
            return web.json_response({"events": [json.loads(e) for e in raw]})
        except Exception as e: return web.json_response({"error": str(e)}, status=500)

    async def chain_history_handler(self, request):
        """REST API endpoint to get TPS history for a single chain."""
        chain = request.match_info.get('chain_name')
        if not chain: return web.json_response({"error": "Chain name not provided"}, status=400)
        try:
            limit = int(request.query.get('limit', 100)); hours = int(request.query.get('hours', 24))
            now, min_ts = int(datetime.now().timestamp()*1000), int((datetime.now()-timedelta(hours=hours)).timestamp()*1000)
            raw = await self.redis_client.zrevrangebyscore(RedisKeys.chain_tps_history_24h(chain), now, min_ts, start=0, num=limit)
            return web.json_response({"history": [json.loads(e) for e in raw]})
        except Exception as e: return web.json_response({"error": str(e)}, status=500)

async def create_app(server: RedisSSEServer):
    """Create and configure the aiohttp application."""
    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={"*": aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_headers="*", allow_methods="*")})
    
    # ### MODIFIED ### - Routes have been updated
    routes = [
        web.get("/events", server.sse_handler),
        web.get("/health", server.health_handler),
        web.get("/api/data", server.api_data_handler),
        web.get("/api/history", server.history_handler),
        web.get("/api/chain/{chain_name}", server.chain_data_handler),
        web.get("/api/chain/{chain_name}/history", server.chain_history_handler),
        # New real-time SSE endpoint
        web.get("/api/chain/{chain_name}/events", server.chain_sse_handler),
        # Renamed static endpoint for historical ATHs
        web.get("/api/chain/{chain_name}/ath-history", server.chain_ath_history_handler),
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
        
        update_task = asyncio.create_task(server.data_update_loop())
        maintenance_task = asyncio.create_task(server.periodic_maintenance_loop())
        
        logger.info(f"🚀 Starting SSE server on {config.server_host}:{config.server_port}")
        logger.info(f"📡 Global SSE stream: /events")
        logger.info(f"🏥 Health check: /health")
        logger.info(f"📈 Global API: /api/data")
        logger.info(f"📊 Global History: /api/history")
        logger.info(f"⛓️ Chain SSE stream: /api/chain/{{chain_name}}/events")
        logger.info(f"📜 Chain History: /api/chain/{{chain_name}}/history")
        logger.info(f"🎉 Static ATH list: /api/chain/{{chain_name}}/ath-history")
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, config.server_host, config.server_port)
        await site.start()

        try:
            await asyncio.Future()
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