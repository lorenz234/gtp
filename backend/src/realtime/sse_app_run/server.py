import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set, Optional
from dataclasses import dataclass

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

    @classmethod
    def from_env(cls) -> 'ServerConfig':
        """Create config from environment variables."""
        import os
        return cls(
            redis_host=os.getenv("REDIS_HOST", cls.redis_host),
            redis_port=int(os.getenv("REDIS_PORT", cls.redis_port)),
            redis_db=int(os.getenv("REDIS_DB", cls.redis_db)),
            redis_password=os.getenv("REDIS_PASSWORD"),
            server_port=int(os.getenv("SERVER_PORT", cls.server_port))
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
        """Load existing TPS ATH and 24hr high from Redis."""
        try:
            # Load ATH
            ath_data = await self.redis_client.hgetall(RedisKeys.GLOBAL_TPS_ATH)
            if ath_data:
                self.tps_ath = float(ath_data.get("value", 0))
                self.tps_ath_timestamp = ath_data.get("timestamp", "")
                logger.info(f"📈 Loaded TPS ATH: {self.tps_ath} (set at {self.tps_ath_timestamp})")
            
            # Load 24hr high
            h24_data = await self.redis_client.hgetall(RedisKeys.GLOBAL_TPS_24H)
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
            removed = await self.redis_client.zremrangebyscore(
                RedisKeys.TPS_HISTORY_24H, 0, cutoff_time
            )
            if removed > 0:
                logger.debug(f"🧹 Cleaned up {removed} old TPS history entries")
        except Exception as e:
            logger.error(f"Error cleaning up 24hr history: {str(e)}")
    
    def _extract_chain_breakdown(self, chain_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract TPS values for each active chain."""
        breakdown = {}
        for chain_name, data in chain_data.items():
            if isinstance(data, dict) and "tps" in data:
                tps_value = data.get("tps", 0)
                if tps_value > 0:
                    breakdown[chain_name] = tps_value
        return breakdown
    
    async def _store_tps_record(self, record: TPSRecord):
        """Store TPS record in Redis."""
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
            
            if record.is_ath:
                await self.redis_client.zadd(
                    RedisKeys.ATH_HISTORY,
                    {json.dumps(history_entry): record.timestamp_ms}
                )
            
            await self.redis_client.zadd(
                RedisKeys.TPS_HISTORY_24H,
                {json.dumps(history_entry): record.timestamp_ms}
            )
            
        except Exception as e:
            logger.error(f"Error storing TPS record: {str(e)}")
    
    async def _update_tps_records(self, current_tps: float, timestamp: str, chain_data: Dict[str, Any]):
        """Update TPS ATH and 24hr high records if new highs are reached."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            chain_breakdown = self._extract_chain_breakdown(chain_data)
            
            is_new_ath = current_tps > self.tps_ath
            
            if is_new_ath:
                old_ath = self.tps_ath
                self.tps_ath = current_tps
                self.tps_ath_timestamp = timestamp
                
                await self.redis_client.hset(RedisKeys.GLOBAL_TPS_ATH, mapping={
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                })
                
                logger.info(f"🚀 NEW TPS ALL-TIME HIGH: {current_tps} TPS! (Previous: {old_ath})")
            
            ## load current 24h high from Redis 24h history
            ## TODO: seems like a lot of overhead / unnecessary calls to Redis. Maybe we can optimize this
            try:
                current_24h_history = await self.redis_client.zrevrange(
                    RedisKeys.TPS_HISTORY_24H, 0, 0, withscores=True
                )
                
                if current_24h_history:
                    entry_str = current_24h_history[0][0]
                    last_entry = json.loads(entry_str)

                    tps_str = last_entry.get("tps", "0")
                    try:
                        last_tps = float(tps_str)
                    except ValueError:
                        last_tps = 0
                else:
                    last_tps = 0

            except Exception as e:
                # Optional: log the error here
                last_tps = 0
            logger.info(f"Last 24h TPS: {last_tps} (from Redis history)")
            
            if last_tps > current_tps:
                high_24h = last_tps
                timestamp_24h = last_entry.get("timestamp", timestamp)
            else:
                high_24h = current_tps
                timestamp_24h = timestamp
                logger.info(f"📊 NEW 24HR TPS HIGH: {current_tps} TPS!")
                
            self.tps_24h_high = high_24h
            self.tps_24h_high_timestamp = timestamp_24h
            
            await self.redis_client.hset(RedisKeys.GLOBAL_TPS_24H, mapping={
                "value": str(current_tps),
                "timestamp": timestamp,
                "timestamp_ms": str(current_timestamp_ms)
            })
            
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
            logger.info(f"💾 Stored {record_type} history: {current_tps} TPS with {len(chain_breakdown)} chains")
            
            # Periodic cleanup
            if current_timestamp_ms % self.config.cleanup_interval_ms == 0:
                await self._cleanup_24h_history()
                
        except Exception as e:
            logger.error(f"Error updating TPS records: {str(e)}")
    
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
    
    async def _get_all_chains(self) -> List[str]:
        """Get all chain names from Redis."""
        try:
            keys = await self.redis_client.keys("chain:*")
            return [key.replace("chain:", "") for key in keys]
        except Exception as e:
            logger.error(f"Error getting chain keys: {str(e)}")
            return []
    
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
            
            return {
                "chain_name": chain_name,
                "tps": float(fields.get("tps", 0)),
                "timestamp": int(fields.get("timestamp", 0)),
                "tx_cost_erc20_transfer": float(fields.get("tx_cost_erc20_transfer", 0)),
                "tx_cost_erc20_transfer_usd": float(fields.get("tx_cost_erc20_transfer_usd", 0)),
                "last_updated": datetime.fromtimestamp(int(fields.get("timestamp", 0)) / 1000).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting data for {chain_name}: {str(e)}")
            return {
                "chain_name": chain_name,
                "tps": 0,
                "error": str(e)
            }
    
    async def _get_all_chain_data(self) -> Dict[str, Any]:
        """Get latest data for all chains."""
        chains = await self._get_all_chains()
        chain_data = {}
        
        for chain_name in chains:
            chain_data[chain_name] = await self._get_latest_chain_data(chain_name)
        
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
            
            sse_message = f"data: {message}\n\n"
            disconnected_clients = set()
            
            for client in self.connected_clients:
                try:
                    await client.write(sse_message.encode('utf-8'))
                    await client.drain()
                except (ConnectionResetError, ConnectionAbortedError):
                    disconnected_clients.add(client)
                except Exception as e:
                    logger.warning(f"Error sending to client: {str(e)}")
                    disconnected_clients.add(client)
            
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
        """Handle SSE connection requests."""
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
                    await response.drain()
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
            "total_chains": len(self.latest_data),
            "active_chains": self.global_metrics.get("active_chains", 0),
            "total_tps": self.global_metrics.get("total_tps", 0),
            "total_tps_ath": self.global_metrics.get("total_tps_ath", 0),
            "total_tps_24h_high": self.global_metrics.get("total_tps_24h_high", 0),
            "redis_connected": self.redis_client is not None,
            "timestamp": datetime.now().isoformat()
        })
    
    async def api_data_handler(self, request):
        """REST API endpoint to get current data."""
        return web.json_response({
            "data": self.latest_data,
            "global_metrics": self.global_metrics,
            "timestamp": datetime.now().isoformat()
        })


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
        
        # Start the data update loop
        update_task = asyncio.create_task(server.data_update_loop())
        
        # Start the web server
        logger.info(f"🚀 Starting SSE server on {config.server_host}:{config.server_port}")
        logger.info(f"📡 SSE endpoint: http://{config.server_host}:{config.server_port}/events")
        logger.info(f"🏥 Health check: http://{config.server_host}:{config.server_port}/health")
        logger.info(f"📈 API endpoint: http://{config.server_host}:{config.server_port}/api/data")
        
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