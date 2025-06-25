import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set

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

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Server configuration
SERVER_HOST = "0.0.0.0"
SERVER_PORT = "8080"
UPDATE_INTERVAL = 1 # seconds

# Redis keys for storing global metrics
REDIS_KEY_GLOBAL_TPS_ATH = "global:tps:ath"
REDIS_KEY_GLOBAL_TPS_24H = "global:tps:24h_high"
REDIS_KEY_TPS_HISTORY_24H = "global:tps:history_24h"
REDIS_KEY_ATH_HISTORY = "global:tps:ath_history"

class RedisSSEServer:
    """SSE Server that reads blockchain data from Redis streams."""
    
    def __init__(self):
        self.redis_client = None
        self.connected_clients: Set[web.StreamResponse] = set()
        self.latest_data: Dict[str, Any] = {}
        self.global_metrics: Dict[str, Any] = {}
        
        # Local tracking for TPS highs
        self.tps_ath: float = 0.0
        self.tps_ath_timestamp: str = ""
        self.tps_24h_high: float = 0.0
        self.tps_24h_high_timestamp: str = ""
        
    async def initialize(self):
        """Initialize Redis connection and load existing TPS records."""
        try:
            redis_params = {
                "host": REDIS_HOST,
                "port": REDIS_PORT,
                "db": REDIS_DB,
                "decode_responses": True,
                "socket_keepalive": True,
                "retry_on_timeout": True,
                "health_check_interval": 30,
            }
            
            if REDIS_PASSWORD:
                redis_params["password"] = REDIS_PASSWORD
                
            self.redis_client = aioredis.Redis(**redis_params)
            await self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            
            # Load existing TPS records from Redis
            await self.load_tps_records()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {str(e)}")
            raise
    
    async def load_tps_records(self):
        """Load existing TPS ATH and 24hr high from Redis."""
        try:
            # Load ATH
            ath_data = await self.redis_client.hgetall(REDIS_KEY_GLOBAL_TPS_ATH)
            if ath_data:
                self.tps_ath = float(ath_data.get("value", 0))
                self.tps_ath_timestamp = ath_data.get("timestamp", "")
                logger.info(f"📈 Loaded TPS ATH: {self.tps_ath} (set at {self.tps_ath_timestamp})")
            
            # Load 24hr high
            h24_data = await self.redis_client.hgetall(REDIS_KEY_GLOBAL_TPS_24H)
            if h24_data:
                self.tps_24h_high = float(h24_data.get("value", 0))
                self.tps_24h_high_timestamp = h24_data.get("timestamp", "")
                logger.info(f"📊 Loaded 24hr TPS High: {self.tps_24h_high} (set at {self.tps_24h_high_timestamp})")
            
            # Clean up old 24hr history entries
            await self.cleanup_24h_history()
            
        except Exception as e:
            logger.error(f"Error loading TPS records: {str(e)}")
    
    async def cleanup_24h_history(self):
        """Remove TPS history entries older than 24 hours."""
        try:
            # Calculate 24 hours ago timestamp
            cutoff_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
            
            # Remove old entries from the sorted set
            removed = await self.redis_client.zremrangebyscore(
                REDIS_KEY_TPS_HISTORY_24H, 
                0, 
                cutoff_time
            )
            
            if removed > 0:
                logger.debug(f"🧹 Cleaned up {removed} old TPS history entries")
                
        except Exception as e:
            logger.error(f"Error cleaning up 24hr history: {str(e)}")
    
    async def store_tps_history_entry(self, tps_value: float, timestamp: str, chain_breakdown: Dict[str, float], is_ath: bool = False):
        """Store TPS history entry with full context (only for ATH or 24hr highs)."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            
            # Create unified history entry format
            history_entry = {
                "tps": str(tps_value),
                "timestamp": timestamp,
                "timestamp_ms": str(current_timestamp_ms),
                "chain_breakdown": json.dumps(chain_breakdown),
                "total_chains": str(len(chain_breakdown)),
                "active_chains": str(sum(1 for tps in chain_breakdown.values() if tps > 0)),
                "is_ath": str(is_ath)
            }
            
            # Store in ATH history if it's an ATH
            if is_ath:
                await self.redis_client.zadd(
                    REDIS_KEY_ATH_HISTORY,
                    {json.dumps(history_entry): current_timestamp_ms}
                )
            
            # Always store in 24h history (whether ATH or just 24hr high)
            await self.redis_client.zadd(
                REDIS_KEY_TPS_HISTORY_24H,
                {json.dumps(history_entry): current_timestamp_ms}
            )
            
        except Exception as e:
            logger.error(f"Error storing TPS history entry: {str(e)}")
    
    async def get_chain_tps_breakdown(self, chain_data: Dict[str, Any]) -> Dict[str, float]:
        """Return current chain TPS breakdown."""
        try:
            # Extract TPS values for each chain
            breakdown = {}
            for chain_name, data in chain_data.items():
                if isinstance(data, dict) and "tps" in data:
                    tps_value = data.get("tps", 0)
                    if tps_value > 0:  # Only store active chains
                        breakdown[chain_name] = tps_value
                
            return breakdown
            
        except Exception as e:
            logger.error(f"Error getting chain TPS breakdown: {str(e)}")
            return {}
    
    async def update_tps_records(self, current_tps: float, timestamp: str, chain_data: Dict[str, Any]):
        """Update TPS ATH and 24hr high records if new highs are reached."""
        try:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            
            # Get current chain breakdown
            chain_breakdown = await self.get_chain_tps_breakdown(chain_data)
            
            is_new_ath = False
            is_new_24h_high = False
            should_store = False
            
            # Check and update ATH
            if current_tps > self.tps_ath:
                old_ath = self.tps_ath
                self.tps_ath = current_tps
                self.tps_ath_timestamp = timestamp
                is_new_ath = True
                should_store = True
                
                # Store in Redis
                await self.redis_client.hset(REDIS_KEY_GLOBAL_TPS_ATH, mapping={
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                })
                
                logger.info(f"🚀 NEW TPS ALL-TIME HIGH: {current_tps} TPS! (Previous: {old_ath})")
            
            # Check if it's a new 24hr high (only if not already ATH)
            if not is_new_ath and current_tps > self.tps_24h_high:
                is_new_24h_high = True
                should_store = True
                
                self.tps_24h_high = current_tps
                self.tps_24h_high_timestamp = timestamp
                
                # Store in Redis
                await self.redis_client.hset(REDIS_KEY_GLOBAL_TPS_24H, mapping={
                    "value": str(current_tps),
                    "timestamp": timestamp,
                    "timestamp_ms": str(current_timestamp_ms)
                })
                
                logger.info(f"📊 NEW 24HR TPS HIGH: {current_tps} TPS!")
            
            # Only store in history if it's a new ATH or new 24hr high
            if should_store:
                await self.store_tps_history_entry(current_tps, timestamp, chain_breakdown, is_new_ath)
                
                if is_new_ath:
                    logger.info(f"💾 Stored ATH history: {current_tps} TPS with {len(chain_breakdown)} chains")
                elif is_new_24h_high:
                    logger.info(f"💾 Stored 24hr high history: {current_tps} TPS with {len(chain_breakdown)} chains")
            
            # Clean up old entries periodically
            if current_timestamp_ms % 60000 == 0:  # Every minute
                await self.cleanup_24h_history()
                
        except Exception as e:
            logger.error(f"Error updating TPS records: {str(e)}")
    
    async def get_ath_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get ATH history from Redis."""
        try:
            # Get recent ATH entries
            entries = await self.redis_client.zrevrange(
                REDIS_KEY_ATH_HISTORY,
                start=0,
                end=limit-1,
                withscores=True
            )
            
            ath_history = []
            for entry_str, timestamp_ms in entries:
                try:
                    entry_data = json.loads(entry_str)
                    # Parse chain breakdown
                    chain_breakdown = json.loads(entry_data.get("chain_breakdown", "{}"))
                    
                    ath_record = {
                        "tps": float(entry_data.get("tps", 0)),
                        "timestamp": entry_data.get("timestamp", ""),
                        "timestamp_ms": int(entry_data.get("timestamp_ms", 0)),
                        "chain_breakdown": chain_breakdown,
                        "total_chains": int(entry_data.get("total_chains", 0)),
                        "active_chains": int(entry_data.get("active_chains", 0)),
                        "is_ath": entry_data.get("is_ath", "True") == "True"
                    }
                    
                    ath_history.append(ath_record)
                    
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse ATH history entry: {entry_str}")
                    continue
            
            return ath_history
            
        except Exception as e:
            logger.error(f"Error getting ATH history: {str(e)}")
            return []
    
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
    
    async def get_all_chains(self) -> List[str]:
        """Get all chain names from Redis."""
        try:
            keys = await self.redis_client.keys("chain:*")
            return [key.replace("chain:", "") for key in keys]
        except Exception as e:
            logger.error(f"Error getting chain keys: {str(e)}")
            return []
    
    async def get_latest_chain_data(self, chain_name: str) -> Dict[str, Any]:
        """Get latest data for a specific chain from Redis stream."""
        try:
            stream_key = f"chain:{chain_name}"
            
            # Get the latest entry from the stream
            entries = await self.redis_client.xrevrange(stream_key, count=1)
            
            if not entries:
                return {
                    "chain_name": chain_name,
                    "tps": 0,
                    "error": "No data available"
                }
            
            # Parse the latest entry
            entry_id, fields = entries[0]
            
            # Convert string values back to appropriate types
            chain_data = {
                "chain_name": chain_name,
                "tps": float(fields.get("tps", 0)),
                #"block_number": int(fields.get("block_number", 0)),
                "timestamp": int(fields.get("timestamp", 0)),
                #"tx_count": int(fields.get("tx_count", 0)),
                "tx_cost_erc20_transfer": float(fields.get("tx_cost_erc20_transfer", 0)),
                "tx_cost_erc20_transfer_usd": float(fields.get("tx_cost_erc20_transfer_usd", 0)),
                "last_updated": datetime.fromtimestamp(int(fields.get("timestamp", 0)) / 1000).isoformat()
            }
            
            return chain_data
            
        except Exception as e:
            logger.error(f"Error getting data for {chain_name}: {str(e)}")
            return {
                "chain_name": chain_name,
                "tps": 0,
                "error": str(e)
            }
    
    async def get_historical_chain_data(self, chain_name: str, seconds: int = 20) -> List[Dict[str, Any]]:
        """Get historical data for a specific chain from Redis stream."""
        try:
            stream_key = f"chain:{chain_name}"
            
            # Calculate timestamp range (last N seconds)
            current_time_ms = int(datetime.now().timestamp() * 1000)
            start_time_ms = current_time_ms - (seconds * 1000)
            
            # Get entries from the last N seconds
            entries = await self.redis_client.xrevrange(
                stream_key, 
                max=current_time_ms,
                min=start_time_ms,
                count=100  # Limit to prevent too much data
            )
            
            historical_data = []
            
            for entry_id, fields in entries:
                # Convert string values back to appropriate types
                chain_data = {
                    "chain_name": chain_name,
                    "tps": float(fields.get("tps", 0)),
                    #"block_number": int(fields.get("block_number", 0)),
                    "timestamp": int(fields.get("timestamp", 0)),
                    #"tx_count": int(fields.get("tx_count", 0)),
                    #"errors": int(fields.get("errors", 0)),
                    "tx_cost_erc20_transfer": float(fields.get("tx_cost_erc20_transfer", 0)),
                    "tx_cost_erc20_transfer_usd": float(fields.get("tx_cost_erc20_transfer_usd", 0)),
                    "last_updated": datetime.fromtimestamp(int(fields.get("timestamp", 0)) / 1000).isoformat()
                }
                
                historical_data.append(chain_data)
            
            # Return in chronological order (oldest first)
            return list(reversed(historical_data))
            
        except Exception as e:
            logger.error(f"Error getting historical data for {chain_name}: {str(e)}")
            return []
    
    async def get_all_chain_data(self) -> Dict[str, Any]:
        """Get latest data for all chains."""
        chains = await self.get_all_chains()
        chain_data = {}
        
        for chain_name in chains:
            chain_data[chain_name] = await self.get_latest_chain_data(chain_name)
        
        return chain_data
    
    async def calculate_global_metrics(self, chain_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate global metrics from chain data."""
        try:
            # Calculate total TPS across all chains
            total_tps = sum(
                data.get("tps", 0) for data in chain_data.values() 
                if isinstance(data.get("tps"), (int, float))
            )

            # Store highest recorded TPS value across all chains
            highest_tps = max(
                (data.get("tps", 0) for data in chain_data.values() if isinstance(data.get("tps"), (int, float))),
                default=0
            )
            
            # Get Ethereum data if available
            eth_data = chain_data.get("ethereum", {})
            ethereum_tx_cost_usd = eth_data.get("tx_cost_erc20_transfer_usd", None)
            ethereum_tx_cost_eth = eth_data.get("tx_cost_erc20_transfer", None)
            
            l2_costs_usd = [
                data.get("tx_cost_erc20_transfer_usd", 0)
                for name, data in chain_data.items()
                if (name != "ethereum" and 
                    data.get("tps", 0) > 0 and 
                    data.get("tx_cost_erc20_transfer_usd", 0) > 0)
            ]
            # Calculate weighted average L2 costs (excluding Ethereum)
            l2_costs_weighted_usd = [
                data.get("tx_cost_erc20_transfer_usd", 0) * data.get("tps", 0)
                for name, data in chain_data.items()
                if (name != "ethereum" and 
                    data.get("tps", 0) > 0 and 
                    data.get("tx_cost_erc20_transfer_usd", 0) > 0)
            ]
            l2_costs_weighted_eth = [
                data.get("tx_cost_erc20_transfer", 0) * data.get("tps", 0)
                for name, data in chain_data.items()
                if (name != "ethereum" and 
                    data.get("tps", 0) > 0 and 
                    data.get("tx_cost_erc20_transfer", 0) > 0)
            ]
            l2_tps = [
                data.get("tps", 0)
                for name, data in chain_data.items()
                if (name != "ethereum" and 
                    data.get("tps", 0) > 0 and 
                    data.get("tx_cost_erc20_transfer_usd", 0) > 0)
            ]
            
            avg_l2_tx_cost_usd = (sum(l2_costs_weighted_usd) / sum(l2_tps)) if l2_tps and sum(l2_tps) > 0 else None
            avg_l2_tx_cost_eth = (sum(l2_costs_weighted_eth) / sum(l2_tps)) if l2_tps and sum(l2_tps) > 0 else None
            highest_l2_cost_usd = max(l2_costs_usd) if l2_costs_usd else None
            
            # Count chains by type
            active_chains = 0
            
            for data in chain_data.values():
                if data.get("tps", 0) > 0:
                    active_chains += 1
            
            # Update TPS records if we have a new high
            current_timestamp = datetime.now().isoformat()
            if total_tps > 0:
                await self.update_tps_records(total_tps, current_timestamp, chain_data)
            
            return {
                "total_tps": round(total_tps, 1), 
                "highest_tps": round(highest_tps, 1),
                "highest_l2_cost_usd": highest_l2_cost_usd,
                "total_chains": len(chain_data),
                "active_chains": active_chains,
                "ethereum_tx_cost_usd": ethereum_tx_cost_usd,
                "ethereum_tx_cost_eth": ethereum_tx_cost_eth,
                "layer2s_tx_cost_usd": avg_l2_tx_cost_usd, 
                "layer2s_tx_cost_eth": avg_l2_tx_cost_eth,
                # Add TPS records to global metrics
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
    
    async def update_data(self):
        """Fetch latest data from Redis and update internal state."""
        try:
            # Get all chain data
            chain_data = await self.get_all_chain_data()
            
            # Calculate global metrics
            global_metrics = await self.calculate_global_metrics(chain_data)
            
            # Update internal state
            self.latest_data = chain_data
            self.global_metrics = global_metrics
            
            logger.debug(f"Updated data for {len(chain_data)} chains, total TPS: {global_metrics.get('total_tps', 0)}")
            
        except Exception as e:
            logger.error(f"Error updating data: {str(e)}")
    
    async def broadcast_to_clients(self):
        """Broadcast updated data to all connected SSE clients."""
        if not self.connected_clients:
            return
        
        try:
            # Prepare the message
            message = json.dumps({
                "type": "update",
                "data": self.latest_data,
                "global_metrics": self.global_metrics,
                "timestamp": datetime.now().isoformat()
            })
            
            # Format for SSE
            sse_message = f"data: {message}\n\n"
            
            # Send to all clients
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
            
            # Remove disconnected clients
            if disconnected_clients:
                self.connected_clients.difference_update(disconnected_clients)
                logger.info(f"Removed {len(disconnected_clients)} disconnected clients. {len(self.connected_clients)} remaining.")
                
        except Exception as e:
            logger.error(f"Error broadcasting to clients: {str(e)}")
    
    async def data_update_loop(self):
        """Background task that continuously updates data and broadcasts to clients."""
        logger.info(f"Starting data update loop with {UPDATE_INTERVAL}s interval")
        
        while True:
            try:
                # Update data from Redis
                await self.update_data()
                
                # Broadcast to clients
                await self.broadcast_to_clients()
                
                # Wait before next update
                await asyncio.sleep(UPDATE_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in data update loop: {str(e)}")
                await asyncio.sleep(5)  # Wait longer on error
    
    async def sse_handler(self, request):
        """Handle SSE connection requests."""
        response = web.StreamResponse()
        response.headers['Content-Type'] = 'text/event-stream'
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Connection'] = 'keep-alive'
        response.headers['X-Accel-Buffering'] = 'no'
        
        client_ip = request.remote
        logger.info(f"New SSE client connected from {client_ip}")
        
        try:
            await response.prepare(request)
            
            # Add client to connected set
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
                await asyncio.sleep(30)  # Heartbeat every 30 seconds
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
    
    async def history_handler(self, request):
        """Handle requests for historical data formatted as SSE events."""
        # Get query parameters
        seconds = int(request.query.get('seconds', 60))  # Default 60 seconds
        format_type = request.query.get('format', 'sse')  # 'sse' or 'json'
        
        try:
            # Get current chains
            chains = await self.get_all_chains()
            
            # Create timeline by getting historical data for each chain
            timeline_events = []
            
            # Get historical data for each chain
            for chain_name in chains:
                chain_history = await self.get_historical_chain_data(chain_name, seconds)
                
                for historical_point in chain_history:
                    timestamp_ms = historical_point["timestamp"]
                    
                    # Find or create timeline event for this timestamp (group by time)
                    existing_event = None
                    for event in timeline_events:
                        if abs(event["timestamp_ms"] - timestamp_ms) < 2000:  # Within 2 seconds
                            existing_event = event
                            break
                    
                    if existing_event:
                        # Add this chain's data to existing event
                        existing_event["data"][chain_name] = historical_point
                    else:
                        # Create new timeline event
                        timeline_events.append({
                            "timestamp_ms": timestamp_ms,
                            "data": {chain_name: historical_point},
                            "iso_timestamp": datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
                        })
            
            # Sort by timestamp and calculate global metrics for each event
            timeline_events.sort(key=lambda x: x["timestamp_ms"])
            
            # Calculate global metrics for each timeline event
            formatted_events = []
            for event in timeline_events:
                global_metrics = await self.calculate_global_metrics(event["data"])
                global_metrics["timestamp_ms"] = event["timestamp_ms"]
                
                formatted_event = {
                    "type": "historical",
                    "data": event["data"],
                    "global_metrics": global_metrics,
                    "timestamp": event["iso_timestamp"]
                }
                
                formatted_events.append(formatted_event)
            
            # Limit to reasonable number of events (last 20 events)
            formatted_events = formatted_events[-20:]
            
            if format_type == 'json':
                return web.json_response({
                    "events": formatted_events,
                    "count": len(formatted_events),
                    "timeframe_seconds": seconds,
                    "chains_included": len(chains),
                    "timestamp": datetime.now().isoformat()
                })
            
            else:
                # SSE format
                response = web.StreamResponse()
                response.headers['Content-Type'] = 'text/event-stream'
                response.headers['Cache-Control'] = 'no-cache'
                response.headers['Connection'] = 'keep-alive'
                response.headers['X-Accel-Buffering'] = 'no'
                
                await response.prepare(request)
                
                # Send historical events with small delays
                for i, event in enumerate(formatted_events):
                    sse_message = f"data: {json.dumps(event)}\n\n"
                    await response.write(sse_message.encode('utf-8'))
                    
                    # Small delay between events for smooth playback
                    if i < len(formatted_events) - 1:
                        await asyncio.sleep(0.1)  # 100ms between events
                
                # Send completion marker
                completion_event = {
                    "type": "history_complete",
                    "message": f"Sent {len(formatted_events)} historical events from last {seconds}s",
                    "count": len(formatted_events),
                    "timestamp": datetime.now().isoformat()
                }
                
                await response.write(f"data: {json.dumps(completion_event)}\n\n".encode('utf-8'))
                
                return response
                
        except Exception as e:
            logger.error(f"Error in history handler: {str(e)}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
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
        web.get("/history", server.history_handler),
    ]
    
    for route in routes:
        cors.add(app.router.add_route(route.method, route.path, route.handler))
    
    return app


async def main():
    """Main function to start the SSE server."""
    server = RedisSSEServer()
    
    try:
        # Initialize Redis connection
        await server.initialize()
        
        # Create the web application
        app = await create_app(server)
        
        # Start the data update loop
        update_task = asyncio.create_task(server.data_update_loop())
        
        # Start the web server
        logger.info(f"🚀 Starting SSE server on {SERVER_HOST}:{SERVER_PORT}")
        logger.info(f"📡 SSE endpoint: http://{SERVER_HOST}:{SERVER_PORT}/events")
        logger.info(f"📊 History endpoint: http://{SERVER_HOST}:{SERVER_PORT}/history")
        logger.info(f"🏥 Health check: http://{SERVER_HOST}:{SERVER_PORT}/health")
        logger.info(f"📈 API endpoint: http://{SERVER_HOST}:{SERVER_PORT}/api/data")
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, SERVER_HOST, SERVER_PORT)
        await site.start()
        
        # Keep server running
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