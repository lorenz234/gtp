# Redis SSE Server

Real-time blockchain TPS monitoring API that reads from Redis streams and provides Server-Sent Events.

## Endpoints

- `GET /events` - SSE stream for real-time updates
- `GET /health` - Health check
- `GET /api/data` - Current data snapshot

## Environment Variables

- `REDIS_HOST` - Redis server host
- `REDIS_PASSWORD` - Redis auth string (if enabled)
- `SERVER_PORT` - Server port (default: 8080)
- `UPDATE_INTERVAL` - Update frequency in seconds (default: 3)

## Deployment

Automatically deploys to Google Cloud Run on push to main branch.