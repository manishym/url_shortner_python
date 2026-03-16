# Distributed URL Shortener

Microservice-based URL shortener with Snowflake ID generation, multi-tier cache-aside (Local LRU → Redis → Postgres), and Redpanda for cache invalidation.

## Architecture

- **ID Service**: Standalone FastAPI service that generates 64-bit Snowflake IDs. Uses `WORKER_ID` from environment; includes clock drift protection.
- **Shortener Service**: Handles URL shortening - calls ID service, Base62 encodes, stores in Postgres + Redis.
- **Redirection Service**: Handles redirects (GET /r/{path}) with multi-tier cache (LRU → Redis → Postgres). Background consumer invalidates LRU on purge events.
- **Deletion Service**: Handles URL deletion (DELETE /r/{path}) - produces Kafka purge event. Background consumers delete from Postgres and Redis.
- **Gateway (nginx)**: API gateway routing requests to appropriate services based on path and HTTP method.
- **Redpanda**: Single-node Kafka-compatible broker for `url-purge-events` topic.
- **Postgres**: Persistent store for short_path → long_url.
- **Redis**: Distributed cache (5-minute TTL).

## Run with Docker Compose

```bash
docker compose up --build
```

- **Gateway** (main entry): http://localhost:8080 — **Demo UI** at http://localhost:8080/
- **ID Service**: http://localhost:8000 (GET `/generate` returns 64-bit integer)
- **Shortener Service**: http://localhost:8001 (internal: shortener-service:8000)
- **Redirection Service**: http://localhost:8002 (internal: redirection-service:8000)
- **Deletion Service**: http://localhost:8003 (internal: deletion-service:8000)

## API Endpoints

| Method | Path | Service | Description |
|--------|------|---------|-------------|
| POST | /shorten | Shortener | Create short URL |
| GET | /r/{path} | Redirection | Redirect to long URL |
| DELETE | /r/{path} | Deletion | Delete short URL |
| GET | /health | All | Health check |

## Environment

| Service | Variable | Default |
|---------|----------|---------|
| ID Service | `WORKER_ID` | `0` (required in production) |
| ID Service | `EPOCH_MS` | `1739980800000` |
| Shortener | `ID_SERVICE_URL` | `http://id-service:8000` |
| Shortener/Redirection/Deletion | `DATABASE_URL` | `postgresql://shortener:shortener@postgres:5432/shortener` |
| Shortener/Redirection/Deletion | `REDIS_URL` | `redis://redis:6379/0` |
| Redirection/Deletion | `KAFKA_BOOTSTRAP` | `redpanda:9092` |
| Redirection/Deletion | `PURGE_TOPIC` | `url-purge-events` |
| Redirection | `LRU_MAX_SIZE` | `10000` |

## Observability

Logs are prefixed for filtering:

- **[ID-SERVICE]**: e.g. `Generated ID: 12345`, clock drift warnings/errors
- **[SHORTENER-SERVICE]**: e.g. `Shorten: id=... -> path=...`
- **[REDIRECTION-SERVICE]**: e.g. `Redirect cache hit (LRU): ...`, `Redirect cache hit (Redis): ...`, `Redirect DB hit: ...`
- **[DELETION-SERVICE]**: e.g. `Purge event produced for: ...`, `DB deleted: ...`, `Redis deleted: ...`

## Example

```bash
# Shorten (through gateway)
curl -X POST http://localhost:8080/shorten -H "Content-Type: application/json" -d '{"long_url": "https://example.com"}'

# Redirect
curl -vL http://localhost:8080/r/abc1234

# Delete (produces purge event; consumers remove from DB, Redis, LRU)
curl -X DELETE http://localhost:8080/r/abc1234
```

## Testing (Per Service)

Unit and integration tests use **pytest** with mocks for Kafka, Redis, Postgres, and the ID service. Target coverage is **>85%**.

### Shortener Service
```bash
cd shortener_service
pip install -r requirements.txt -r requirements-dev.txt
PYTHONPATH=.. python -m pytest tests/ -v --cov=main --cov-report=term-missing --cov-fail-under=85
```

### Redirection Service
```bash
cd redirection_service
pip install -r requirements.txt -r requirements-dev.txt
PYTHONPATH=.. python -m pytest tests/ -v --cov=main --cov-report=term-missing --cov-fail-under=85
```

### Deletion Service
```bash
cd deletion_service
pip install -r requirements.txt -r requirements-dev.txt
PYTHONPATH=.. python -m pytest tests/ -v --cov=main --cov-report=term-missing --cov-fail-under=85
```
