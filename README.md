# Distributed URL Shortener

Microservice-based URL shortener with Snowflake ID generation, multi-tier cache-aside (Local LRU → Redis → Postgres), and Redpanda for cache invalidation.

## Architecture

- **ID Service**: Standalone FastAPI service that generates 64-bit Snowflake IDs. Uses `WORKER_ID` from environment; includes clock drift protection.
- **URL Service**: Shorten (ID service → Base62 → Postgres + Redis), Redirect (LRU → Redis → Postgres), Delete (Postgres + Redpanda purge event). Background consumer invalidates Local LRU and Redis on purge events.
- **Redpanda**: Single-node Kafka-compatible broker for `url-purge-events` topic.
- **Postgres**: Persistent store for short_path → long_url.
- **Redis**: Distributed cache (7-day TTL).

## Run with Docker Compose

```bash
cd url_shortener
docker compose up --build
```

- ID Service: http://localhost:8000 (GET `/generate` returns 64-bit integer)
- URL Service: http://localhost:8080 — **Demo UI** at http://localhost:8080/ (POST `/shorten`, GET `/r/{path}`, DELETE `/r/{path}`)

## Environment

| Service       | Variable         | Default                          |
|---------------|------------------|----------------------------------|
| ID Service    | `WORKER_ID`      | `0` (required in production)    |
| ID Service    | `EPOCH_MS`       | `1739980800000`                  |
| URL Service   | `ID_SERVICE_URL` | `http://id-service:8000`         |
| URL Service   | `DATABASE_URL`   | postgresql://...@postgres/...    |
| URL Service   | `REDIS_URL`      | redis://redis:6379/0             |
| URL Service   | `KAFKA_BOOTSTRAP`| redpanda:9092                    |
| URL Service   | `PURGE_TOPIC`    | url-purge-events                 |
| URL Service   | `LRU_MAX_SIZE`   | 10000                            |

## Observability

Logs are prefixed for filtering:

- **\[ID-SERVICE]** e.g. `Generated ID: 12345`, clock drift warnings/errors
- **\[URL-SERVICE]** e.g. `Shorten: id=... -> path=...`, `Redirect cache hit (LRU): ...`, `Purging Cache for Key: ...`, `Produced purge event for key: ...`

## Example

```bash
# Shorten
curl -X POST http://localhost:8080/shorten -H "Content-Type: application/json" -d '{"long_url": "https://example.com"}'

# Redirect
curl -vL http://localhost:8080/r/abc1234

# Delete (produces purge event; consumers remove from DB, Redis, LRU)
curl -X DELETE http://localhost:8080/r/abc1234
```

## Testing (URL Service)

Unit and integration tests use **pytest** with mocks for Kafka, Redis, Postgres, and the ID service. Target coverage is **>85%**.

```bash
cd url_service
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest tests/ -v --cov=main --cov-report=term-missing --cov-fail-under=85
```

- **Unit tests** (`tests/test_unit.py`): Base62 encode/decode, LRUCache, `_redis_ttl`, `_short_path_from_message`.
- **API tests** (`tests/test_api.py`): Shorten, redirect, delete with mocked DB, Redis, Kafka, and httpx.
- **Service tests** (`tests/test_services.py`): `init_db`, `send_purge_event`, `ensure_purge_topic` with mocks.

To run inside Docker (same Python as production, no Kafka/Redis/Postgres needed):

```bash
cd url_service
docker build -t url-service-test -f Dockerfile.test .
docker run --rm url-service-test
```
