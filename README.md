# Amazon Reviews — Analytics with Apache Spark & Cassandra

Continuation of the previous homework using the same Amazon Reviews dataset (verified purchases, ~46k rows).
PySpark transforms and loads data into Cassandra, FastAPI exposes it via REST with Redis caching.

## Stack

- **PySpark** — data cleaning, transformation, aggregation
- **Cassandra 5.0** — one table per query pattern
- **Redis 7** — response caching, TTL 5 minutes
- **FastAPI** — REST API
- **Docker Compose** — orchestrates all services

## Files

**`docker-compose.yml`** — starts Cassandra, Redis and the API container

**`cql/01_schema.cql`** — Cassandra keyspace + all 6 table definitions

**`etl_to_cassandra/main.py`** — PySpark ETL: reads CSV → cleans → aggregates → loads all Cassandra tables

**`etl_to_cassandra/load_putput.txt`** — captured terminal output of a full ETL run

**`api/main.py`** — FastAPI entry point, lifespan, router registration

**`api/database.py`** — Cassandra singleton session and query helper

**`api/cache.py`** — Redis singleton, `cache_get` / `cache_set` / `cache_flush` with silent fallback on outage

**`api/routes/products.py`** — endpoints 1 & 2: reviews by product, reviews by product + star rating

**`api/routes/customers.py`** — endpoint 3: reviews by customer

**`api/routes/top.py`** — endpoints 4–7: top products, top customers, top haters, top backers

**`api/Dockerfile`** — builds the API image

**`screenshots_with_demonstation/`** — `fast_api_int.png`, `fastapi_endpoint.png`, `endpoing_test.png`

## How to Run

**1. Start services**
```bash
docker-compose up --build
```

**2. Load Cassandra schema**
```bash
docker exec -i cassandra_reviews cqlsh < cql/01_schema.cql
```

**3. Run ETL** (place `amazon_reviews.csv` inside `etl_to_cassandra/`)
* make sure create cassandra user before run, and update .env file
```bash
cd etl_to_cassandra
python main.py
```

**4. Open API docs**
http://localhost:8000/docs

check screenshots for `screenshots_with_demonstation/` for expected interface
