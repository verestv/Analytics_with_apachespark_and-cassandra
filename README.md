# Amazon Reviews — Analytics with Apache Spark & Cassandra

Continuation of the previous homework using the same Amazon Reviews dataset (verified purchases, ~46k rows).  
PySpark transforms and aggregates the data, loads it into Cassandra, and a FastAPI service exposes it via REST with Redis caching.

---

## Stack

| Component | Role |
|-----------|------|
| Apache Spark (PySpark) | Data cleaning, transformation, aggregation |
| Cassandra 5.0 | Storage — one table per query pattern, no `ALLOW FILTERING` |
| Redis 7 | Response caching, TTL 5 minutes |
| FastAPI | REST API |
| Docker Compose | Orchestrates all services |

---

## Repository Structure

```
.
├── docker-compose.yml          # Cassandra + Redis + API services
├── .env.example                # Environment variables template
├── cql/
│   └── schema.cql              # Cassandra keyspace + all table definitions
├── etl_to_cassandra/
│   └── main.py                 # PySpark ETL: reads CSV → transforms → loads Cassandra
├── api/
│   ├── Dockerfile              # API container build
│   ├── requirements.txt        # Python deps (exported from uv)
│   ├── main.py                 # FastAPI app entry point, lifespan, router registration
│   ├── database.py             # Cassandra singleton session + query helper
│   ├── cache.py                # Redis singleton + cache_get / cache_set / cache_flush
│   └── routes/
│       ├── products.py         # Endpoints 1 & 2 — reviews by product / product + stars
│       ├── customers.py        # Endpoint 3 — reviews by customer
│       └── top.py              # Endpoints 4–7 — top products, customers, haters, backers
└── screenshots_with_demonstation/
    └── *.png                   # Required demo screenshots
```

---

## Cassandra Schema Design

Each endpoint maps to a dedicated table so every query hits only the partition key — no `ALLOW FILTERING` anywhere.

| Table | Partition Key | Serves |
|-------|--------------|--------|
| `reviews_by_product` | `product_id` | Endpoints 1 & 2 (star_rating is clustering key) |
| `reviews_by_customer` | `customer_id` | Endpoint 3 |
| `top_products_by_period` | `year_month` | Endpoint 4 |
| `top_customers_by_period` | `year_month` | Endpoint 5 |
| `top_haters_by_period` | `year_month` | Endpoint 6 |
| `top_backers_by_period` | `year_month` | Endpoint 7 |

Endpoints 4–7 query Cassandra once per month in the requested range, then merge and rank results in Python.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/reviews/product/{product_id}` | All reviews for a product |
| GET | `/reviews/product/{product_id}/stars/{star_rating}` | Reviews for a product filtered by stars (1–5) |
| GET | `/reviews/customer/{customer_id}` | All reviews by a customer |
| GET | `/top/products?start=YYYY-MM&end=YYYY-MM&n=10` | N most reviewed products in period |
| GET | `/top/customers?start=YYYY-MM&end=YYYY-MM&n=10` | N most productive customers (verified purchases) |
| GET | `/top/haters?start=YYYY-MM&end=YYYY-MM&n=10` | N customers with most 1–2 star reviews |
| GET | `/top/backers?start=YYYY-MM&end=YYYY-MM&n=10` | N customers with most 4–5 star reviews |
| GET | `/health` | Health check |
| POST | `/cache/flush` | Flush Redis cache |

Every response includes `"cache": "HIT"` or `"cache": "MISS"` for visibility.

Interactive docs available at **http://localhost:8000/docs**

---

## Running

### 1. Environment

```bash
cp .env.example .env
# fill in values if needed (defaults work for local Docker)
```

### 2. Start services

```bash
docker-compose up --build
```

### 3. Create Cassandra schema

```bash
docker exec -i cassandra_reviews cqlsh < cql/schema.cql
```

### 4. Run ETL (place `amazon_reviews.csv` in `etl_to_cassandra/`)

```bash
cd etl_to_cassandra
python main.py
```

### 5. Test API

```
http://localhost:8000/docs
```
