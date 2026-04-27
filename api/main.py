from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

import database
from cache import cache_flush
from routes import products, customers, top


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.get_session()   # connect on startup
    yield
    database.shutdown()      # clean disconnect on shutdown


app = FastAPI(
    title="Amazon Reviews API",
    description="REST API over Cassandra with Redis caching (TTL 5 min)",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(products.router)
app.include_router(customers.router)
app.include_router(top.router)


@app.get("/health", tags=["Ops"])
def health():
    return {"status": "ok", "cassandra": "connected", "redis": "connected"}


@app.post("/cache/flush", tags=["Ops"])
def flush_cache():
    """Flush all Redis keys. Useful for testing to guarantee a fresh MISS."""
    ok = cache_flush()
    return JSONResponse({"flushed": ok})