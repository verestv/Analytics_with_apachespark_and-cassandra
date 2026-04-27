import json
import os

import redis

_client = None
REDIS_TTL = 300  # 5 minutes


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.getenv("REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
        )
    return _client


def cache_get(key: str):
    """Return cached value or None. Never crashes — Redis outage is silent."""
    try:
        val = get_redis().get(key)
        return json.loads(val) if val else None
    except Exception:
        return None  # Redis down → treat as cache miss, serve from Cassandra


def cache_set(key: str, value) -> None:
    """Store value with TTL. Never crashes — Redis outage is silent."""
    try:
        get_redis().setex(key, REDIS_TTL, json.dumps(value, default=str))
    except Exception:
        pass  # Redis down → just skip caching


def cache_flush() -> bool:
    """Flush all Redis keys. Returns True on success."""
    try:
        get_redis().flushdb()
        return True
    except Exception:
        return False