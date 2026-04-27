from fastapi.responses import JSONResponse
from fastapi import APIRouter

from database import query
from cache import cache_get, cache_set

router = APIRouter(prefix="/reviews", tags=["Reviews"])


@router.get("/customer/{customer_id}",
            summary="Endpoint 3 — All reviews by a customer")
def get_reviews_by_customer(customer_id: str):
    """
    Returns all reviews written by customer_id.
    Uses separate table reviews_by_customer — no ALLOW FILTERING needed.
    """
    key = f"customer:{customer_id}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    data = query(
        "SELECT * FROM reviews_by_customer WHERE customer_id = %s",
        (customer_id,)
    )
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})