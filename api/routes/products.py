from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from database import query
from cache import cache_get, cache_set

router = APIRouter(prefix="/reviews", tags=["Reviews"])


@router.get("/product/{product_id}",
            summary="Endpoint 1 — All reviews for a product")
def get_reviews_by_product(product_id: str):
    """Returns all reviews for the given product_id."""
    key = f"product:{product_id}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    data = query(
        "SELECT * FROM reviews_by_product WHERE product_id = %s",
        (product_id,)
    )
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})


@router.get("/product/{product_id}/stars/{star_rating}",
            summary="Endpoint 2 — Reviews for a product filtered by star rating")
def get_reviews_by_product_and_stars(product_id: str, star_rating: int):
    """
    Returns reviews for product_id with given star_rating.
    star_rating is a clustering key — no ALLOW FILTERING needed.
    """
    if star_rating not in (1, 2, 3, 4, 5):
        raise HTTPException(status_code=400, detail="star_rating must be between 1 and 5")

    key = f"product:{product_id}:stars:{star_rating}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    data = query(
        "SELECT * FROM reviews_by_product WHERE product_id = %s AND star_rating = %s",
        (product_id, star_rating)
    )
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})