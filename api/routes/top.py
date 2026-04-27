from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

from database import query
from cache import cache_get, cache_set

router = APIRouter(prefix="/top", tags=["Top Lists"])


def _month_range(start: str, end: str) -> list[str]:
    """Generate YYYY-MM strings between start and end inclusive."""
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    months, y, m = [], sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return months


def _validate_period(start: str, end: str):
    try:
        sy, sm = map(int, start.split("-"))
        ey, em = map(int, end.split("-"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Period must be YYYY-MM format")
    if (sy, sm) > (ey, em):
        raise HTTPException(status_code=400, detail="start must be <= end")


@router.get("/products",
            summary="Endpoint 4 — N most reviewed products for a period")
def get_top_products(
    start: str = Query(..., example="2014-01", description="Start month YYYY-MM"),
    end:   str = Query(..., example="2014-06", description="End month YYYY-MM"),
    n:     int = Query(10, ge=1, le=100),
):
    """
    Queries top_products_by_period once per month in range,
    merges counts in Python, returns top N.
    No ALLOW FILTERING — year_month is the partition key.
    """
    _validate_period(start, end)
    key = f"top:products:{start}:{end}:{n}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    aggregated: dict = {}
    for ym in _month_range(start, end):
        for row in query(
            "SELECT product_id, product_title, review_count FROM top_products_by_period WHERE year_month = %s",
            (ym,)
        ):
            pid = row["product_id"]
            if pid not in aggregated:
                aggregated[pid] = {"product_id": pid, "product_title": row["product_title"], "review_count": 0}
            aggregated[pid]["review_count"] += row["review_count"]

    data = sorted(aggregated.values(), key=lambda x: x["review_count"], reverse=True)[:n]
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})


@router.get("/customers",
            summary="Endpoint 5 — N most productive customers (verified purchases)")
def get_top_customers(
    start: str = Query(..., example="2014-01"),
    end:   str = Query(..., example="2014-06"),
    n:     int = Query(10, ge=1, le=100),
):
    """
    Queries top_customers_by_period once per month, merges in Python.
    Only verified purchase reviews counted — filtered at ETL time.
    """
    _validate_period(start, end)
    key = f"top:customers:{start}:{end}:{n}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    aggregated: dict = {}
    for ym in _month_range(start, end):
        for row in query(
            "SELECT customer_id, review_count FROM top_customers_by_period WHERE year_month = %s",
            (ym,)
        ):
            cid = row["customer_id"]
            aggregated[cid] = aggregated.get(cid, 0) + row["review_count"]

    data = sorted(
        [{"customer_id": k, "review_count": v} for k, v in aggregated.items()],
        key=lambda x: x["review_count"], reverse=True
    )[:n]
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})


@router.get("/haters",
            summary="Endpoint 6 — N most productive haters (1 or 2 star reviews)")
def get_top_haters(
    start: str = Query(..., example="2014-01"),
    end:   str = Query(..., example="2014-06"),
    n:     int = Query(10, ge=1, le=100),
):
    """
    Queries top_haters_by_period once per month, merges in Python.
    Low star = 1 or 2 stars, filtered at ETL time.
    """
    _validate_period(start, end)
    key = f"top:haters:{start}:{end}:{n}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    aggregated: dict = {}
    for ym in _month_range(start, end):
        for row in query(
            "SELECT customer_id, low_star_count FROM top_haters_by_period WHERE year_month = %s",
            (ym,)
        ):
            cid = row["customer_id"]
            aggregated[cid] = aggregated.get(cid, 0) + row["low_star_count"]

    data = sorted(
        [{"customer_id": k, "low_star_count": v} for k, v in aggregated.items()],
        key=lambda x: x["low_star_count"], reverse=True
    )[:n]
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})


@router.get("/backers",
            summary="Endpoint 7 — N most productive backers (4 or 5 star reviews)")
def get_top_backers(
    start: str = Query(..., example="2014-01"),
    end:   str = Query(..., example="2014-06"),
    n:     int = Query(10, ge=1, le=100),
):
    """
    Queries top_backers_by_period once per month, merges in Python.
    High star = 4 or 5 stars, filtered at ETL time.
    """
    _validate_period(start, end)
    key = f"top:backers:{start}:{end}:{n}"
    cached = cache_get(key)
    if cached is not None:
        return JSONResponse({"cache": "HIT", "count": len(cached), "data": cached})

    aggregated: dict = {}
    for ym in _month_range(start, end):
        for row in query(
            "SELECT customer_id, high_star_count FROM top_backers_by_period WHERE year_month = %s",
            (ym,)
        ):
            cid = row["customer_id"]
            aggregated[cid] = aggregated.get(cid, 0) + row["high_star_count"]

    data = sorted(
        [{"customer_id": k, "high_star_count": v} for k, v in aggregated.items()],
        key=lambda x: x["high_star_count"], reverse=True
    )[:n]
    cache_set(key, data)
    return JSONResponse({"cache": "MISS", "count": len(data), "data": data})