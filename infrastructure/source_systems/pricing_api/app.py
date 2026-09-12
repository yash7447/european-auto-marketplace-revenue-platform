from pathlib import Path
import math

import pandas as pd
from fastapi import FastAPI, Query


DATA_DIR = Path(__file__).parent / "data"


app = FastAPI(
    title="European Auto Marketplace Pricing API",
    description="Product catalogue and historical market pricing",
    version="1.0"
)


products = pd.read_parquet(
    DATA_DIR / "products.parquet"
)

prices = pd.read_parquet(
    DATA_DIR / "prices.parquet"
)


def paginate(df, page, page_size):

    total_records = len(df)

    total_pages = math.ceil(
        total_records / page_size
    )

    start = (page - 1) * page_size
    end = start + page_size

    return {
        "page": page,
        "page_size": page_size,
        "total_records": total_records,
        "total_pages": total_pages,
        "data": (
            df.iloc[start:end]
            .to_dict(orient="records")
        )
    }


@app.get("/health")
def health():

    return {
        "status": "healthy",
        "products": len(products),
        "prices": len(prices)
    }


@app.get("/products")
def get_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(
        100,
        ge=1,
        le=1000
    )
):

    return paginate(
        products,
        page,
        page_size
    )


@app.get("/prices")
def get_prices(
    page: int = Query(1, ge=1),
    page_size: int = Query(
        200,
        ge=1,
        le=1000
    ),
    market: str | None = None,
    product_id: str | None = None,
    dealer_segment: str | None = None,
    effective_on: str | None = None,
    updated_since: str | None = None
):

    filtered = prices.copy()

    if market:
        filtered = filtered[
            filtered["market"] == market.upper()
        ]

    if product_id:
        filtered = filtered[
            filtered["product_id"]
            == product_id.upper()
        ]

    if dealer_segment:
        filtered = filtered[
            filtered["dealer_segment"]
            == dealer_segment.upper()
        ]

    if updated_since:
        filtered = filtered[
            filtered["updated_at"]
            >= updated_since
        ]

    if effective_on:

        effective_to = (
            filtered["effective_to"]
            .fillna("9999-12-31")
        )

        filtered = filtered[
            (filtered["effective_from"] <= effective_on)
            &
            (effective_to >= effective_on)
        ]

    filtered = filtered.sort_values(
        [
            "product_id",
            "market",
            "dealer_segment",
            "effective_from"
        ]
    )

    return paginate(
        filtered,
        page,
        page_size
    )