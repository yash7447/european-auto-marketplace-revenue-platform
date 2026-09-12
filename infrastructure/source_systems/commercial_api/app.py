from pathlib import Path
import math

import pandas as pd

from fastapi import FastAPI, Query


DATA_DIR = Path(__file__).parent / "data"


app = FastAPI(
    title="European Auto Marketplace Commercial API",
    description=(
        "Demo Salesforce-like commercial platform "
        "for revenue analytics"
    ),
    version="1.0"
)


accounts = pd.read_parquet(
    DATA_DIR / "accounts.parquet"
)

contracts = pd.read_parquet(
    DATA_DIR / "contracts.parquet"
)

subscriptions = pd.read_parquet(
    DATA_DIR / "subscriptions.parquet"
)

invoices = pd.read_parquet(
    DATA_DIR / "invoices.parquet"
)


def paginate(
    df,
    page,
    page_size,
    updated_since=None
):

    filtered = df

    if updated_since:

        filtered = filtered[
            filtered["updated_at"]
            >= updated_since
        ]

    total_records = len(filtered)

    total_pages = math.ceil(
        total_records / page_size
    )

    start = (page - 1) * page_size
    end = start + page_size

    data = (
        filtered
        .iloc[start:end]
        .to_dict(orient="records")
    )

    return {
        "page": page,
        "page_size": page_size,
        "total_records": total_records,
        "total_pages": total_pages,
        "data": data
    }


@app.get("/health")
def health():

    return {
        "status": "healthy",
        "accounts": len(accounts),
        "contracts": len(contracts),
        "subscriptions": len(subscriptions),
        "invoices": len(invoices)
    }


@app.get("/accounts")
def get_accounts(
    page: int = Query(1, ge=1),
    page_size: int = Query(
        500,
        ge=1,
        le=1000
    ),
    updated_since: str | None = None
):

    return paginate(
        accounts,
        page,
        page_size,
        updated_since
    )


@app.get("/contracts")
def get_contracts(
    page: int = Query(1, ge=1),
    page_size: int = Query(
        500,
        ge=1,
        le=1000
    ),
    updated_since: str | None = None
):

    return paginate(
        contracts,
        page,
        page_size,
        updated_since
    )


@app.get("/subscriptions")
def get_subscriptions(
    page: int = Query(1, ge=1),
    page_size: int = Query(
        500,
        ge=1,
        le=1000
    ),
    updated_since: str | None = None
):

    return paginate(
        subscriptions,
        page,
        page_size,
        updated_since
    )


@app.get("/invoices")
def get_invoices(
    page: int = Query(1, ge=1),
    page_size: int = Query(
        500,
        ge=1,
        le=1000
    ),
    updated_since: str | None = None
):

    return paginate(
        invoices,
        page,
        page_size,
        updated_since
    )