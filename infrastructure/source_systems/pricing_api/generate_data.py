from pathlib import Path
from datetime import date, timedelta
import random

import pandas as pd


SEED = 42
random.seed(SEED)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


PRODUCTS = [
    {
        "product_id": "BASIC",
        "product_name": "Basic Dealer Package",
        "product_family": "SUBSCRIPTION",
        "base_price": 399.00,
    },
    {
        "product_id": "PREMIUM",
        "product_name": "Premium Dealer Package",
        "product_family": "SUBSCRIPTION",
        "base_price": 899.00,
    },
    {
        "product_id": "VISIBILITY_BOOST",
        "product_name": "Visibility Boost",
        "product_family": "VISIBILITY",
        "base_price": 149.00,
    },
    {
        "product_id": "SOCIAL_BOOST",
        "product_name": "Social Media Boost",
        "product_family": "VISIBILITY",
        "base_price": 199.00,
    },
    {
        "product_id": "LEAD_PACKAGE",
        "product_name": "Lead Package",
        "product_family": "LEADS",
        "base_price": 249.00,
    },
    {
        "product_id": "FEATURED_LISTING",
        "product_name": "Featured Listing",
        "product_family": "VISIBILITY",
        "base_price": 99.00,
    },
    {
        "product_id": "TOP_LISTING",
        "product_name": "Top Listing",
        "product_family": "VISIBILITY",
        "base_price": 129.00,
    },
    {
        "product_id": "DEALER_WEBSITE",
        "product_name": "Dealer Website",
        "product_family": "DIGITAL_SERVICES",
        "base_price": 179.00,
    },
    {
        "product_id": "LEAD_PLUS",
        "product_name": "Lead Plus",
        "product_family": "LEADS",
        "base_price": 349.00,
    },
    {
        "product_id": "ANALYTICS_PRO",
        "product_name": "Dealer Analytics Pro",
        "product_family": "ANALYTICS",
        "base_price": 159.00,
    },
    {
        "product_id": "INVENTORY_SYNC",
        "product_name": "Inventory Sync",
        "product_family": "INTEGRATION",
        "base_price": 119.00,
    },
    {
        "product_id": "MULTI_MARKET",
        "product_name": "Multi Market Package",
        "product_family": "SUBSCRIPTION",
        "base_price": 599.00,
    },
]


MARKET_FACTORS = {
    "DE": 1.00,
    "IT": 0.82,
    "NL": 0.90,
    "BE": 0.88,
    "AT": 0.92,
    "FR": 0.95,
}


SEGMENT_FACTORS = {
    "SMB": 1.00,
    "MID_MARKET": 1.08,
    "ENTERPRISE": 1.15,
}


QUARTERS = [
    date(2024, 1, 1),
    date(2024, 4, 1),
    date(2024, 7, 1),
    date(2024, 10, 1),

    date(2025, 1, 1),
    date(2025, 4, 1),
    date(2025, 7, 1),
    date(2025, 10, 1),

    date(2026, 1, 1),
    date(2026, 4, 1),
    date(2026, 7, 1),
]


def quarter_name(d):
    quarter = ((d.month - 1) // 3) + 1
    return f"{d.year}Q{quarter}"


def create_products():

    rows = []

    for product in PRODUCTS:

        rows.append({
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_family": product["product_family"],
            "billing_frequency": "MONTHLY",
            "active": True,
            "updated_at": "2026-09-01"
        })

    return pd.DataFrame(rows)


def create_prices():

    rows = []
    price_id = 1

    for product in PRODUCTS:

        for market, market_factor in MARKET_FACTORS.items():

            for segment, segment_factor in SEGMENT_FACTORS.items():

                for period_index, effective_from in enumerate(QUARTERS):

                    if period_index < len(QUARTERS) - 1:
                        effective_to = (
                            QUARTERS[period_index + 1]
                            - timedelta(days=1)
                        )
                    else:
                        effective_to = None

                    # Small normal price growth over time
                    trend_factor = 1 + (period_index * 0.012)

                    price = (
                        product["base_price"]
                        * market_factor
                        * segment_factor
                        * trend_factor
                    )

                    # Deliberate pricing event:
                    # Germany PREMIUM gets a stronger increase
                    # from 2026 Q3 onward.
                    if (
                        product["product_id"] == "PREMIUM"
                        and market == "DE"
                        and effective_from >= date(2026, 7, 1)
                    ):
                        price *= 1.10

                    # Another smaller market/product pricing change
                    if (
                        product["product_id"] == "VISIBILITY_BOOST"
                        and market == "IT"
                        and effective_from >= date(2026, 4, 1)
                    ):
                        price *= 1.07

                    price = round(price, 2)

                    rows.append({
                        "price_id": f"P{price_id:07d}",
                        "product_id": product["product_id"],
                        "market": market,
                        "dealer_segment": segment,
                        "currency": "EUR",
                        "list_price": price,
                        "effective_from": effective_from.isoformat(),
                        "effective_to": (
                            effective_to.isoformat()
                            if effective_to
                            else None
                        ),
                        "pricing_version": quarter_name(
                            effective_from
                        ),
                        "updated_at": effective_from.isoformat()
                    })

                    price_id += 1

    return pd.DataFrame(rows)


def main():

    print("Generating pricing source data...")

    products = create_products()
    prices = create_prices()

    products.to_parquet(
        DATA_DIR / "products.parquet",
        index=False
    )

    prices.to_parquet(
        DATA_DIR / "prices.parquet",
        index=False
    )

    print()
    print("Generated:")
    print(f"Products: {len(products):,}")
    print(f"Prices:   {len(prices):,}")


if __name__ == "__main__":
    main()