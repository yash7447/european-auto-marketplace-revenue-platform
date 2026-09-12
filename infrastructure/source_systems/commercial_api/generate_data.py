from pathlib import Path
from datetime import date, timedelta
import random

import pandas as pd
from faker import Faker


SEED = 42

random.seed(SEED)
Faker.seed(SEED)

fake = Faker()

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


MARKETS = {
    "DE": {
        "sales_reps": [
            "Anna Keller",
            "Max Weber",
            "Laura Schmidt",
            "Felix Wagner"
        ]
    },
    "IT": {
        "sales_reps": [
            "Luca Rossi",
            "Giulia Romano",
            "Marco Bianchi"
        ]
    },
    "NL": {
        "sales_reps": [
            "Eva de Vries",
            "Daan Jansen"
        ]
    },
    "BE": {
        "sales_reps": [
            "Sophie Peeters",
            "Lucas Dubois"
        ]
    },
    "AT": {
        "sales_reps": [
            "Paul Gruber",
            "Lisa Bauer"
        ]
    },
    "FR": {
        "sales_reps": [
            "Camille Martin",
            "Louis Bernard",
            "Emma Robert"
        ]
    }
}


PRODUCTS = [
    "BASIC",
    "PREMIUM",
    "VISIBILITY_BOOST",
    "SOCIAL_BOOST",
    "LEAD_PACKAGE"
]


BASE_PRICES = {
    "BASIC": 399,
    "PREMIUM": 899,
    "VISIBILITY_BOOST": 149,
    "SOCIAL_BOOST": 199,
    "LEAD_PACKAGE": 249
}


def random_date(start: date, end: date):
    days = (end - start).days
    return start + timedelta(days=random.randint(0, days))


def create_accounts(n=5000):

    rows = []

    markets = list(MARKETS.keys())

    for i in range(1, n + 1):

        market = random.choice(markets)

        segment = random.choices(
            ["SMB", "MID_MARKET", "ENTERPRISE"],
            weights=[60, 30, 10]
        )[0]

        created = random_date(
            date(2023, 1, 1),
            date(2026, 8, 31)
        )

        updated = created + timedelta(
            days=random.randint(
                0,
                max(0, (date(2026, 9, 11) - created).days)
            )
        )

        rows.append({
            "dealer_id": f"D{i:06d}",
            "dealer_name": fake.company(),
            "market": market,
            "segment": segment,
            "sales_rep": random.choice(
                MARKETS[market]["sales_reps"]
            ),
            "account_status": random.choices(
                ["ACTIVE", "CHURNED", "SUSPENDED"],
                weights=[88, 9, 3]
            )[0],
            "created_date": created.isoformat(),
            "updated_at": updated.isoformat()
        })

    return pd.DataFrame(rows)


def create_contracts(accounts, n=8000):

    rows = []

    dealer_ids = accounts["dealer_id"].tolist()

    for i in range(1, n + 1):

        dealer_id = random.choice(dealer_ids)

        start_date = random_date(
            date(2024, 1, 1),
            date(2026, 8, 31)
        )

        contract_status = random.choices(
            ["ACTIVE", "EXPIRED", "CANCELLED"],
            weights=[75, 18, 7]
        )[0]

        if contract_status == "ACTIVE":
            end_date = None
        else:
            end_date = start_date + timedelta(
                days=random.randint(90, 720)
            )

        rows.append({
            "contract_id": f"C{i:06d}",
            "dealer_id": dealer_id,
            "contract_start_date": start_date.isoformat(),
            "contract_end_date":
                end_date.isoformat() if end_date else None,
            "contract_status": contract_status,
            "auto_renew": random.choice([True, True, True, False]),
            "updated_at": random_date(
                start_date,
                date(2026, 9, 11)
            ).isoformat()
        })

    return pd.DataFrame(rows)


def create_subscriptions(contracts, accounts, n=12000):

    rows = []

    contract_ids = contracts["contract_id"].tolist()

    contract_to_dealer = dict(
        zip(
            contracts["contract_id"],
            contracts["dealer_id"]
        )
    )

    dealer_to_market = dict(
        zip(
            accounts["dealer_id"],
            accounts["market"]
        )
    )

    for i in range(1, n + 1):

        contract_id = random.choice(contract_ids)

        dealer_id = contract_to_dealer[contract_id]

        market = dealer_to_market[dealer_id]

        product_id = random.choices(
            PRODUCTS,
            weights=[32, 30, 14, 12, 12]
        )[0]

        start_date = random_date(
            date(2024, 1, 1),
            date(2026, 8, 31)
        )

        status = random.choices(
            ["ACTIVE", "CANCELLED", "EXPIRED"],
            weights=[78, 12, 10]
        )[0]

        if status == "ACTIVE":
            end_date = None
        else:
            end_date = start_date + timedelta(
                days=random.randint(60, 600)
            )

        base_price = BASE_PRICES[product_id]

        market_factor = {
            "DE": 1.00,
            "IT": 0.82,
            "NL": 0.90,
            "BE": 0.88,
            "AT": 0.92,
            "FR": 0.95
        }[market]

        estimated_list_price = round(
            base_price * market_factor,
            2
        )

        discount_pct = round(
            max(
                0,
                random.gauss(6, 5)
            ),
            2
        )

        discount_pct = min(discount_pct, 30)

        contract_price = round(
            estimated_list_price
            * (1 - discount_pct / 100),
            2
        )

        rows.append({
            "subscription_id": f"S{i:06d}",
            "contract_id": contract_id,
            "dealer_id": dealer_id,
            "product_id": product_id,
            "subscription_start_date":
                start_date.isoformat(),
            "subscription_end_date":
                end_date.isoformat() if end_date else None,
            "contract_price": contract_price,
            "discount_pct": discount_pct,
            "billing_frequency":
                "MONTHLY",
            "subscription_status": status,
            "updated_at": random_date(
                start_date,
                date(2026, 9, 11)
            ).isoformat()
        })

    return pd.DataFrame(rows)


def create_invoices(subscriptions, n=60000):

    rows = []

    subscription_ids = subscriptions[
        "subscription_id"
    ].tolist()

    subscription_lookup = (
        subscriptions
        .set_index("subscription_id")
        .to_dict("index")
    )

    for i in range(1, n + 1):

        subscription_id = random.choice(
            subscription_ids
        )

        sub = subscription_lookup[
            subscription_id
        ]

        start_date = date.fromisoformat(
            sub["subscription_start_date"]
        )

        invoice_date = random_date(
            start_date,
            date(2026, 9, 11)
        )

        amount = sub["contract_price"]

        rows.append({
            "invoice_id": f"INV{i:07d}",
            "subscription_id": subscription_id,
            "dealer_id": sub["dealer_id"],
            "invoice_date": invoice_date.isoformat(),
            "invoice_amount": amount,
            "currency": "EUR",
            "payment_status": random.choices(
                ["PAID", "OPEN", "OVERDUE"],
                weights=[88, 8, 4]
            )[0],
            "updated_at": random_date(
                invoice_date,
                date(2026, 9, 11)
            ).isoformat()
        })

    return pd.DataFrame(rows)


def main():

    print("Generating commercial source data...")

    accounts = create_accounts()

    contracts = create_contracts(
        accounts
    )

    subscriptions = create_subscriptions(
        contracts,
        accounts
    )

    invoices = create_invoices(
        subscriptions
    )

    accounts.to_parquet(
        DATA_DIR / "accounts.parquet",
        index=False
    )

    contracts.to_parquet(
        DATA_DIR / "contracts.parquet",
        index=False
    )

    subscriptions.to_parquet(
        DATA_DIR / "subscriptions.parquet",
        index=False
    )

    invoices.to_parquet(
        DATA_DIR / "invoices.parquet",
        index=False
    )

    print()
    print("Generated:")
    print(f"Accounts:      {len(accounts):,}")
    print(f"Contracts:     {len(contracts):,}")
    print(f"Subscriptions: {len(subscriptions):,}")
    print(f"Invoices:      {len(invoices):,}")


if __name__ == "__main__":
    main()