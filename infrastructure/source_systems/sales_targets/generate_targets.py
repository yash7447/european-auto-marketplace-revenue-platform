import csv
import random
from datetime import date
from pathlib import Path


SEED = 42
random.seed(SEED)

OUTPUT_DIR = Path("/data")


SALES_TEAMS = {
    "DE": [
        "Anna Keller",
        "Max Weber",
        "Laura Schmidt",
        "Felix Wagner",
    ],
    "IT": [
        "Luca Rossi",
        "Giulia Romano",
        "Marco Bianchi",
    ],
    "NL": [
        "Eva de Vries",
        "Daan Jansen",
    ],
    "BE": [
        "Sophie Peeters",
        "Lucas Dubois",
    ],
    "AT": [
        "Paul Gruber",
        "Lisa Bauer",
    ],
    "FR": [
        "Camille Martin",
        "Louis Bernard",
        "Emma Robert",
    ],
}


PRODUCT_GROUPS = {
    "BASIC": {
        "revenue_base": 65_000,
        "upsell_base": 8_000,
        "new_dealer_base": 15,
    },

    "PREMIUM": {
        "revenue_base": 130_000,
        "upsell_base": 30_000,
        "new_dealer_base": 10,
    },

    "VISIBILITY": {
        "revenue_base": 45_000,
        "upsell_base": 18_000,
        "new_dealer_base": 8,
    },

    "LEADS": {
        "revenue_base": 35_000,
        "upsell_base": 15_000,
        "new_dealer_base": 6,
    },

    "DIGITAL_SERVICES": {
        "revenue_base": 25_000,
        "upsell_base": 12_000,
        "new_dealer_base": 5,
    },
}


MARKET_FACTORS = {
    "DE": 1.30,
    "IT": 0.90,
    "NL": 0.80,
    "BE": 0.72,
    "AT": 0.68,
    "FR": 1.05,
}


def month_range(start_year, start_month, end_year, end_month):

    months = []

    year = start_year
    month = start_month

    while (year, month) <= (end_year, end_month):

        months.append(
            date(year, month, 1)
        )

        if month == 12:
            year += 1
            month = 1

        else:
            month += 1

    return months


def seasonal_factor(month):

    factors = {
        1: 0.92,
        2: 0.95,
        3: 1.00,
        4: 1.03,
        5: 1.05,
        6: 1.06,
        7: 1.02,
        8: 0.94,
        9: 1.08,
        10: 1.10,
        11: 1.12,
        12: 1.06,
    }

    return factors[month]


def annual_growth_factor(year):

    return {
        2024: 1.00,
        2025: 1.08,
        2026: 1.17,
    }[year]


def generate_targets():

    rows = []

    months = month_range(
        2024,
        1,
        2026,
        12
    )

    target_id = 1

    for month_date in months:

        for market, reps in SALES_TEAMS.items():

            for sales_rep in reps:

                # Slightly different expectations per rep.
                rep_factor = random.uniform(
                    0.90,
                    1.10
                )

                for product_group, values in PRODUCT_GROUPS.items():

                    base_factor = (
                        MARKET_FACTORS[market]
                        * seasonal_factor(month_date.month)
                        * annual_growth_factor(month_date.year)
                        * rep_factor
                    )

                    revenue_target = round(
                        values["revenue_base"]
                        * base_factor,
                        2
                    )

                    upsell_target = round(
                        values["upsell_base"]
                        * base_factor,
                        2
                    )

                    new_dealer_target = round(
                        values["new_dealer_base"]
                        * MARKET_FACTORS[market]
                        * seasonal_factor(
                            month_date.month
                        )
                    )

                    retention_target_pct = round(
                        random.uniform(
                            93.0,
                            98.5
                        ),
                        2
                    )

                    rows.append({
                        "target_id":
                            f"T{target_id:07d}",

                        "target_month":
                            month_date.strftime(
                                "%Y-%m"
                            ),

                        "market":
                            market,

                        "sales_rep":
                            sales_rep,

                        "product_group":
                            product_group,

                        "revenue_target_eur":
                            revenue_target,

                        "upsell_target_eur":
                            upsell_target,

                        "new_dealer_target":
                            new_dealer_target,

                        "retention_target_pct":
                            retention_target_pct,

                        "planning_version":
                            f"BUDGET_{month_date.year}",

                        "created_at":
                            f"{month_date.year}-01-01",
                    })

                    target_id += 1

    return rows


def write_csv(rows):

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        OUTPUT_DIR /
        "sales_targets.csv"
    )

    fieldnames = [
        "target_id",
        "target_month",
        "market",
        "sales_rep",
        "product_group",
        "revenue_target_eur",
        "upsell_target_eur",
        "new_dealer_target",
        "retention_target_pct",
        "planning_version",
        "created_at",
    ]

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(
            rows
        )

    return output_file


def main():

    print(
        "Generating commercial sales targets..."
    )

    rows = generate_targets()

    output_file = write_csv(
        rows
    )

    print()
    print(
        f"Generated target records: "
        f"{len(rows):,}"
    )

    print(
        f"Output file: {output_file}"
    )


if __name__ == "__main__":
    main()