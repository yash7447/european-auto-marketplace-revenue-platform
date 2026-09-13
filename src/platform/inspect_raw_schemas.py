import gzip
import json

import boto3


BUCKET = "european-auto-marketplace-data-yash-2026-01"

PREFIXES = {
    "commercial_accounts":
        "raw/commercial/accounts/",
    "commercial_contracts":
        "raw/commercial/contracts/",
    "commercial_subscriptions":
        "raw/commercial/subscriptions/",
    "commercial_invoices":
        "raw/commercial/invoices/",
    "pricing_products":
        "raw/pricing/products/",
    "pricing_prices":
        "raw/pricing/prices/",
}


session = boto3.Session(
    profile_name="auto-marketplace",
    region_name="us-east-1",
)

s3 = session.client("s3")


def find_first_data_file(prefix):

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
    ):

        for obj in page.get(
            "Contents",
            []
        ):

            key = obj["Key"]

            if (
                key.endswith(".json")
                and not key.endswith(
                    "_manifest.json"
                )
            ):
                return key

    return None


for dataset, prefix in PREFIXES.items():

    print()
    print("=" * 70)
    print(dataset)
    print("=" * 70)

    key = find_first_data_file(
        prefix
    )

    if not key:
        print(
            "No source file found."
        )
        continue

    print(
        f"S3 file: {key}"
    )

    response = s3.get_object(
        Bucket=BUCKET,
        Key=key,
    )

    payload = json.loads(
        response["Body"]
        .read()
        .decode("utf-8")
    )

    records = payload.get(
        "data",
        []
    )

    if not records:
        print(
            "No records in data array."
        )
        continue

    sample = records[0]

    print(
        json.dumps(
            sample,
            indent=2,
            default=str,
        )
    )

    print(
        "\nColumns:"
    )

    for key_name, value in sample.items():

        print(
            f"{key_name:30} "
            f"{type(value).__name__}"
        )