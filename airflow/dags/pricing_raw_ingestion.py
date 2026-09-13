import json
import os
from datetime import timedelta
from urllib.parse import urlencode
from urllib.request import urlopen

import boto3
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context

from platform_utils.glue_partitions import register_partition


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET_NAME = (
    "european-auto-marketplace-data-yash-2026-01"
)

AWS_REGION = "us-east-1"

API_BASE_URL = (
    "http://pricing-api:8000"
)

RAW_DATABASE = "auto_marketplace_raw"

PAGE_SIZE = 500


# ============================================================
# RAW TABLE MAPPING
# ============================================================

RAW_TABLES = {
    "products": "raw_pricing_products",
    "prices": "raw_pricing_prices",
}


# ============================================================
# AWS / S3
# ============================================================

def get_s3_client():

    profile = os.getenv(
        "AWS_PROFILE",
        "auto-marketplace"
    )

    session = boto3.Session(
        profile_name=profile,
        region_name=AWS_REGION,
    )

    return session.client(
        "s3"
    )


# ============================================================
# SAFE AIRFLOW RUN ID
# ============================================================

def safe_run_id(run_id):

    return (
        run_id
        .replace(":", "-")
        .replace("+", "_")
    )


# ============================================================
# BUILD RAW S3 PREFIX
# ============================================================

def build_raw_prefix(
    dataset,
    extract_date,
    run_id,
):

    return (
        f"raw/pricing/{dataset}/"
        f"extract_date={extract_date}/"
        f"run_id={run_id}/"
    )


# ============================================================
# REGISTER GLUE PARTITION
# ============================================================

def register_pricing_partition(
    dataset,
    extract_date,
    run_id,
):

    table_name = (
        RAW_TABLES[dataset]
    )

    raw_prefix = build_raw_prefix(
        dataset=dataset,
        extract_date=extract_date,
        run_id=run_id,
    )

    s3_location = (
        f"s3://{BUCKET_NAME}/"
        f"{raw_prefix}"
    )

    print(
        "Registering Pricing RAW partition..."
    )

    print(
        f"Dataset: {dataset}"
    )

    print(
        f"Table: "
        f"{RAW_DATABASE}.{table_name}"
    )

    print(
        f"extract_date: "
        f"{extract_date}"
    )

    print(
        f"run_id: "
        f"{run_id}"
    )

    print(
        f"S3 location: "
        f"{s3_location}"
    )

    created = register_partition(
        database_name=RAW_DATABASE,
        table_name=table_name,
        partition_values={
            "extract_date": extract_date,
            "run_id": run_id,
        },
        s3_location=s3_location,
        region_name=AWS_REGION,
    )

    if created:

        print(
            "Glue partition successfully "
            "registered."
        )

    else:

        print(
            "Glue partition already exists."
        )


# ============================================================
# PRODUCTS
#
# Full snapshot every run.
# ============================================================

def extract_products():

    context = get_current_context()

    run_id = safe_run_id(
        context["run_id"]
    )

    extract_date = (
        context["logical_date"]
        .strftime("%Y-%m-%d")
    )

    s3 = get_s3_client()

    raw_prefix = build_raw_prefix(
        dataset="products",
        extract_date=extract_date,
        run_id=run_id,
    )

    page = 1
    total_records = 0
    total_pages_written = 0


    # --------------------------------------------------------
    # Extract API pages
    # --------------------------------------------------------

    while True:

        params = {
            "page": page,
            "page_size": PAGE_SIZE,
        }

        url = (
            f"{API_BASE_URL}/products?"
            f"{urlencode(params)}"
        )

        print(
            f"Requesting {url}"
        )

        with urlopen(
            url,
            timeout=30
        ) as response:

            payload = json.loads(
                response
                .read()
                .decode("utf-8")
            )

        records = payload.get(
            "data",
            []
        )

        total_pages = payload.get(
            "total_pages",
            1
        )

        if not records:

            print(
                "No product records returned."
            )

            break


        # ----------------------------------------------------
        # RAW API page -> S3
        # ----------------------------------------------------

        s3_key = (
            f"{raw_prefix}"
            f"page={page:04d}.json"
        )

        print(
            f"Writing "
            f"{len(records)} products to "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(
                payload,
                ensure_ascii=False
            ).encode(
                "utf-8"
            ),
            ContentType=(
                "application/json"
            ),
        )

        total_records += (
            len(records)
        )

        total_pages_written += 1

        print(
            f"Uploaded products page "
            f"{page}: "
            f"{len(records)} records"
        )

        if page >= total_pages:

            break

        page += 1


    # ========================================================
    # MANIFEST
    # ========================================================

    manifest = {
        "dataset": "products",
        "mode": "full_snapshot",
        "extract_date": extract_date,
        "run_id": run_id,
        "records": total_records,
        "pages": total_pages_written,
    }

    manifest_key = (
        f"{raw_prefix}"
        "_manifest.json"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=2
        ).encode(
            "utf-8"
        ),
        ContentType=(
            "application/json"
        ),
    )


    # ========================================================
    # REGISTER GLUE PARTITION
    # ========================================================

    if total_records > 0:

        register_pricing_partition(
            dataset="products",
            extract_date=extract_date,
            run_id=run_id,
        )

    else:

        print(
            "No products written. "
            "Skipping Glue partition registration."
        )


    print(
        "Products extraction complete: "
        f"{manifest}"
    )


# ============================================================
# PRICES
#
# Incremental after initial full load.
# ============================================================

def extract_prices():

    context = get_current_context()

    run_id = safe_run_id(
        context["run_id"]
    )

    extract_date = (
        context["logical_date"]
        .strftime("%Y-%m-%d")
    )


    # --------------------------------------------------------
    # Watermark
    # --------------------------------------------------------

    watermark_name = (
        "pricing_prices_watermark"
    )

    previous_watermark = (
        Variable.get(
            watermark_name,
            default_var=None
        )
    )


    # Capture this BEFORE extraction.
    #
    # It only becomes the persisted watermark
    # after:
    #
    # API extraction
    # S3 page writes
    # manifest
    # Glue registration
    #
    # all succeed.
    new_watermark = (
        pendulum.now("UTC")
    )


    if previous_watermark:

        mode = "incremental"

        print(
            "Previous price watermark: "
            f"{previous_watermark}"
        )

    else:

        mode = "full"

        print(
            "No pricing watermark exists. "
            "Running initial full extraction."
        )


    s3 = get_s3_client()

    raw_prefix = build_raw_prefix(
        dataset="prices",
        extract_date=extract_date,
        run_id=run_id,
    )

    page = 1
    total_records = 0
    total_pages_written = 0


    # ========================================================
    # API EXTRACTION
    # ========================================================

    while True:

        params = {
            "page": page,
            "page_size": PAGE_SIZE,
        }

        if previous_watermark:

            params["updated_since"] = (
                previous_watermark
            )


        url = (
            f"{API_BASE_URL}/prices?"
            f"{urlencode(params)}"
        )

        print(
            f"Requesting {url}"
        )


        with urlopen(
            url,
            timeout=30
        ) as response:

            payload = json.loads(
                response
                .read()
                .decode("utf-8")
            )


        records = payload.get(
            "data",
            []
        )

        total_pages = payload.get(
            "total_pages",
            1
        )


        if not records:

            print(
                "No more price records "
                "returned."
            )

            break


        # ----------------------------------------------------
        # RAW API page -> S3
        # ----------------------------------------------------

        s3_key = (
            f"{raw_prefix}"
            f"page={page:04d}.json"
        )

        print(
            f"Writing "
            f"{len(records)} prices to "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(
                payload,
                ensure_ascii=False
            ).encode(
                "utf-8"
            ),
            ContentType=(
                "application/json"
            ),
        )


        total_records += (
            len(records)
        )

        total_pages_written += 1


        print(
            f"Uploaded prices page "
            f"{page}: "
            f"{len(records)} records"
        )


        if page >= total_pages:

            break

        page += 1


    # ========================================================
    # MANIFEST
    # ========================================================

    manifest = {

        "dataset": "prices",

        "mode": mode,

        "extract_date": (
            extract_date
        ),

        "run_id": (
            run_id
        ),

        "previous_watermark": (
            previous_watermark
        ),

        "new_watermark": (
            new_watermark
            .to_iso8601_string()
        ),

        "records": (
            total_records
        ),

        "pages": (
            total_pages_written
        ),
    }


    manifest_key = (
        f"{raw_prefix}"
        "_manifest.json"
    )


    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=2
        ).encode(
            "utf-8"
        ),
        ContentType=(
            "application/json"
        ),
    )


    # ========================================================
    # REGISTER GLUE PARTITION
    # ========================================================

    if total_records > 0:

        register_pricing_partition(
            dataset="prices",
            extract_date=extract_date,
            run_id=run_id,
        )

    else:

        print(
            "No new price records. "
            "Skipping Glue partition registration."
        )


    # ========================================================
    # ADVANCE WATERMARK
    #
    # IMPORTANT:
    #
    # If partition registration fails,
    # this line is never reached.
    # ========================================================

    Variable.set(
        watermark_name,
        new_watermark
        .to_iso8601_string()
    )


    print(
        "Updated pricing watermark: "
        f"{new_watermark.to_iso8601_string()}"
    )


    print(
        "Prices extraction complete: "
        f"{manifest}"
    )


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id="pricing_raw_ingestion",

    description=(
        "Extract Pricing API products and "
        "incremental prices into AWS S3 RAW "
        "and register exact Glue partitions"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="UTC"
    ),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    default_args={
        "retries": 2,
        "retry_delay": timedelta(
            seconds=30
        ),
    },

    tags=[
        "pricing",
        "api",
        "raw",
        "s3",
        "glue",
        "incremental",
    ],

) as dag:


    extract_products_task = (
        PythonOperator(
            task_id=(
                "extract_products"
            ),
            python_callable=(
                extract_products
            ),
        )
    )


    extract_prices_task = (
        PythonOperator(
            task_id=(
                "extract_prices"
            ),
            python_callable=(
                extract_prices
            ),
        )
    )