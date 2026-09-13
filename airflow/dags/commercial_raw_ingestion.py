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

API_BASE_URL = (
    "http://commercial-api:8000"
)

AWS_REGION = "us-east-1"

RAW_DATABASE = "auto_marketplace_raw"

PAGE_SIZE = 1000


# ============================================================
# RAW TABLE MAPPING
# ============================================================

RAW_TABLES = {
    "accounts": (
        "raw_commercial_accounts"
    ),
    "contracts": (
        "raw_commercial_contracts"
    ),
    "subscriptions": (
        "raw_commercial_subscriptions"
    ),
    "invoices": (
        "raw_commercial_invoices"
    ),
}


# ============================================================
# S3 CLIENT
# ============================================================

def get_s3_client():

    aws_profile = os.getenv(
        "AWS_PROFILE",
        "auto-marketplace"
    )

    session = boto3.Session(
        profile_name=aws_profile,
        region_name=AWS_REGION,
    )

    return session.client(
        "s3"
    )


# ============================================================
# BUILD S3 PREFIX
# ============================================================

def build_raw_prefix(
    endpoint,
    extract_date,
    safe_run_id,
):

    return (
        f"raw/commercial/{endpoint}/"
        f"extract_date={extract_date}/"
        f"run_id={safe_run_id}/"
    )


# ============================================================
# REGISTER GLUE PARTITION
# ============================================================

def register_commercial_partition(
    endpoint,
    extract_date,
    safe_run_id,
):

    table_name = (
        RAW_TABLES[endpoint]
    )

    raw_prefix = build_raw_prefix(
        endpoint=endpoint,
        extract_date=extract_date,
        safe_run_id=safe_run_id,
    )

    s3_location = (
        f"s3://{BUCKET_NAME}/"
        f"{raw_prefix}"
    )

    print(
        "Registering Commercial RAW partition..."
    )

    print(
        f"Endpoint: {endpoint}"
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
        f"{safe_run_id}"
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
            "run_id": safe_run_id,
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
# INGEST ONE API ENDPOINT
# ============================================================

def ingest_endpoint(
    endpoint: str
):

    if endpoint not in RAW_TABLES:

        raise ValueError(
            f"Unsupported endpoint: "
            f"{endpoint}"
        )


    # --------------------------------------------------------
    # Airflow execution context
    # --------------------------------------------------------

    context = (
        get_current_context()
    )

    logical_date = (
        context["logical_date"]
    )

    run_id = (
        context["run_id"]
    )

    extract_date = (
        logical_date.strftime(
            "%Y-%m-%d"
        )
    )

    safe_run_id = (
        run_id
        .replace(":", "-")
        .replace("+", "_")
    )


    # --------------------------------------------------------
    # High watermark for this extraction
    #
    # Captured BEFORE extraction.
    #
    # We only persist it after:
    #
    # API extraction succeeds
    # S3 writes succeed
    # manifest succeeds
    # Glue partition registration succeeds
    # --------------------------------------------------------

    extraction_watermark = (
        pendulum.now("UTC")
    )


    # --------------------------------------------------------
    # Previous endpoint watermark
    # --------------------------------------------------------

    watermark_variable = (
        f"commercial_"
        f"{endpoint}_watermark"
    )

    previous_watermark = (
        Variable.get(
            watermark_variable,
            default_var=None,
        )
    )

    if previous_watermark:

        extraction_mode = (
            "incremental"
        )

        print(
            f"Previous watermark for "
            f"{endpoint}: "
            f"{previous_watermark}"
        )

    else:

        extraction_mode = (
            "full"
        )

        print(
            f"No watermark found for "
            f"{endpoint}. "
            "Running initial full extraction."
        )


    # --------------------------------------------------------
    # AWS S3 connection
    # --------------------------------------------------------

    s3 = (
        get_s3_client()
    )


    # --------------------------------------------------------
    # Build S3 partition prefix
    # --------------------------------------------------------

    raw_prefix = build_raw_prefix(
        endpoint=endpoint,
        extract_date=extract_date,
        safe_run_id=safe_run_id,
    )


    # --------------------------------------------------------
    # Pagination
    # --------------------------------------------------------

    page = 1

    total_records_written = 0

    total_pages_written = 0


    while True:

        params = {
            "page": page,
            "page_size": PAGE_SIZE,
        }


        # ----------------------------------------------------
        # Incremental API extraction
        # ----------------------------------------------------

        if previous_watermark:

            params[
                "updated_since"
            ] = previous_watermark


        url = (
            f"{API_BASE_URL}/"
            f"{endpoint}?"
            f"{urlencode(params)}"
        )

        print(
            f"Requesting: {url}"
        )


        # ----------------------------------------------------
        # Call Commercial API
        # ----------------------------------------------------

        with urlopen(
            url,
            timeout=30
        ) as response:

            payload = json.loads(
                response
                .read()
                .decode("utf-8")
            )


        records = (
            payload["data"]
        )

        total_pages = (
            payload["total_pages"]
        )


        # ----------------------------------------------------
        # No more data
        # ----------------------------------------------------

        if not records:

            print(
                f"No records returned "
                f"for {endpoint} "
                f"page {page}"
            )

            break


        # ----------------------------------------------------
        # Write complete API page to S3
        #
        # We intentionally preserve the RAW API envelope:
        #
        # {
        #   page,
        #   page_size,
        #   total_records,
        #   total_pages,
        #   data: [...]
        # }
        # ----------------------------------------------------

        s3_key = (
            f"{raw_prefix}"
            f"page={page:04d}.json"
        )


        print(
            f"Writing "
            f"{len(records)} records "
            f"to "
            f"s3://{BUCKET_NAME}/"
            f"{s3_key}"
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


        total_records_written += (
            len(records)
        )

        total_pages_written += 1


        # ----------------------------------------------------
        # Last API page reached
        # ----------------------------------------------------

        if page >= total_pages:

            break


        page += 1


    # ========================================================
    # MANIFEST
    # ========================================================

    manifest = {

        "endpoint": (
            endpoint
        ),

        "mode": (
            extraction_mode
        ),

        "extract_date": (
            extract_date
        ),

        "run_id": (
            safe_run_id
        ),

        "previous_watermark": (
            previous_watermark
        ),

        "new_watermark": (
            extraction_watermark
            .to_iso8601_string()
        ),

        "records": (
            total_records_written
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


    print(
        "Manifest written: "
        f"s3://{BUCKET_NAME}/"
        f"{manifest_key}"
    )


    # ========================================================
    # GLUE PARTITION REGISTRATION
    # ========================================================
    #
    # Only register a partition if this run wrote actual
    # business-data page files.
    #
    # A zero-record incremental run only contains a manifest,
    # so there is no RAW business partition to query.
    # ========================================================

    if total_records_written > 0:

        register_commercial_partition(
            endpoint=endpoint,
            extract_date=extract_date,
            safe_run_id=safe_run_id,
        )

    else:

        print(
            f"No new {endpoint} records. "
            "Skipping Glue partition "
            "registration."
        )


    # ========================================================
    # UPDATE WATERMARK
    # ========================================================
    #
    # IMPORTANT:
    #
    # This is after partition registration.
    #
    # If Glue fails:
    #
    # task fails
    # watermark does not move
    #
    # so the next retry can safely extract the data again.
    # ========================================================

    Variable.set(
        watermark_variable,
        extraction_watermark
        .to_iso8601_string()
    )


    print(
        f"Updated watermark "
        f"{watermark_variable} -> "
        f"{extraction_watermark.to_iso8601_string()}"
    )


    print(
        f"Extraction completed: "
        f"{manifest}"
    )


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id=(
        "commercial_raw_ingestion"
    ),

    description=(
        "Incrementally extract Commercial "
        "CRM/Billing API data into AWS S3 RAW "
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
        "commercial",
        "api",
        "raw",
        "s3",
        "glue",
        "incremental",
    ],

) as dag:


    # --------------------------------------------------------
    # Accounts
    # --------------------------------------------------------

    extract_accounts = (
        PythonOperator(

            task_id=(
                "extract_accounts"
            ),

            python_callable=(
                ingest_endpoint
            ),

            op_kwargs={
                "endpoint": (
                    "accounts"
                )
            },
        )
    )


    # --------------------------------------------------------
    # Contracts
    # --------------------------------------------------------

    extract_contracts = (
        PythonOperator(

            task_id=(
                "extract_contracts"
            ),

            python_callable=(
                ingest_endpoint
            ),

            op_kwargs={
                "endpoint": (
                    "contracts"
                )
            },
        )
    )


    # --------------------------------------------------------
    # Subscriptions
    # --------------------------------------------------------

    extract_subscriptions = (
        PythonOperator(

            task_id=(
                "extract_subscriptions"
            ),

            python_callable=(
                ingest_endpoint
            ),

            op_kwargs={
                "endpoint": (
                    "subscriptions"
                )
            },
        )
    )


    # --------------------------------------------------------
    # Invoices
    # --------------------------------------------------------

    extract_invoices = (
        PythonOperator(

            task_id=(
                "extract_invoices"
            ),

            python_callable=(
                ingest_endpoint
            ),

            op_kwargs={
                "endpoint": (
                    "invoices"
                )
            },
        )
    )