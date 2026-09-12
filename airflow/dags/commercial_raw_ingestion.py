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


BUCKET_NAME = "european-auto-marketplace-data-yash-2026-01"
API_BASE_URL = "http://commercial-api:8000"
AWS_REGION = "us-east-1"
PAGE_SIZE = 1000


def ingest_endpoint(endpoint: str):

    context = get_current_context()

    logical_date = context["logical_date"]
    run_id = context["run_id"]

    # Capture the beginning of this extraction.
    # We only save it as the next watermark after
    # the entire extraction succeeds.
    extraction_watermark = pendulum.now("UTC")

    extract_date = logical_date.strftime("%Y-%m-%d")

    safe_run_id = (
        run_id
        .replace(":", "-")
        .replace("+", "_")
    )

    watermark_variable = (
        f"commercial_{endpoint}_watermark"
    )

    previous_watermark = Variable.get(
        watermark_variable,
        default_var=None
    )

    if previous_watermark:
        extraction_mode = "incremental"

        print(
            f"Previous watermark for {endpoint}: "
            f"{previous_watermark}"
        )
    else:
        extraction_mode = "full"

        print(
            f"No watermark found for {endpoint}. "
            "Running initial full extraction."
        )

    aws_profile = os.getenv(
        "AWS_PROFILE",
        "auto-marketplace"
    )

    session = boto3.Session(
        profile_name=aws_profile
    )

    s3 = session.client(
        "s3",
        region_name=AWS_REGION
    )

    page = 1
    total_records_written = 0
    total_pages_written = 0

    while True:

        params = {
            "page": page,
            "page_size": PAGE_SIZE,
        }

        if previous_watermark:
            params["updated_since"] = previous_watermark

        url = (
            f"{API_BASE_URL}/{endpoint}?"
            f"{urlencode(params)}"
        )

        print(f"Requesting: {url}")

        with urlopen(
            url,
            timeout=30
        ) as response:

            payload = json.loads(
                response.read().decode("utf-8")
            )

        records = payload["data"]
        total_pages = payload["total_pages"]

        if not records:
            print(
                f"No records returned for "
                f"{endpoint} page {page}"
            )
            break

        s3_key = (
            f"raw/commercial/{endpoint}/"
            f"extract_date={extract_date}/"
            f"run_id={safe_run_id}/"
            f"page={page:04d}.json"
        )

        print(
            f"Writing {len(records)} records to "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(
                payload,
                ensure_ascii=False
            ).encode("utf-8"),
            ContentType="application/json",
        )

        total_records_written += len(records)
        total_pages_written += 1

        if page >= total_pages:
            break

        page += 1

    manifest = {
        "endpoint": endpoint,
        "mode": extraction_mode,
        "previous_watermark": previous_watermark,
        "new_watermark": (
            extraction_watermark.to_iso8601_string()
        ),
        "records": total_records_written,
        "pages": total_pages_written,
    }

    manifest_key = (
        f"raw/commercial/{endpoint}/"
        f"extract_date={extract_date}/"
        f"run_id={safe_run_id}/"
        "_manifest.json"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json",
    )

    # Only advance the watermark after
    # extraction + S3 writes succeeded.
    Variable.set(
        watermark_variable,
        extraction_watermark.to_iso8601_string()
    )

    print(
        f"Extraction completed: {manifest}"
    )


with DAG(
    dag_id="commercial_raw_ingestion",
    description=(
        "Incrementally extract commercial "
        "CRM/Billing API data to S3 RAW"
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
        "retry_delay": timedelta(seconds=30),
    },
    tags=[
        "commercial",
        "raw",
        "s3",
        "incremental"
    ],
) as dag:

    extract_accounts = PythonOperator(
        task_id="extract_accounts",
        python_callable=ingest_endpoint,
        op_kwargs={"endpoint": "accounts"},
    )

    extract_contracts = PythonOperator(
        task_id="extract_contracts",
        python_callable=ingest_endpoint,
        op_kwargs={"endpoint": "contracts"},
    )

    extract_subscriptions = PythonOperator(
        task_id="extract_subscriptions",
        python_callable=ingest_endpoint,
        op_kwargs={"endpoint": "subscriptions"},
    )

    extract_invoices = PythonOperator(
        task_id="extract_invoices",
        python_callable=ingest_endpoint,
        op_kwargs={"endpoint": "invoices"},
    )