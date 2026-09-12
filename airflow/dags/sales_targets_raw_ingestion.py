import csv
import hashlib
import json
import os
from datetime import timedelta
from pathlib import Path

import boto3
import pendulum
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context


BUCKET_NAME = "european-auto-marketplace-data-yash-2026-01"
AWS_REGION = "us-east-1"

SOURCE_FILE = Path(
    "/opt/airflow/sample_data/"
    "sales_targets/sales_targets.csv"
)

EXPECTED_COLUMNS = {
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
}


def get_s3_client():

    profile = os.getenv(
        "AWS_PROFILE",
        "auto-marketplace"
    )

    session = boto3.Session(
        profile_name=profile
    )

    return session.client(
        "s3",
        region_name=AWS_REGION
    )


def calculate_sha256(file_path):

    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:

        while True:

            chunk = file.read(
                1024 * 1024
            )

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def validate_csv(file_path):

    with open(
        file_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        columns = set(
            reader.fieldnames or []
        )

        missing_columns = (
            EXPECTED_COLUMNS - columns
        )

        if missing_columns:

            raise ValueError(
                "Sales target CSV is missing "
                f"columns: {missing_columns}"
            )

        row_count = sum(
            1 for _ in reader
        )

    if row_count == 0:

        raise ValueError(
            "Sales target CSV contains "
            "no data rows."
        )

    return row_count


def object_exists(
    s3,
    bucket,
    key,
):

    try:

        s3.head_object(
            Bucket=bucket,
            Key=key
        )

        return True

    except ClientError as exc:

        code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

        if code in (
            "404",
            "NoSuchKey",
            "NotFound",
        ):
            return False

        raise


def ingest_sales_targets():

    context = get_current_context()

    run_id = (
        context["run_id"]
        .replace(":", "-")
        .replace("+", "_")
    )

    extract_date = (
        context["logical_date"]
        .strftime("%Y-%m-%d")
    )

    if not SOURCE_FILE.exists():

        raise FileNotFoundError(
            f"Sales target file not found: "
            f"{SOURCE_FILE}"
        )

    # -----------------------------------------
    # Validate source file
    # -----------------------------------------

    row_count = validate_csv(
        SOURCE_FILE
    )

    file_size = (
        SOURCE_FILE.stat().st_size
    )

    checksum = calculate_sha256(
        SOURCE_FILE
    )

    print(
        f"Validated sales targets: "
        f"{row_count} rows"
    )

    print(
        f"SHA256: {checksum}"
    )

    # -----------------------------------------
    # Connect to S3
    # -----------------------------------------

    s3 = get_s3_client()

    # Content-addressed raw storage.
    # Same file = same checksum = same S3 key.
    data_key = (
        "raw/sales_targets/"
        "files/"
        f"sha256={checksum}/"
        "sales_targets.csv"
    )

    already_exists = object_exists(
        s3,
        BUCKET_NAME,
        data_key,
    )

    if already_exists:

        uploaded = False

        print(
            "Identical sales target file "
            "already exists in S3. "
            "Skipping upload."
        )

    else:

        uploaded = True

        print(
            f"Uploading to "
            f"s3://{BUCKET_NAME}/{data_key}"
        )

        s3.upload_file(
            str(SOURCE_FILE),
            BUCKET_NAME,
            data_key,
            ExtraArgs={
                "ContentType": "text/csv"
            },
        )

    # -----------------------------------------
    # Manifest for this Airflow run
    # -----------------------------------------

    manifest = {
        "dataset": "sales_targets",
        "source_file": (
            SOURCE_FILE.name
        ),
        "records": row_count,
        "file_size_bytes": file_size,
        "sha256": checksum,
        "uploaded": uploaded,
        "s3_data_key": data_key,
    }

    manifest_key = (
        "raw/sales_targets/"
        "_manifests/"
        f"extract_date={extract_date}/"
        f"run_id={run_id}.json"
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

    print(
        f"Sales targets ingestion complete: "
        f"{manifest}"
    )


with DAG(
    dag_id="sales_targets_raw_ingestion",
    description=(
        "Validate and ingest sales target "
        "CSV into AWS S3 RAW"
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
        "sales",
        "targets",
        "csv",
        "raw",
        "s3",
    ],
) as dag:

    ingest_sales_targets_task = (
        PythonOperator(
            task_id="ingest_sales_targets",
            python_callable=ingest_sales_targets,
        )
    )