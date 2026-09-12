import json
import os
import re
from pathlib import Path
from datetime import timedelta

import boto3
import pendulum
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context


BUCKET_NAME = "european-auto-marketplace-data-yash-2026-01"
AWS_REGION = "us-east-1"

SOURCE_ROOT = Path(
    "/opt/airflow/sample_data/engagement_events"
)


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


def s3_object_exists(
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

        error_code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound",
        ):
            return False

        raise


def ingest_engagement_events():

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

    s3 = get_s3_client()

    if not SOURCE_ROOT.exists():

        raise FileNotFoundError(
            f"Source directory not found: "
            f"{SOURCE_ROOT}"
        )

    source_files = sorted(
        SOURCE_ROOT.glob(
            "event_date=*/events.jsonl.gz"
        )
    )

    if not source_files:

        raise RuntimeError(
            "No engagement event files found."
        )

    print(
        f"Found {len(source_files)} "
        "engagement partitions."
    )

    uploaded_partitions = []
    skipped_partitions = []

    total_uploaded_bytes = 0

    for source_file in source_files:

        partition_name = (
            source_file.parent.name
        )

        match = re.fullmatch(
            r"event_date=(\d{4}-\d{2}-\d{2})",
            partition_name
        )

        if not match:

            print(
                f"Skipping invalid partition: "
                f"{partition_name}"
            )

            continue

        event_date = match.group(1)

        s3_key = (
            "raw/engagement/"
            f"event_date={event_date}/"
            "events.jsonl.gz"
        )

        if s3_object_exists(
            s3,
            BUCKET_NAME,
            s3_key,
        ):

            print(
                f"Already exists, skipping: "
                f"{s3_key}"
            )

            skipped_partitions.append(
                event_date
            )

            continue

        file_size = (
            source_file.stat().st_size
        )

        print(
            f"Uploading "
            f"{source_file} "
            f"→ s3://{BUCKET_NAME}/{s3_key}"
        )

        s3.upload_file(
            str(source_file),
            BUCKET_NAME,
            s3_key,
            ExtraArgs={
                "ContentType": (
                    "application/x-ndjson"
                ),
                "ContentEncoding": "gzip",
            },
        )

        uploaded_partitions.append(
            event_date
        )

        total_uploaded_bytes += (
            file_size
        )

    manifest = {
        "dataset": "engagement_events",
        "source_partitions_found": (
            len(source_files)
        ),
        "uploaded_partitions": (
            len(uploaded_partitions)
        ),
        "skipped_existing_partitions": (
            len(skipped_partitions)
        ),
        "uploaded_bytes": (
            total_uploaded_bytes
        ),
        "first_uploaded_partition": (
            uploaded_partitions[0]
            if uploaded_partitions
            else None
        ),
        "last_uploaded_partition": (
            uploaded_partitions[-1]
            if uploaded_partitions
            else None
        ),
    }

    manifest_key = (
        "raw/engagement/_manifests/"
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
        f"Engagement ingestion complete: "
        f"{manifest}"
    )


with DAG(
    dag_id="engagement_raw_ingestion",
    description=(
        "Upload daily engagement event "
        "partitions into AWS S3 RAW"
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
        "engagement",
        "events",
        "raw",
        "s3",
    ],
) as dag:

    ingest_engagement_events_task = (
        PythonOperator(
            task_id=(
                "ingest_engagement_events"
            ),
            python_callable=(
                ingest_engagement_events
            ),
        )
    )