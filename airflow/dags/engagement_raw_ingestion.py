import json
import os
import re
from datetime import timedelta
from pathlib import Path

import boto3
import pendulum
from botocore.exceptions import ClientError

from airflow import DAG
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

RAW_DATABASE = "auto_marketplace_raw"

RAW_TABLE = "raw_engagement_events"

SOURCE_ROOT = Path(
    "/opt/airflow/sample_data/"
    "engagement_events"
)


# ============================================================
# AWS S3 CLIENT
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
# CHECK S3 OBJECT
# ============================================================

def s3_object_exists(
    s3,
    bucket,
    key,
):

    try:

        s3.head_object(
            Bucket=bucket,
            Key=key,
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


# ============================================================
# BUILD ENGAGEMENT S3 LOCATION
# ============================================================

def build_partition_location(
    event_date,
):

    return (
        f"s3://{BUCKET_NAME}/"
        f"raw/engagement/"
        f"event_date={event_date}/"
    )


# ============================================================
# REGISTER ENGAGEMENT PARTITION
# ============================================================

def register_engagement_partition(
    event_date,
):

    s3_location = (
        build_partition_location(
            event_date
        )
    )

    print(
        "Ensuring Engagement Glue partition..."
    )

    print(
        f"Table: "
        f"{RAW_DATABASE}.{RAW_TABLE}"
    )

    print(
        f"event_date: {event_date}"
    )

    print(
        f"S3 location: {s3_location}"
    )

    created = register_partition(
        database_name=RAW_DATABASE,
        table_name=RAW_TABLE,
        partition_values={
            "event_date": event_date,
        },
        s3_location=s3_location,
        region_name=AWS_REGION,
    )

    if created:

        print(
            "Glue partition registered."
        )

        return "created"

    print(
        "Glue partition already exists."
    )

    return "existing"


# ============================================================
# ENGAGEMENT INGESTION
# ============================================================

def ingest_engagement_events():

    context = get_current_context()


    # --------------------------------------------------------
    # Airflow run information
    # --------------------------------------------------------

    run_id = (
        context["run_id"]
        .replace(":", "-")
        .replace("+", "_")
    )

    extract_date = (
        context["logical_date"]
        .strftime("%Y-%m-%d")
    )


    # --------------------------------------------------------
    # S3
    # --------------------------------------------------------

    s3 = get_s3_client()


    # --------------------------------------------------------
    # Validate local source directory
    # --------------------------------------------------------

    if not SOURCE_ROOT.exists():

        raise FileNotFoundError(
            "Source directory not found: "
            f"{SOURCE_ROOT}"
        )


    # --------------------------------------------------------
    # Discover local event-date partitions
    #
    # Example:
    #
    # event_date=2026-09-13/
    #     events.jsonl.gz
    # --------------------------------------------------------

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
        "engagement source partitions."
    )


    # ========================================================
    # METRICS
    # ========================================================

    uploaded_partitions = []

    skipped_s3_partitions = []

    new_glue_partitions = []

    existing_glue_partitions = []

    invalid_partitions = []

    total_uploaded_bytes = 0


    # ========================================================
    # PROCESS EACH EVENT DATE
    # ========================================================

    for source_file in source_files:

        partition_name = (
            source_file.parent.name
        )


        # ----------------------------------------------------
        # Validate Hive-style partition directory
        # ----------------------------------------------------

        match = re.fullmatch(
            r"event_date="
            r"(\d{4}-\d{2}-\d{2})",
            partition_name
        )


        if not match:

            print(
                "Skipping invalid partition: "
                f"{partition_name}"
            )

            invalid_partitions.append(
                partition_name
            )

            continue


        event_date = (
            match.group(1)
        )


        # ----------------------------------------------------
        # S3 RAW object
        # ----------------------------------------------------

        s3_key = (
            "raw/engagement/"
            f"event_date={event_date}/"
            "events.jsonl.gz"
        )


        # ====================================================
        # STEP 1:
        # ENSURE RAW FILE EXISTS IN S3
        # ====================================================

        if s3_object_exists(
            s3,
            BUCKET_NAME,
            s3_key,
        ):

            print(
                "S3 object already exists, "
                "skipping upload: "
                f"{s3_key}"
            )

            skipped_s3_partitions.append(
                event_date
            )


        else:

            file_size = (
                source_file.stat().st_size
            )


            print(
                f"Uploading "
                f"{source_file} "
                f"-> "
                f"s3://{BUCKET_NAME}/"
                f"{s3_key}"
            )


            s3.upload_file(
                str(source_file),
                BUCKET_NAME,
                s3_key,
                ExtraArgs={
                    "ContentType": (
                        "application/"
                        "x-ndjson"
                    ),
                    "ContentEncoding": (
                        "gzip"
                    ),
                },
            )


            uploaded_partitions.append(
                event_date
            )

            total_uploaded_bytes += (
                file_size
            )


            print(
                "S3 upload complete: "
                f"{event_date}"
            )


        # ====================================================
        # STEP 2:
        # ENSURE GLUE KNOWS ABOUT THE PARTITION
        #
        # IMPORTANT:
        #
        # We do this even when the S3 file already existed.
        #
        # This makes retries safe when:
        #
        # S3 succeeded
        # Glue failed
        #
        # on the previous attempt.
        # ====================================================

        registration_result = (
            register_engagement_partition(
                event_date
            )
        )


        if (
            registration_result
            == "created"
        ):

            new_glue_partitions.append(
                event_date
            )

        else:

            existing_glue_partitions.append(
                event_date
            )


    # ========================================================
    # MANIFEST
    # ========================================================

    manifest = {

        "dataset": (
            "engagement_events"
        ),

        "extract_date": (
            extract_date
        ),

        "run_id": (
            run_id
        ),

        "source_partitions_found": (
            len(source_files)
        ),

        "uploaded_s3_partitions": (
            len(uploaded_partitions)
        ),

        "skipped_existing_s3_partitions": (
            len(skipped_s3_partitions)
        ),

        "new_glue_partitions": (
            len(new_glue_partitions)
        ),

        "existing_glue_partitions": (
            len(existing_glue_partitions)
        ),

        "invalid_partitions": (
            len(invalid_partitions)
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

        "first_new_glue_partition": (
            new_glue_partitions[0]
            if new_glue_partitions
            else None
        ),

        "last_new_glue_partition": (
            new_glue_partitions[-1]
            if new_glue_partitions
            else None
        ),
    }


    # --------------------------------------------------------
    # Manifest is not part of the Athena business table.
    # --------------------------------------------------------

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
        ).encode(
            "utf-8"
        ),
        ContentType=(
            "application/json"
        ),
    )


    print(
        "Engagement ingestion complete:"
    )

    print(
        json.dumps(
            manifest,
            indent=2
        )
    )


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id=(
        "engagement_raw_ingestion"
    ),

    description=(
        "Upload engagement event-date "
        "partitions into AWS S3 RAW and "
        "register exact Glue partitions"
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
        "glue",
        "partitioned",
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