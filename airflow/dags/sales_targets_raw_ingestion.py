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

from platform_utils.glue_partitions import register_partition


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET_NAME = (
    "european-auto-marketplace-data-yash-2026-01"
)

AWS_REGION = "us-east-1"

RAW_DATABASE = "auto_marketplace_raw"

RAW_TABLE = "raw_sales_targets"

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
# SHA256
# ============================================================

def calculate_sha256(
    file_path
):

    sha256 = hashlib.sha256()

    with open(
        file_path,
        "rb"
    ) as file:

        while True:

            chunk = file.read(
                1024 * 1024
            )

            if not chunk:
                break

            sha256.update(
                chunk
            )

    return sha256.hexdigest()


# ============================================================
# CSV VALIDATION
# ============================================================

def validate_csv(
    file_path
):

    with open(
        file_path,
        "r",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.DictReader(
            file
        )

        columns = set(
            reader.fieldnames
            or []
        )


        # ----------------------------------------------------
        # Required columns
        # ----------------------------------------------------

        missing_columns = (
            EXPECTED_COLUMNS
            - columns
        )

        if missing_columns:

            raise ValueError(
                "Sales target CSV is missing "
                "required columns: "
                f"{sorted(missing_columns)}"
            )


        # ----------------------------------------------------
        # Count business rows
        # ----------------------------------------------------

        row_count = sum(
            1
            for _ in reader
        )


    if row_count == 0:

        raise ValueError(
            "Sales target CSV contains "
            "no data rows."
        )


    return row_count


# ============================================================
# CHECK WHETHER S3 OBJECT EXISTS
# ============================================================

def object_exists(
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


# ============================================================
# BUILD S3 DATA KEY
# ============================================================

def build_data_key(
    checksum
):

    return (
        "raw/sales_targets/"
        "files/"
        f"sha256={checksum}/"
        "sales_targets.csv"
    )


# ============================================================
# BUILD GLUE PARTITION LOCATION
# ============================================================

def build_partition_location(
    checksum
):

    return (
        f"s3://{BUCKET_NAME}/"
        "raw/sales_targets/"
        "files/"
        f"sha256={checksum}/"
    )


# ============================================================
# REGISTER SALES TARGET PARTITION
# ============================================================

def register_sales_target_partition(
    checksum
):

    s3_location = (
        build_partition_location(
            checksum
        )
    )

    print(
        "Ensuring Sales Targets "
        "Glue partition..."
    )

    print(
        f"Table: "
        f"{RAW_DATABASE}.{RAW_TABLE}"
    )

    print(
        f"sha256: {checksum}"
    )

    print(
        f"S3 location: "
        f"{s3_location}"
    )


    created = register_partition(
        database_name=RAW_DATABASE,
        table_name=RAW_TABLE,
        partition_values={
            "sha256": checksum,
        },
        s3_location=s3_location,
        region_name=AWS_REGION,
    )


    if created:

        print(
            "Glue partition successfully "
            "registered."
        )

        return "created"


    print(
        "Glue partition already exists."
    )

    return "existing"


# ============================================================
# SALES TARGET INGESTION
# ============================================================

def ingest_sales_targets():

    context = (
        get_current_context()
    )


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


    # ========================================================
    # 1. VALIDATE SOURCE FILE EXISTS
    # ========================================================

    if not SOURCE_FILE.exists():

        raise FileNotFoundError(
            "Sales target file not found: "
            f"{SOURCE_FILE}"
        )


    # ========================================================
    # 2. VALIDATE CSV SCHEMA + DATA
    # ========================================================

    row_count = (
        validate_csv(
            SOURCE_FILE
        )
    )

    file_size = (
        SOURCE_FILE
        .stat()
        .st_size
    )


    # ========================================================
    # 3. CALCULATE CONTENT HASH
    #
    # Same content
    #     ↓
    # same SHA256
    #     ↓
    # same S3 partition
    #
    # New content
    #     ↓
    # new SHA256
    #     ↓
    # new immutable RAW partition
    # ========================================================

    checksum = (
        calculate_sha256(
            SOURCE_FILE
        )
    )


    print(
        "Validated sales targets:"
    )

    print(
        f"Rows: {row_count}"
    )

    print(
        f"File size: "
        f"{file_size} bytes"
    )

    print(
        f"SHA256: {checksum}"
    )


    # ========================================================
    # 4. CONNECT TO S3
    # ========================================================

    s3 = (
        get_s3_client()
    )


    # ========================================================
    # 5. CONTENT-ADDRESSED S3 LOCATION
    # ========================================================

    data_key = (
        build_data_key(
            checksum
        )
    )


    print(
        "RAW data location:"
    )

    print(
        f"s3://{BUCKET_NAME}/"
        f"{data_key}"
    )


    # ========================================================
    # 6. ENSURE FILE EXISTS IN S3
    # ========================================================

    already_exists = (
        object_exists(
            s3,
            BUCKET_NAME,
            data_key,
        )
    )


    if already_exists:

        uploaded = False

        print(
            "Identical Sales Targets file "
            "already exists in S3."
        )

        print(
            "Skipping duplicate upload."
        )


    else:

        uploaded = True

        print(
            "Uploading Sales Targets file..."
        )

        s3.upload_file(
            str(SOURCE_FILE),
            BUCKET_NAME,
            data_key,
            ExtraArgs={
                "ContentType": (
                    "text/csv"
                )
            },
        )

        print(
            "S3 upload complete."
        )


    # ========================================================
    # 7. ENSURE GLUE PARTITION EXISTS
    #
    # IMPORTANT:
    #
    # We ALWAYS run this step.
    #
    # Even if the S3 file already exists, Glue may not know
    # about it because a previous task attempt could have:
    #
    # uploaded S3 successfully
    #         ↓
    # failed before Glue registration
    #
    # On retry:
    #
    # skip duplicate S3 upload
    #         ↓
    # still create missing Glue partition
    # ========================================================

    partition_status = (
        register_sales_target_partition(
            checksum
        )
    )


    # ========================================================
    # 8. WRITE RUN MANIFEST
    # ========================================================

    manifest = {

        "dataset": (
            "sales_targets"
        ),

        "extract_date": (
            extract_date
        ),

        "run_id": (
            run_id
        ),

        "source_file": (
            SOURCE_FILE.name
        ),

        "records": (
            row_count
        ),

        "file_size_bytes": (
            file_size
        ),

        "sha256": (
            checksum
        ),

        "uploaded": (
            uploaded
        ),

        "glue_partition_status": (
            partition_status
        ),

        "s3_data_key": (
            data_key
        ),
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
        ).encode(
            "utf-8"
        ),
        ContentType=(
            "application/json"
        ),
    )


    print(
        "Sales Targets ingestion complete:"
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
        "sales_targets_raw_ingestion"
    ),

    description=(
        "Validate and ingest Sales Targets "
        "CSV into content-addressed AWS S3 RAW "
        "and register the exact Glue partition"
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
        "glue",
        "content-addressed",
    ],

) as dag:


    ingest_sales_targets_task = (
        PythonOperator(

            task_id=(
                "ingest_sales_targets"
            ),

            python_callable=(
                ingest_sales_targets
            ),
        )
    )