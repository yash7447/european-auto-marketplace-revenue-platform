import gzip
import io
import json
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

import boto3
import pendulum
import psycopg2

from airflow import DAG
from airflow.hooks.base import BaseHook
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

RAW_DATABASE = "auto_marketplace_raw"

BATCH_SIZE = 10000


# ============================================================
# JSON SERIALIZATION
# ============================================================

def json_default(value):
    """
    Convert PostgreSQL/Python data types into
    JSON-compatible values.
    """

    if isinstance(
        value,
        (datetime, date)
    ):
        return value.isoformat()

    if isinstance(
        value,
        Decimal
    ):
        return float(value)

    return str(value)


# ============================================================
# POSTGRESQL CONNECTION
# ============================================================

def get_database_connection():
    """
    Read PostgreSQL credentials from the
    Airflow connection marketplace_postgres.
    """

    airflow_conn = (
        BaseHook.get_connection(
            "marketplace_postgres"
        )
    )

    return psycopg2.connect(
        host=airflow_conn.host,
        port=airflow_conn.port,
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
    )


# ============================================================
# TABLE METADATA
# ============================================================

def get_table_columns(
    db,
    table_name
):
    """
    Retrieve PostgreSQL column names.

    We use a normal cursor because the actual extraction
    uses a server-side cursor for streaming large datasets.
    """

    metadata_cursor = db.cursor()

    metadata_cursor.execute(
        f"""
        SELECT *
        FROM {table_name}
        LIMIT 0
        """
    )

    columns = [
        description[0]
        for description
        in metadata_cursor.description
    ]

    metadata_cursor.close()

    return columns


# ============================================================
# S3 CONNECTION
# ============================================================

def get_s3_client():
    """
    Use the AWS profile mounted into Airflow.
    """

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
# AIRFLOW RUN INFORMATION
# ============================================================

def get_run_information():
    """
    Return the normalized Airflow run ID and extraction date.

    Example:

    extract_date:
        2026-09-13

    run_id:
        scheduled__2026-09-13T02-00-00_00-00
    """

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

    return (
        extract_date,
        run_id,
    )


# ============================================================
# BUILD MARKETPLACE RAW S3 PREFIX
# ============================================================

def build_raw_prefix(
    dataset,
    extract_date,
    run_id,
):
    """
    Return the S3 key prefix for one extraction run.
    """

    return (
        f"raw/marketplace/{dataset}/"
        f"extract_date={extract_date}/"
        f"run_id={run_id}/"
    )


# ============================================================
# REGISTER GLUE PARTITION
# ============================================================

def register_marketplace_partition(
    table_name,
    dataset,
    extract_date,
    run_id,
):
    """
    Register one exact Marketplace RAW partition
    in the AWS Glue Data Catalog.
    """

    s3_prefix = build_raw_prefix(
        dataset=dataset,
        extract_date=extract_date,
        run_id=run_id,
    )

    s3_location = (
        f"s3://{BUCKET_NAME}/"
        f"{s3_prefix}"
    )

    print(
        "Registering RAW partition..."
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
            "Partition successfully registered."
        )

    else:

        print(
            "Partition already existed. "
            "Nothing to change."
        )


# ============================================================
# COMPRESS + UPLOAD ONE BATCH
# ============================================================

def upload_batch(
    s3,
    rows,
    columns,
    s3_key,
):
    """
    Convert PostgreSQL rows into JSON Lines,
    gzip them in memory, and upload to S3.
    """

    buffer = io.BytesIO()

    with gzip.GzipFile(
        fileobj=buffer,
        mode="wb"
    ) as gz:

        for row in rows:

            record = dict(
                zip(
                    columns,
                    row
                )
            )

            line = (
                json.dumps(
                    record,
                    default=json_default,
                    ensure_ascii=False,
                )
                + "\n"
            )

            gz.write(
                line.encode(
                    "utf-8"
                )
            )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/json",
        ContentEncoding="gzip",
    )


# ============================================================
# VEHICLES EXTRACTION
# ============================================================

def extract_vehicles():

    (
        extract_date,
        run_id,
    ) = get_run_information()

    db = None
    cursor = None

    try:

        # ----------------------------------------------------
        # Connections
        # ----------------------------------------------------

        db = (
            get_database_connection()
        )

        s3 = (
            get_s3_client()
        )

        print(
            "Connected to marketplace PostgreSQL "
            "for vehicles extraction."
        )


        # ----------------------------------------------------
        # Retrieve source columns
        # ----------------------------------------------------

        columns = get_table_columns(
            db,
            "vehicles"
        )

        print(
            f"Vehicles columns: "
            f"{columns}"
        )


        # ----------------------------------------------------
        # Server-side cursor
        # ----------------------------------------------------

        cursor = db.cursor(
            name="vehicles_cursor"
        )

        cursor.itersize = (
            BATCH_SIZE
        )

        cursor.execute(
            """
            SELECT *
            FROM vehicles
            """
        )


        # ----------------------------------------------------
        # Stream PostgreSQL -> S3
        # ----------------------------------------------------

        total_records = 0
        part_number = 1

        raw_prefix = build_raw_prefix(
            dataset="vehicles",
            extract_date=extract_date,
            run_id=run_id,
        )

        while True:

            rows = cursor.fetchmany(
                BATCH_SIZE
            )

            if not rows:
                break

            s3_key = (
                f"{raw_prefix}"
                f"part-{part_number:04d}"
                ".jsonl.gz"
            )

            print(
                f"Uploading "
                f"{len(rows)} vehicles "
                f"to {s3_key}"
            )

            upload_batch(
                s3=s3,
                rows=rows,
                columns=columns,
                s3_key=s3_key,
            )

            total_records += (
                len(rows)
            )

            print(
                f"Uploaded vehicles part "
                f"{part_number}: "
                f"{len(rows)} rows"
            )

            part_number += 1


        # ----------------------------------------------------
        # Manifest
        # ----------------------------------------------------

        manifest = {
            "table": "vehicles",
            "mode": "full_snapshot",
            "extract_date": (
                extract_date
            ),
            "run_id": run_id,
            "records": (
                total_records
            ),
            "batch_size": (
                BATCH_SIZE
            ),
            "parts": (
                part_number - 1
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


        # ----------------------------------------------------
        # Glue partition registration
        #
        # Register only if actual business data was written.
        # We do not create a Glue partition for a folder
        # containing only a manifest.
        # ----------------------------------------------------

        if total_records > 0:

            register_marketplace_partition(
                table_name=(
                    "raw_marketplace_vehicles"
                ),
                dataset="vehicles",
                extract_date=extract_date,
                run_id=run_id,
            )

        else:

            print(
                "No vehicle records extracted. "
                "Skipping Glue partition registration."
            )


        print(
            "Vehicles extraction complete: "
            f"{manifest}"
        )

    finally:

        if cursor is not None:

            cursor.close()

        if db is not None:

            db.close()


# ============================================================
# LISTINGS EXTRACTION
# ============================================================

def extract_listings():

    (
        extract_date,
        run_id,
    ) = get_run_information()


    # --------------------------------------------------------
    # Incremental watermark
    # --------------------------------------------------------

    watermark_name = (
        "marketplace_listings_watermark"
    )

    previous_watermark = (
        Variable.get(
            watermark_name,
            default_var=None
        )
    )


    db = None
    cursor = None

    try:

        # ----------------------------------------------------
        # Connections
        # ----------------------------------------------------

        db = (
            get_database_connection()
        )

        s3 = (
            get_s3_client()
        )

        print(
            "Connected to marketplace PostgreSQL "
            "for listings extraction."
        )


        # ----------------------------------------------------
        # Determine source high watermark
        # ----------------------------------------------------

        control_cursor = (
            db.cursor()
        )

        control_cursor.execute(
            """
            SELECT MAX(updated_at)
            FROM listings
            """
        )

        high_watermark = (
            control_cursor
            .fetchone()[0]
        )

        control_cursor.close()


        print(
            "Previous listings watermark: "
            f"{previous_watermark}"
        )

        print(
            "Current high watermark: "
            f"{high_watermark}"
        )


        # ----------------------------------------------------
        # Empty source
        # ----------------------------------------------------

        if high_watermark is None:

            print(
                "Listings table is empty. "
                "Nothing to extract."
            )

            return


        # ----------------------------------------------------
        # Retrieve source columns
        # ----------------------------------------------------

        columns = get_table_columns(
            db,
            "listings"
        )

        print(
            f"Listings columns: "
            f"{columns}"
        )


        # ----------------------------------------------------
        # Server-side cursor
        # ----------------------------------------------------

        cursor = db.cursor(
            name="listings_cursor"
        )

        cursor.itersize = (
            BATCH_SIZE
        )


        # ----------------------------------------------------
        # Incremental extraction
        # ----------------------------------------------------

        if previous_watermark:

            mode = "incremental"

            print(
                "Running incremental "
                "listings extraction."
            )

            cursor.execute(
                """
                SELECT *
                FROM listings
                WHERE updated_at > %s
                  AND updated_at <= %s
                ORDER BY updated_at
                """,
                (
                    previous_watermark,
                    high_watermark,
                ),
            )


        # ----------------------------------------------------
        # Initial full extraction
        # ----------------------------------------------------

        else:

            mode = "full"

            print(
                "No listings watermark exists. "
                "Running initial full extraction."
            )

            cursor.execute(
                """
                SELECT *
                FROM listings
                WHERE updated_at <= %s
                ORDER BY updated_at
                """,
                (
                    high_watermark,
                ),
            )


        # ----------------------------------------------------
        # Stream PostgreSQL -> S3
        # ----------------------------------------------------

        total_records = 0
        part_number = 1

        raw_prefix = build_raw_prefix(
            dataset="listings",
            extract_date=extract_date,
            run_id=run_id,
        )

        while True:

            rows = cursor.fetchmany(
                BATCH_SIZE
            )

            if not rows:
                break

            s3_key = (
                f"{raw_prefix}"
                f"part-{part_number:04d}"
                ".jsonl.gz"
            )

            print(
                f"Uploading "
                f"{len(rows)} listings "
                f"to {s3_key}"
            )

            upload_batch(
                s3=s3,
                rows=rows,
                columns=columns,
                s3_key=s3_key,
            )

            total_records += (
                len(rows)
            )

            print(
                f"Uploaded listings part "
                f"{part_number}: "
                f"{len(rows)} rows"
            )

            part_number += 1


        # ----------------------------------------------------
        # Manifest
        # ----------------------------------------------------

        manifest = {
            "table": "listings",
            "mode": mode,
            "extract_date": (
                extract_date
            ),
            "run_id": run_id,
            "previous_watermark": (
                previous_watermark
            ),
            "new_watermark": (
                high_watermark
                .isoformat()
            ),
            "records": (
                total_records
            ),
            "batch_size": (
                BATCH_SIZE
            ),
            "parts": (
                part_number - 1
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


        # ----------------------------------------------------
        # Glue partition registration
        #
        # IMPORTANT:
        #
        # S3 must succeed first.
        # Glue registration must succeed second.
        # Only THEN may the watermark advance.
        # ----------------------------------------------------

        if total_records > 0:

            register_marketplace_partition(
                table_name=(
                    "raw_marketplace_listings"
                ),
                dataset="listings",
                extract_date=extract_date,
                run_id=run_id,
            )

        else:

            print(
                "No new listing records extracted. "
                "Skipping Glue partition registration."
            )


        # ----------------------------------------------------
        # Update watermark
        #
        # This happens only after:
        #
        # PostgreSQL extraction succeeded
        # S3 uploads succeeded
        # manifest succeeded
        # Glue registration succeeded
        #
        # If registration fails, Airflow fails this task and
        # does NOT advance the watermark.
        # ----------------------------------------------------

        Variable.set(
            watermark_name,
            high_watermark.isoformat()
        )


        print(
            "Listings watermark updated to: "
            f"{high_watermark.isoformat()}"
        )

        print(
            "Listings extraction complete: "
            f"{manifest}"
        )


    finally:

        if cursor is not None:

            cursor.close()

        if db is not None:

            db.close()


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id=(
        "marketplace_raw_ingestion"
    ),

    description=(
        "Extract marketplace PostgreSQL "
        "tables into AWS S3 RAW and register "
        "exact AWS Glue partitions"
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
        "marketplace",
        "postgresql",
        "raw",
        "s3",
        "glue",
    ],

) as dag:


    extract_vehicles_task = (
        PythonOperator(

            task_id=(
                "extract_vehicles"
            ),

            python_callable=(
                extract_vehicles
            ),
        )
    )


    extract_listings_task = (
        PythonOperator(

            task_id=(
                "extract_listings"
            ),

            python_callable=(
                extract_listings
            ),
        )
    )