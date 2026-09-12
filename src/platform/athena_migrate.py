import argparse
import hashlib
import json
import re
import sys
import time

from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


# ============================================================
# MIGRATION FILE NAMING
#
# Examples:
# V001__create_raw_database.sql
# V012__create_staging_database.sql
# ============================================================

MIGRATION_PATTERN = re.compile(
    r"^(V\d{3,})__(.+)\.sql$"
)


# ============================================================
# NORMALIZE SQL
#
# Important:
# Windows normally uses CRLF:
#
# \r\n
#
# Linux normally uses LF:
#
# \n
#
# Jenkins runs on Linux while development is on Windows.
#
# We normalize SQL before calculating SHA256 so the same
# migration gets the same checksum on both operating systems.
# ============================================================

def read_normalized_sql(path: Path) -> str:

    text = path.read_text(
        encoding="utf-8-sig"
    )

    # Normalize line endings.
    text = (
        text
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    # Remove trailing spaces from each line.
    lines = [
        line.rstrip()
        for line in text.splitlines()
    ]

    # Normalize beginning/end of file.
    normalized = (
        "\n".join(lines).strip()
        + "\n"
    )

    return normalized


# ============================================================
# SHA256
# ============================================================

def calculate_sha256_from_sql(
    sql: str
) -> str:

    return hashlib.sha256(
        sql.encode("utf-8")
    ).hexdigest()


# ============================================================
# AWS SESSION
# ============================================================

def create_session(
    profile,
    region,
):

    if profile:

        return boto3.Session(
            profile_name=profile,
            region_name=region,
        )

    return boto3.Session(
        region_name=region
    )


# ============================================================
# DISCOVER MIGRATIONS
# ============================================================

def get_migrations(
    migrations_dir: Path
):

    migrations = []

    for path in migrations_dir.glob(
        "V*.sql"
    ):

        match = MIGRATION_PATTERN.match(
            path.name
        )

        if not match:

            raise ValueError(
                "Invalid migration filename: "
                f"{path.name}"
            )

        version = match.group(1)
        description = match.group(2)

        sql = read_normalized_sql(
            path
        )

        if not sql.strip():

            raise ValueError(
                "Migration is empty: "
                f"{path.name}"
            )

        checksum = (
            calculate_sha256_from_sql(
                sql
            )
        )

        migrations.append(
            {
                "version": version,
                "description": description,
                "filename": path.name,
                "path": path,
                "sql": sql,
                "sha256": checksum,
            }
        )

    # Sort:
    #
    # V001
    # V002
    # V003
    # ...
    migrations.sort(
        key=lambda migration: int(
            migration["version"][1:]
        )
    )

    if not migrations:

        raise RuntimeError(
            "No migrations found in "
            f"{migrations_dir}"
        )

    # --------------------------------------------------------
    # Detect duplicate migration versions
    # --------------------------------------------------------

    versions = [
        migration["version"]
        for migration in migrations
    ]

    duplicates = {
        version
        for version in versions
        if versions.count(version) > 1
    }

    if duplicates:

        raise RuntimeError(
            "Duplicate migration versions: "
            f"{sorted(duplicates)}"
        )

    return migrations


# ============================================================
# MIGRATION HISTORY KEY
#
# Example:
#
# platform-control/
# schema-migrations-v2/
# V012__create_staging_database.sql.json
# ============================================================

def marker_key(
    history_prefix,
    migration,
):

    prefix = (
        history_prefix.rstrip("/")
    )

    return (
        f"{prefix}/"
        f"{migration['filename']}.json"
    )


# ============================================================
# READ MIGRATION HISTORY
# ============================================================

def read_marker(
    s3,
    bucket,
    key,
):

    try:

        response = s3.get_object(
            Bucket=bucket,
            Key=key,
        )

        body = (
            response["Body"]
            .read()
            .decode("utf-8")
        )

        return json.loads(
            body
        )

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

            return None

        raise


# ============================================================
# WRITE MIGRATION HISTORY
# ============================================================

def write_marker(
    s3,
    bucket,
    key,
    migration,
    query_execution_id,
):

    marker = {

        "version": (
            migration["version"]
        ),

        "filename": (
            migration["filename"]
        ),

        "description": (
            migration["description"]
        ),

        "sha256": (
            migration["sha256"]
        ),

        "query_execution_id": (
            query_execution_id
        ),

        "applied_at": (
            datetime
            .now(timezone.utc)
            .isoformat()
        ),
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            marker,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json",
    )


# ============================================================
# START ATHENA QUERY
# ============================================================

def start_query(
    athena,
    sql,
    output_location,
):

    response = (
        athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={
                "OutputLocation": (
                    output_location
                )
            },
        )
    )

    return response[
        "QueryExecutionId"
    ]


# ============================================================
# WAIT FOR ATHENA QUERY
# ============================================================

def wait_for_query(
    athena,
    query_execution_id,
    timeout_seconds,
):

    started = time.time()

    while True:

        response = (
            athena.get_query_execution(
                QueryExecutionId=(
                    query_execution_id
                )
            )
        )

        status = (
            response[
                "QueryExecution"
            ]["Status"]
        )

        state = status["State"]

        print(
            f"    Athena state: "
            f"{state}"
        )

        if state == "SUCCEEDED":

            return

        if state in (
            "FAILED",
            "CANCELLED",
        ):

            reason = status.get(
                "StateChangeReason",
                "No failure reason returned."
            )

            raise RuntimeError(
                f"Athena query {state}: "
                f"{reason}"
            )

        elapsed = (
            time.time() - started
        )

        if elapsed > timeout_seconds:

            try:

                athena.stop_query_execution(
                    QueryExecutionId=(
                        query_execution_id
                    )
                )

            finally:

                raise TimeoutError(
                    "Athena query exceeded "
                    f"{timeout_seconds} seconds."
                )

        time.sleep(2)


# ============================================================
# MAIN MIGRATION PROCESS
# ============================================================

def migrate(args):

    migrations_dir = Path(
        args.migrations_dir
    ).resolve()

    migrations = get_migrations(
        migrations_dir
    )

    session = create_session(
        args.profile,
        args.region,
    )

    athena = session.client(
        "athena"
    )

    s3 = session.client(
        "s3"
    )

    output_location = (
        f"s3://{args.bucket}/"
        f"{args.athena_results_prefix.strip('/')}/"
    )

    print()
    print(
        "========================================"
    )
    print(
        "ATHENA SCHEMA MIGRATION"
    )
    print(
        "========================================"
    )

    print(
        "Migrations directory: "
        f"{migrations_dir}"
    )

    print(
        "Migration count: "
        f"{len(migrations)}"
    )

    print(
        "AWS region: "
        f"{args.region}"
    )

    print(
        "S3 bucket: "
        f"{args.bucket}"
    )

    print(
        "Athena results: "
        f"{output_location}"
    )

    print(
        "Migration history: "
        f"s3://{args.bucket}/"
        f"{args.history_prefix}/"
    )

    print()

    applied_count = 0
    skipped_count = 0


    # ========================================================
    # PROCESS MIGRATIONS IN VERSION ORDER
    # ========================================================

    for migration in migrations:

        key = marker_key(
            args.history_prefix,
            migration,
        )

        print(
            "----------------------------------------"
        )

        print(
            f"{migration['version']} "
            f"{migration['description']}"
        )

        print(
            f"File: "
            f"{migration['filename']}"
        )

        print(
            f"SHA256: "
            f"{migration['sha256']}"
        )

        existing_marker = read_marker(
            s3,
            args.bucket,
            key,
        )


        # ====================================================
        # MIGRATION ALREADY APPLIED
        # ====================================================

        if existing_marker:

            recorded_hash = (
                existing_marker.get(
                    "sha256"
                )
            )

            current_hash = (
                migration["sha256"]
            )

            # ------------------------------------------------
            # Applied migrations are immutable.
            # ------------------------------------------------

            if (
                recorded_hash
                != current_hash
            ):

                raise RuntimeError(
                    "\n"
                    "APPLIED MIGRATION WAS MODIFIED.\n\n"
                    f"File: "
                    f"{migration['filename']}\n"
                    f"Recorded SHA256: "
                    f"{recorded_hash}\n"
                    f"Current SHA256: "
                    f"{current_hash}\n\n"
                    "Never edit an applied migration.\n"
                    "Create a new migration version "
                    "instead."
                )

            print(
                "    Already applied "
                "- checksum matches."
            )

            skipped_count += 1

            continue


        # ====================================================
        # NEW MIGRATION
        # ====================================================

        print(
            "    New migration."
        )

        print(
            "    Executing in Athena..."
        )

        query_execution_id = (
            start_query(
                athena,
                migration["sql"],
                output_location,
            )
        )

        print(
            "    QueryExecutionId: "
            f"{query_execution_id}"
        )

        wait_for_query(
            athena,
            query_execution_id,
            args.timeout,
        )


        # ====================================================
        # WRITE HISTORY ONLY AFTER SUCCESS
        # ====================================================

        write_marker(
            s3,
            args.bucket,
            key,
            migration,
            query_execution_id,
        )

        print(
            "    Migration applied "
            "and recorded."
        )

        applied_count += 1


    # ========================================================
    # SUMMARY
    # ========================================================

    print()
    print(
        "========================================"
    )
    print(
        "MIGRATION SUMMARY"
    )
    print(
        "========================================"
    )

    print(
        f"Applied: {applied_count}"
    )

    print(
        f"Skipped: {skipped_count}"
    )

    print(
        f"Total:   {len(migrations)}"
    )

    print(
        "========================================"
    )


# ============================================================
# COMMAND LINE ARGUMENTS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Apply versioned Athena / Glue "
            "schema migrations."
        )
    )


    parser.add_argument(

        "--migrations-dir",

        default=(
            "sql/migrations"
        ),

        help=(
            "Directory containing "
            "versioned SQL migration files."
        ),
    )


    parser.add_argument(

        "--bucket",

        default=(
            "european-auto-marketplace-"
            "data-yash-2026-01"
        ),
    )


    parser.add_argument(

        "--region",

        default="us-east-1",
    )


    parser.add_argument(

        "--profile",

        default=None,

        help=(
            "Optional AWS profile. "
            "Used locally and by the "
            "current Jenkins environment. "
            "Can be omitted when using "
            "an IAM role."
        ),
    )


    parser.add_argument(

        "--athena-results-prefix",

        default=(
            "athena-results"
        ),
    )


    # --------------------------------------------------------
    # V2 history:
    #
    # The first bootstrap history used raw file byte hashes.
    #
    # V2 uses normalized SQL hashes so Windows and Linux
    # produce identical checksums.
    # --------------------------------------------------------

    parser.add_argument(

        "--history-prefix",

        default=(
            "platform-control/"
            "schema-migrations-v2"
        ),
    )


    parser.add_argument(

        "--timeout",

        type=int,

        default=600,

        help=(
            "Maximum seconds to wait for "
            "each Athena migration."
        ),
    )


    return parser.parse_args()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        arguments = parse_args()

        migrate(
            arguments
        )

    except Exception as exc:

        print(
            "\n========================================",
            file=sys.stderr,
        )

        print(
            "MIGRATION FAILED",
            file=sys.stderr,
        )

        print(
            "========================================",
            file=sys.stderr,
        )

        print(
            str(exc),
            file=sys.stderr,
        )

        sys.exit(1)