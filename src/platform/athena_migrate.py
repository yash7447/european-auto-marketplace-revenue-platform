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


MIGRATION_PATTERN = re.compile(
    r"^(V\d{3,})__(.+)\.sql$"
)


def read_normalized_sql(path: Path) -> str:
    """
    Read SQL in a platform-independent way.

    - Removes UTF-8 BOM
    - Converts CRLF/CR to LF
    - Removes trailing whitespace
    - Guarantees one final newline
    """

    text = path.read_text(
        encoding="utf-8-sig"
    )

    text = (
        text
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    lines = [
        line.rstrip()
        for line in text.splitlines()
    ]

    return (
        "\n".join(lines).strip()
        + "\n"
    )


def calculate_sha256_from_sql(
    sql: str
) -> str:

    return hashlib.sha256(
        sql.encode("utf-8")
    ).hexdigest()


def create_session(profile, region):
    if profile:
        return boto3.Session(
            profile_name=profile,
            region_name=region,
        )

    return boto3.Session(
        region_name=region
    )


def get_migrations(migrations_dir: Path):
    migrations = []

    for path in migrations_dir.glob("V*.sql"):

        match = MIGRATION_PATTERN.match(
            path.name
        )

        if not match:
            raise ValueError(
                f"Invalid migration filename: "
                f"{path.name}"
            )

        version = match.group(1)
        description = match.group(2)

        sql = read_normalized_sql(
            path
        )

        if not sql:
            raise ValueError(
                f"Migration is empty: "
                f"{path.name}"
            )

        migrations.append(
            {
                "version": version,
                "description": description,
                "filename": path.name,
                "path": path,
                "sql": sql,
                "sha256": calculate_sha256_from_sql(
                    sql
                ),
            }
        )

    migrations.sort(
        key=lambda item: int(
            item["version"][1:]
        )
    )

    if not migrations:
        raise RuntimeError(
            f"No migrations found in "
            f"{migrations_dir}"
        )

    # Prevent duplicate version numbers.
    versions = [
        item["version"]
        for item in migrations
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


def marker_key(
    history_prefix,
    migration,
):
    prefix = history_prefix.rstrip("/")

    return (
        f"{prefix}/"
        f"{migration['filename']}.json"
    )


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

        return json.loads(
            response["Body"]
            .read()
            .decode("utf-8")
        )

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
            return None

        raise


def write_marker(
    s3,
    bucket,
    key,
    migration,
    query_execution_id,
):
    marker = {
        "version": migration["version"],
        "filename": migration["filename"],
        "description": (
            migration["description"]
        ),
        "sha256": migration["sha256"],
        "query_execution_id": (
            query_execution_id
        ),
        "applied_at": (
            datetime.now(timezone.utc)
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

        status = response[
            "QueryExecution"
        ]["Status"]

        state = status["State"]

        print(
            f"    Athena state: {state}"
        )

        if state == "SUCCEEDED":
            return

        if state in (
            "FAILED",
            "CANCELLED",
        ):
            reason = status.get(
                "StateChangeReason",
                "No reason returned"
            )

            raise RuntimeError(
                f"Athena query {state}: "
                f"{reason}"
            )

        if (
            time.time() - started
            > timeout_seconds
        ):
            try:
                athena.stop_query_execution(
                    QueryExecutionId=(
                        query_execution_id
                    )
                )
            finally:
                raise TimeoutError(
                    "Athena query exceeded "
                    f"{timeout_seconds} seconds"
                )

        time.sleep(2)


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

    print(
        "========================================"
    )
    print("ATHENA SCHEMA MIGRATION")
    print(
        "========================================"
    )
    print(
        f"Migrations directory: "
        f"{migrations_dir}"
    )
    print(
        f"Migration count: "
        f"{len(migrations)}"
    )
    print(
        f"AWS region: {args.region}"
    )
    print(
        f"S3 bucket: {args.bucket}"
    )
    print()

    applied_count = 0
    skipped_count = 0

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

        existing_marker = read_marker(
            s3,
            args.bucket,
            key,
        )

        if existing_marker:

            old_hash = existing_marker.get(
                "sha256"
            )

            new_hash = migration[
                "sha256"
            ]

            if old_hash != new_hash:
                raise RuntimeError(
                    "\nAPPLIED MIGRATION WAS "
                    "MODIFIED.\n"
                    f"File: "
                    f"{migration['filename']}\n"
                    f"Recorded SHA256: "
                    f"{old_hash}\n"
                    f"Current SHA256: "
                    f"{new_hash}\n\n"
                    "Never edit an applied "
                    "migration. Create a new "
                    "migration version instead."
                )

            print(
                "    Already applied "
                "— checksum matches."
            )

            skipped_count += 1
            continue

        print(
            f"    SHA256: "
            f"{migration['sha256']}"
        )

        print(
            "    Executing migration..."
        )

        query_execution_id = (
            start_query(
                athena,
                migration["sql"],
                output_location,
            )
        )

        print(
            f"    QueryExecutionId: "
            f"{query_execution_id}"
        )

        wait_for_query(
            athena,
            query_execution_id,
            args.timeout,
        )

        # Record migration only AFTER
        # Athena has successfully completed.
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

    print()
    print(
        "========================================"
    )
    print("MIGRATION SUMMARY")
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


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Apply versioned Athena/Glue "
            "schema migrations."
        )
    )

    parser.add_argument(
        "--migrations-dir",
        default="sql/migrations",
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
            "Optional local AWS profile. "
            "Omit in CI when credentials "
            "come from the environment/"
            "IAM role."
        ),
    )

    parser.add_argument(
        "--athena-results-prefix",
        default="athena-results",
    )

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
    )

    return parser.parse_args()


if __name__ == "__main__":

    try:
        migrate(
            parse_args()
        )

    except Exception as exc:

        print(
            f"\nMIGRATION FAILED:\n{exc}",
            file=sys.stderr,
        )

        sys.exit(1)