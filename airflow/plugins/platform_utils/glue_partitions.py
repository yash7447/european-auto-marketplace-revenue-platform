from copy import deepcopy

import boto3
from botocore.exceptions import ClientError


AWS_REGION = "us-east-1"


def register_partition(
    database_name: str,
    table_name: str,
    partition_values: dict,
    s3_location: str,
    region_name: str = AWS_REGION,
) -> bool:
    """
    Register one existing S3 partition in AWS Glue.

    Example:

        register_partition(
            database_name="auto_marketplace_raw",
            table_name="raw_marketplace_listings",
            partition_values={
                "extract_date": "2026-09-13",
                "run_id": "scheduled__..."
            },
            s3_location=(
                "s3://bucket/raw/marketplace/listings/"
                "extract_date=2026-09-13/"
                "run_id=scheduled__.../"
            ),
        )

    Returns:
        True  -> partition created
        False -> partition already existed
    """

    glue = boto3.client(
        "glue",
        region_name=region_name,
    )

    # --------------------------------------------------------
    # Get table definition from Glue.
    #
    # This means we do NOT hard-code:
    # - JSON SerDe
    # - input format
    # - output format
    # - column definitions
    #
    # We inherit those settings from the RAW table.
    # --------------------------------------------------------

    response = glue.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )

    table = response["Table"]

    partition_keys = [
        item["Name"]
        for item in table.get(
            "PartitionKeys",
            []
        )
    ]

    if not partition_keys:
        raise RuntimeError(
            f"{database_name}.{table_name} "
            "is not partitioned."
        )

    supplied_keys = set(
        partition_values.keys()
    )

    expected_keys = set(
        partition_keys
    )

    if supplied_keys != expected_keys:
        raise ValueError(
            "Partition keys do not match.\n"
            f"Table: {database_name}.{table_name}\n"
            f"Expected: {partition_keys}\n"
            f"Received: {list(partition_values.keys())}"
        )

    # Glue requires values in the exact same order as the
    # table's PartitionKeys definition.
    values = [
        str(partition_values[key])
        for key in partition_keys
    ]

    # --------------------------------------------------------
    # Copy the RAW table's storage configuration.
    # Only the physical S3 location changes.
    # --------------------------------------------------------

    storage_descriptor = deepcopy(
        table["StorageDescriptor"]
    )

    storage_descriptor["Location"] = (
        s3_location.rstrip("/") + "/"
    )

    partition_input = {
        "Values": values,
        "StorageDescriptor": (
            storage_descriptor
        ),
        "Parameters": {
            "registered_by": "airflow",
        },
    }

    try:

        glue.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input,
        )

        print(
            "Glue partition registered:"
        )

        print(
            f"  Table: "
            f"{database_name}.{table_name}"
        )

        print(
            f"  Values: {partition_values}"
        )

        print(
            f"  Location: {s3_location}"
        )

        return True

    except glue.exceptions.AlreadyExistsException:

        print(
            "Glue partition already exists:"
        )

        print(
            f"  Table: "
            f"{database_name}.{table_name}"
        )

        print(
            f"  Values: {partition_values}"
        )

        return False

    except ClientError as exc:

        raise RuntimeError(
            "Glue partition registration failed.\n"
            f"Database: {database_name}\n"
            f"Table: {table_name}\n"
            f"Partition: {partition_values}\n"
            f"Location: {s3_location}\n"
            f"AWS error: {exc}"
        ) from exc