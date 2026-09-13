import os
import time

import boto3


AWS_REGION = "us-east-1"

ATHENA_RESULTS = (
    "s3://european-auto-marketplace-data-yash-2026-01/"
    "athena-results/"
)


def get_athena_client():

    profile = os.getenv(
        "AWS_PROFILE",
        "auto-marketplace"
    )

    session = boto3.Session(
        profile_name=profile,
        region_name=AWS_REGION,
    )

    return session.client(
        "athena"
    )


def execute_query(
    sql,
    database,
    timeout_seconds=120,
):

    athena = get_athena_client()

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": database
        },
        ResultConfiguration={
            "OutputLocation": ATHENA_RESULTS
        },
    )

    execution_id = (
        response["QueryExecutionId"]
    )

    started = time.time()

    while True:

        status_response = (
            athena.get_query_execution(
                QueryExecutionId=execution_id
            )
        )

        status = (
            status_response[
                "QueryExecution"
            ][
                "Status"
            ][
                "State"
            ]
        )

        if status == "SUCCEEDED":
            break

        if status in (
            "FAILED",
            "CANCELLED",
        ):

            reason = (
                status_response[
                    "QueryExecution"
                ][
                    "Status"
                ].get(
                    "StateChangeReason",
                    "Unknown Athena error",
                )
            )

            raise RuntimeError(
                f"Athena query {status}: "
                f"{reason}\n"
                f"SQL:\n{sql}"
            )

        if (
            time.time() - started
            > timeout_seconds
        ):

            athena.stop_query_execution(
                QueryExecutionId=execution_id
            )

            raise TimeoutError(
                "Athena query timed out."
            )

        time.sleep(2)

    return execution_id


def execute_scalar(
    sql,
    database,
):

    athena = get_athena_client()

    execution_id = execute_query(
        sql=sql,
        database=database,
    )

    response = athena.get_query_results(
        QueryExecutionId=execution_id,
        MaxResults=2,
    )

    rows = (
        response[
            "ResultSet"
        ][
            "Rows"
        ]
    )

    if len(rows) < 2:

        raise RuntimeError(
            "Athena query returned "
            "no result row."
        )

    data = (
        rows[1]
        .get(
            "Data",
            []
        )
    )

    if not data:

        return None

    return data[0].get(
        "VarCharValue"
    )