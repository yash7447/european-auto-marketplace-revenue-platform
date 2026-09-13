from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from platform_utils.athena_queries import execute_scalar


RAW_DATABASE = "auto_marketplace_raw"


def assert_positive_count(
    table_name,
):

    sql = (
        f"SELECT COUNT(*) "
        f"FROM {table_name}"
    )

    value = execute_scalar(
        sql=sql,
        database=RAW_DATABASE,
    )

    row_count = int(value)

    print(
        f"{table_name}: "
        f"{row_count} rows/files"
    )

    if row_count <= 0:

        raise ValueError(
            f"RAW quality check failed: "
            f"{table_name} is empty."
        )


def assert_no_null_key(
    table_name,
    key_column,
):

    sql = (
        f"SELECT COUNT(*) "
        f"FROM {table_name} "
        f"WHERE {key_column} IS NULL"
    )

    value = execute_scalar(
        sql=sql,
        database=RAW_DATABASE,
    )

    null_count = int(value)

    print(
        f"{table_name}.{key_column}: "
        f"{null_count} null keys"
    )

    if null_count > 0:

        raise ValueError(
            f"RAW quality check failed: "
            f"{table_name}.{key_column} "
            f"contains {null_count} NULL values."
        )


with DAG(

    dag_id="raw_quality_checks",

    description=(
        "Validate Athena RAW datasets before "
        "allowing staging transformations"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="UTC",
    ),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    default_args={
        "retries": 1,
        "retry_delay": timedelta(
            seconds=30
        ),
    },

    tags=[
        "quality",
        "raw",
        "athena",
    ],

) as dag:


    check_marketplace_vehicles_count = (
        PythonOperator(
            task_id=(
                "check_marketplace_vehicles_count"
            ),
            python_callable=(
                assert_positive_count
            ),
            op_kwargs={
                "table_name": (
                    "raw_marketplace_vehicles"
                )
            },
        )
    )


    check_marketplace_listings_count = (
        PythonOperator(
            task_id=(
                "check_marketplace_listings_count"
            ),
            python_callable=(
                assert_positive_count
            ),
            op_kwargs={
                "table_name": (
                    "raw_marketplace_listings"
                )
            },
        )
    )


    check_vehicle_keys = (
        PythonOperator(
            task_id="check_vehicle_keys",
            python_callable=(
                assert_no_null_key
            ),
            op_kwargs={
                "table_name": (
                    "raw_marketplace_vehicles"
                ),
                "key_column": (
                    "vehicle_id"
                ),
            },
        )
    )


    check_listing_keys = (
        PythonOperator(
            task_id="check_listing_keys",
            python_callable=(
                assert_no_null_key
            ),
            op_kwargs={
                "table_name": (
                    "raw_marketplace_listings"
                ),
                "key_column": (
                    "listing_id"
                ),
            },
        )
    )


    check_engagement_count = (
        PythonOperator(
            task_id=(
                "check_engagement_count"
            ),
            python_callable=(
                assert_positive_count
            ),
            op_kwargs={
                "table_name": (
                    "raw_engagement_events"
                )
            },
        )
    )


    check_engagement_keys = (
        PythonOperator(
            task_id=(
                "check_engagement_event_keys"
            ),
            python_callable=(
                assert_no_null_key
            ),
            op_kwargs={
                "table_name": (
                    "raw_engagement_events"
                ),
                "key_column": (
                    "event_id"
                ),
            },
        )
    )


    check_sales_targets_count = (
        PythonOperator(
            task_id=(
                "check_sales_targets_count"
            ),
            python_callable=(
                assert_positive_count
            ),
            op_kwargs={
                "table_name": (
                    "raw_sales_targets"
                )
            },
        )
    )


    check_sales_target_keys = (
        PythonOperator(
            task_id=(
                "check_sales_target_keys"
            ),
            python_callable=(
                assert_no_null_key
            ),
            op_kwargs={
                "table_name": (
                    "raw_sales_targets"
                ),
                "key_column": (
                    "target_id"
                ),
            },
        )
    )