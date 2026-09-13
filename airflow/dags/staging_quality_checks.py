from pathlib import Path
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from platform_utils.athena_queries import execute_scalar


DATABASE = "auto_marketplace_staging"

SQL_ROOT = Path(
    "/opt/airflow/sql/quality/staging"
)


QUALITY_CHECKS = {
    "null_primary_keys": (
        "q01_null_primary_keys.sql"
    ),
    "duplicate_primary_keys": (
        "q02_duplicate_primary_keys.sql"
    ),
    "listing_vehicle_integrity": (
        "q03_listing_vehicle_integrity.sql"
    ),
    "contract_account_integrity": (
        "q04_contract_account_integrity.sql"
    ),
    "subscription_contract_integrity": (
        "q05_subscription_contract_integrity.sql"
    ),
    "subscription_account_integrity": (
        "q06_subscription_account_integrity.sql"
    ),
    "invoice_subscription_integrity": (
        "q07_invoice_subscription_integrity.sql"
    ),
    "invoice_account_integrity": (
        "q08_invoice_account_integrity.sql"
    ),
    "price_product_integrity": (
        "q09_price_product_integrity.sql"
    ),
    "engagement_listing_integrity": (
        "q10_engagement_listing_integrity.sql"
    ),
}


def run_quality_check(
    check_name,
    sql_file,
):
    path = SQL_ROOT / sql_file

    if not path.exists():
        raise FileNotFoundError(
            f"Quality SQL not found: {path}"
        )

    sql = path.read_text(
        encoding="utf-8"
    )

    print(
        f"Running staging quality check: "
        f"{check_name}"
    )

    bad_rows = execute_scalar(
        sql=sql,
        database=DATABASE,
    )

    if bad_rows is None:
        raise RuntimeError(
            f"{check_name} returned no result."
        )

    bad_rows = int(
        bad_rows
    )

    print(
        f"{check_name}: "
        f"{bad_rows} bad rows"
    )

    if bad_rows != 0:
        raise RuntimeError(
            f"QUALITY CHECK FAILED: "
            f"{check_name} found "
            f"{bad_rows} bad rows."
        )

    print(
        f"QUALITY CHECK PASSED: "
        f"{check_name}"
    )


with DAG(

    dag_id="staging_quality_checks",

    description=(
        "Validate staged Iceberg data before "
        "downstream dimensional modelling"
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
            minutes=1
        ),
    },

    tags=[
        "quality",
        "staging",
        "athena",
        "iceberg",
    ],

) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    quality_tasks = []

    for (
        check_name,
        sql_file,
    ) in QUALITY_CHECKS.items():

        task = PythonOperator(

            task_id=(
                f"check_{check_name}"
            ),

            python_callable=(
                run_quality_check
            ),

            op_kwargs={
                "check_name": (
                    check_name
                ),
                "sql_file": (
                    sql_file
                ),
            },
        )

        quality_tasks.append(
            task
        )

    quality_complete = EmptyOperator(
        task_id="staging_quality_complete"
    )

    start >> quality_tasks

    quality_tasks >> quality_complete
