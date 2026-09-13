from pathlib import Path
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from platform_utils.athena_queries import execute_scalar

DATABASE = "auto_marketplace_analytics"
SQL_ROOT = Path("/opt/airflow/sql/quality/analytics")

QUALITY_CHECKS = {
    "dimension_duplicate_keys": "q01_dimension_duplicate_keys.sql",
    "fact_duplicate_keys": "q02_fact_duplicate_keys.sql",
    "revenue_dimension_integrity": "q03_revenue_dimension_integrity.sql",
    "subscription_dimension_integrity": "q04_subscription_dimension_integrity.sql",
    "listing_dimension_integrity": "q05_listing_dimension_integrity.sql",
    "sales_mart_duplicate_keys": "q06_sales_mart_duplicate_keys.sql",
    "product_mart_duplicate_keys": "q07_product_mart_duplicate_keys.sql",
    "funnel_mart_duplicate_keys": "q08_funnel_mart_duplicate_keys.sql",
}

def run_quality_check(check_name, sql_file):
    path = SQL_ROOT / sql_file
    if not path.exists():
        raise FileNotFoundError(f"Quality SQL not found: {path}")

    sql = path.read_text(encoding="utf-8")
    bad_rows = execute_scalar(sql=sql, database=DATABASE)

    if bad_rows is None:
        raise RuntimeError(f"{check_name} returned no result")

    bad_rows = int(bad_rows)
    print(f"{check_name}: {bad_rows} bad rows")

    if bad_rows != 0:
        raise RuntimeError(
            f"QUALITY CHECK FAILED: {check_name} found {bad_rows} bad rows"
        )

with DAG(
    dag_id="analytics_quality_checks",
    description="Validate dimensions, facts and commercial marts",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["quality", "analytics", "athena"],
) as dag:

    start = EmptyOperator(task_id="start")
    checks = []

    for check_name, sql_file in QUALITY_CHECKS.items():
        task = PythonOperator(
            task_id=f"check_{check_name}",
            python_callable=run_quality_check,
            op_kwargs={
                "check_name": check_name,
                "sql_file": sql_file,
            },
        )
        checks.append(task)

    quality_complete = EmptyOperator(task_id="analytics_quality_complete")

    start >> checks
    checks >> quality_complete
