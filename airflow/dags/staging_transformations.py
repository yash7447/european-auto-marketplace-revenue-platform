from datetime import timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, get_current_context

from platform_utils.athena_queries import execute_query, execute_scalar


RAW_DATABASE = "auto_marketplace_raw"
STAGING_DATABASE = "auto_marketplace_staging"
SQL_ROOT = Path("/opt/airflow/sql/staging")


STAGING_JOBS = {
    "marketplace_vehicles": {
        "target_table": "stg_marketplace_vehicles",
        "sql_file": "stg_marketplace_vehicles.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "marketplace_listings": {
        "target_table": "stg_marketplace_listings",
        "sql_file": "stg_marketplace_listings.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "commercial_accounts": {
        "target_table": "stg_commercial_accounts",
        "sql_file": "stg_commercial_accounts.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "commercial_contracts": {
        "target_table": "stg_commercial_contracts",
        "sql_file": "stg_commercial_contracts.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "commercial_subscriptions": {
        "target_table": "stg_commercial_subscriptions",
        "sql_file": "stg_commercial_subscriptions.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "commercial_invoices": {
        "target_table": "stg_commercial_invoices",
        "sql_file": "stg_commercial_invoices.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "pricing_products": {
        "target_table": "stg_pricing_products",
        "sql_file": "stg_pricing_products.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "pricing_prices": {
        "target_table": "stg_pricing_prices",
        "sql_file": "stg_pricing_prices.sql",
        "filter_column": "extract_date",
        "lookback_days": 0,
    },
    "engagement_events": {
        "target_table": "stg_engagement_events",
        "sql_file": "stg_engagement_events.sql",
        "filter_column": "event_date",
        "lookback_days": 2,
    },
    "sales_targets": {
        "target_table": "stg_sales_targets",
        "sql_file": "stg_sales_targets.sql",
        "filter_column": None,
        "lookback_days": 0,
    },
}


def target_is_empty(table_name):
    value = execute_scalar(
        sql=f"SELECT COUNT(*) FROM {STAGING_DATABASE}.{table_name}",
        database=STAGING_DATABASE,
    )
    count = int(value)
    print(f"{table_name} currently has {count} rows.")
    return count == 0


def load_sql(file_name):
    file_path = SQL_ROOT / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    sql = file_path.read_text(encoding="utf-8")

    if "__SOURCE_FILTER__" not in sql:
        raise RuntimeError(f"{file_name} does not contain __SOURCE_FILTER__")

    return sql


def build_source_filter(job, logical_date, bootstrap):
    if bootstrap:
        return "1 = 1"

    filter_column = job["filter_column"]

    if filter_column is None:
        # Content-addressed Sales Targets are small. Reconcile all versions.
        return "1 = 1"

    end_date = logical_date.strftime("%Y-%m-%d")
    lookback_days = int(job.get("lookback_days", 0))

    if lookback_days > 0:
        start_date = logical_date.subtract(days=lookback_days).strftime("%Y-%m-%d")
        return (
            f"{filter_column} >= '{start_date}' "
            f"AND {filter_column} <= '{end_date}'"
        )

    return f"{filter_column} = '{end_date}'"


def _read_date_scalar(sql, database):
    value = execute_scalar(
        sql=sql,
        database=database,
    )

    if value in (None, "", "NULL"):
        return None

    return pendulum.parse(str(value)).start_of("day")


def _month_windows(start_date, end_date):
    """Yield [start, end) windows containing at most one calendar month."""
    final_exclusive = end_date.add(days=1)
    month_cursor = start_date.start_of("month")

    while month_cursor < final_exclusive:
        next_month = month_cursor.add(months=1)

        batch_start = max(start_date, month_cursor)
        batch_end_exclusive = min(final_exclusive, next_month)

        if batch_start < batch_end_exclusive:
            yield batch_start, batch_end_exclusive

        month_cursor = next_month


def run_engagement_job(job):
    """
    Process engagement in monthly batches.

    Athena Iceberg allows at most 100 simultaneously open partition writers.
    The historical engagement bootstrap spans more than 100 event_date
    partitions, so a single MERGE can fail with ICEBERG_TOO_MANY_OPEN_PARTITIONS.

    Monthly MERGEs keep each statement well below that limit. The target's
    MAX(event_date) acts as a recovery watermark, so a failed bootstrap can
    safely resume instead of starting over.
    """
    raw_min = _read_date_scalar(
        sql=(
            f"SELECT MIN(event_date) "
            f"FROM {RAW_DATABASE}.raw_engagement_events"
        ),
        database=RAW_DATABASE,
    )

    raw_max = _read_date_scalar(
        sql=(
            f"SELECT MAX(event_date) "
            f"FROM {RAW_DATABASE}.raw_engagement_events"
        ),
        database=RAW_DATABASE,
    )

    if raw_min is None or raw_max is None:
        print("No RAW engagement events found. Nothing to stage.")
        return

    target_max = _read_date_scalar(
        sql=(
            f"SELECT MAX(event_date) "
            f"FROM {STAGING_DATABASE}.{job['target_table']}"
        ),
        database=STAGING_DATABASE,
    )

    if target_max is None:
        start_date = raw_min
        mode = "BOOTSTRAP"
    else:
        start_date = max(
            raw_min,
            target_max.subtract(days=int(job.get("lookback_days", 2))),
        )
        mode = "INCREMENTAL/RESUME"

    if start_date > raw_max:
        print(
            "Engagement staging is already ahead of the latest RAW event_date. "
            "Nothing to process."
        )
        return

    print(f"engagement_events: {mode} MODE")
    print(
        "Processing engagement range "
        f"{start_date.to_date_string()} through {raw_max.to_date_string()} "
        "in monthly batches."
    )

    sql_template = load_sql(job["sql_file"])

    for batch_start, batch_end_exclusive in _month_windows(start_date, raw_max):
        source_filter = (
            f"event_date >= '{batch_start.to_date_string()}' "
            f"AND event_date < '{batch_end_exclusive.to_date_string()}'"
        )

        print("=" * 80)
        print(f"Engagement source filter: {source_filter}")
        print("=" * 80)

        sql = sql_template.replace(
            "__SOURCE_FILTER__",
            source_filter,
        )

        execution_id = execute_query(
            sql=sql,
            database=STAGING_DATABASE,
        )

        print(
            "Engagement MERGE batch completed. "
            f"Athena execution ID: {execution_id}"
        )


def run_staging_job(job_name):
    if job_name not in STAGING_JOBS:
        raise ValueError(f"Unknown staging job: {job_name}")

    job = STAGING_JOBS[job_name]

    # Engagement needs bounded batches because the historical bootstrap spans
    # more than Athena's 100-open-Iceberg-writer limit.
    if job_name == "engagement_events":
        run_engagement_job(job)
        return

    context = get_current_context()
    logical_date = context["logical_date"]

    target_table = job["target_table"]

    bootstrap = target_is_empty(target_table)
    source_filter = build_source_filter(job, logical_date, bootstrap)

    mode = "BOOTSTRAP" if bootstrap else "INCREMENTAL"
    print(f"{job_name}: {mode} MODE")
    print(f"Source filter: {source_filter}")

    sql = load_sql(job["sql_file"]).replace(
        "__SOURCE_FILTER__",
        source_filter,
    )

    execution_id = execute_query(
        sql=sql,
        database=STAGING_DATABASE,
    )

    print(f"MERGE completed. Athena execution ID: {execution_id}")


with DAG(
    dag_id="staging_transformations",
    description="Transform Athena RAW data into Iceberg staging tables",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["staging", "athena", "iceberg", "sql"],
) as dag:

    start = EmptyOperator(task_id="start")

    staging_tasks = []

    for job_name in STAGING_JOBS:
        task = PythonOperator(
            task_id=f"merge_{job_name}",
            python_callable=run_staging_job,
            op_kwargs={"job_name": job_name},
        )
        staging_tasks.append(task)

    staging_complete = EmptyOperator(task_id="staging_complete")

    start >> staging_tasks >> staging_complete
