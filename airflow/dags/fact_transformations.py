from pathlib import Path
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from platform_utils.athena_queries import execute_query

DATABASE = "auto_marketplace_analytics"
SQL_ROOT = Path("/opt/airflow/sql/facts")

JOBS = {
    "fact_revenue": "fact_revenue.sql",
    "fact_subscription": "fact_subscription.sql",
    "fact_listing_performance": "fact_listing_performance.sql",
    "fact_sales_target": "fact_sales_target.sql"
}

def run_sql_model(model_name, sql_file):
    path = SQL_ROOT / sql_file

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    sql = path.read_text(encoding="utf-8")

    print(f"Running {model_name} from {path}")

    execution_id = execute_query(
        sql=sql,
        database=DATABASE,
    )

    print(f"Completed {model_name}. Athena execution ID: {execution_id}")

with DAG(
    dag_id="fact_transformations",
    description="Build analytical fact tables",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=['facts', 'athena', 'iceberg', 'sql'],
) as dag:

    start = EmptyOperator(task_id="start")
    tasks = []

    for model_name, sql_file in JOBS.items():
        task = PythonOperator(
            task_id=f"build_{model_name}",
            python_callable=run_sql_model,
            op_kwargs={
                "model_name": model_name,
                "sql_file": sql_file,
            },
        )
        tasks.append(task)

    complete = EmptyOperator(task_id="fact_transformations_complete")

    start >> tasks
    tasks >> complete
