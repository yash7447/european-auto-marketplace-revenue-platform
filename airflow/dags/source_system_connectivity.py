from datetime import datetime
from pathlib import Path
import json
import urllib.request

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_commercial_api():

    url = "http://commercial-api:8000/health"

    with urllib.request.urlopen(
        url,
        timeout=10
    ) as response:

        data = json.loads(
            response.read().decode("utf-8")
        )

    print(data)

    assert data["status"] == "healthy"
    assert data["accounts"] == 5000
    assert data["contracts"] == 8000
    assert data["subscriptions"] == 12000
    assert data["invoices"] == 60000


def check_pricing_api():

    url = "http://pricing-api:8000/health"

    with urllib.request.urlopen(
        url,
        timeout=10
    ) as response:

        data = json.loads(
            response.read().decode("utf-8")
        )

    print(data)

    assert data["status"] == "healthy"
    assert data["products"] == 12
    assert data["prices"] == 2376


def check_marketplace_database():

    conn = psycopg2.connect(
        host="marketplace-db",
        port=5432,
        dbname="marketplace",
        user="marketuser",
        password="marketpassword",
    )

    try:

        cur = conn.cursor()

        cur.execute(
            "SELECT COUNT(*) FROM vehicles;"
        )

        vehicles = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM listings;"
        )

        listings = cur.fetchone()[0]

        print(
            f"Vehicles: {vehicles}"
        )

        print(
            f"Listings: {listings}"
        )

        assert vehicles == 100000
        assert listings == 150000

    finally:

        conn.close()


def check_engagement_files():

    folder = Path(
        "/opt/airflow/sample_data/"
        "engagement_events"
    )

    files = list(
        folder.glob(
            "event_date=*/events.jsonl.gz"
        )
    )

    print(
        f"Engagement partitions: {len(files)}"
    )

    assert len(files) > 0


def check_sales_targets():

    file_path = Path(
        "/opt/airflow/sample_data/"
        "sales_targets/"
        "sales_targets.csv"
    )

    print(
        f"Sales target file: {file_path}"
    )

    assert file_path.exists()

    with file_path.open(
        "r",
        encoding="utf-8"
    ) as file:

        row_count = sum(1 for _ in file) - 1

    print(
        f"Sales target rows: {row_count}"
    )

    assert row_count == 2880


with DAG(
    dag_id="source_system_connectivity",
    description=(
        "Validate connectivity to all "
        "European auto marketplace sources"
    ),
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "source-validation",
        "commercial"
    ],
) as dag:

    commercial_api = PythonOperator(
        task_id="check_commercial_api",
        python_callable=check_commercial_api,
    )

    marketplace_db = PythonOperator(
        task_id="check_marketplace_database",
        python_callable=check_marketplace_database,
    )

    pricing_api = PythonOperator(
        task_id="check_pricing_api",
        python_callable=check_pricing_api,
    )

    engagement_files = PythonOperator(
        task_id="check_engagement_files",
        python_callable=check_engagement_files,
    )

    sales_targets = PythonOperator(
        task_id="check_sales_targets",
        python_callable=check_sales_targets,
    )


    [
        commercial_api,
        marketplace_db,
        pricing_api,
        engagement_files,
        sales_targets,
    ]
