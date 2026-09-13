from datetime import datetime
import os

import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator


BUCKET_NAME = "european-auto-marketplace-data-yash-2026-01"


def test_s3_connection():

    profile = os.getenv(
        "AWS_PROFILE",
        "auto-marketplace"
    )

    session = boto3.Session(
        profile_name=profile
    )

    s3 = session.client(
        "s3",
        region_name="us-east-1"
    )

    key = "test/airflow/connection-test.txt"

    content = (
        "Airflow successfully connected "
        "to AWS S3."
    )

    print(
        f"Uploading to "
        f"s3://{BUCKET_NAME}/{key}"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=content.encode("utf-8")
    )

    response = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )

    returned_content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    print(
        f"Read back from S3: "
        f"{returned_content}"
    )

    assert returned_content == content

    print(
        "AIRFLOW -> AWS S3 TEST PASSED"
    )


with DAG(
    dag_id="aws_s3_connection_test",
    description=(
        "Validate Airflow authentication "
        "and connectivity to project S3 bucket"
    ),
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "aws",
        "s3",
        "validation"
    ],
) as dag:

    test_s3 = PythonOperator(
        task_id="test_s3_connection",
        python_callable=test_s3_connection,
    )
