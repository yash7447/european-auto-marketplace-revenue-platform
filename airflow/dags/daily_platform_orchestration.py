from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def trigger(task_id, dag_id):
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )


with DAG(
    dag_id="daily_platform_orchestration",
    description=(
        "End-to-end commercial data platform: RAW ingestion, quality, "
        "staging, intermediate models, star schema and BI marts"
    ),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["master", "orchestration", "commercial-analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    marketplace = trigger(
        "trigger_marketplace_raw",
        "marketplace_raw_ingestion",
    )

    commercial = trigger(
        "trigger_commercial_raw",
        "commercial_raw_ingestion",
    )

    pricing = trigger(
        "trigger_pricing_raw",
        "pricing_raw_ingestion",
    )

    engagement = trigger(
        "trigger_engagement_raw",
        "engagement_raw_ingestion",
    )

    sales_targets = trigger(
        "trigger_sales_targets_raw",
        "sales_targets_raw_ingestion",
    )

    raw_ingestion_complete = EmptyOperator(
        task_id="raw_ingestion_complete"
    )

    raw_quality = trigger(
        "trigger_raw_quality_checks",
        "raw_quality_checks",
    )

    raw_quality_complete = EmptyOperator(
        task_id="raw_quality_complete"
    )

    staging = trigger(
        "trigger_staging_transformations",
        "staging_transformations",
    )

    staging_complete = EmptyOperator(
        task_id="staging_layer_complete"
    )

    staging_quality = trigger(
        "trigger_staging_quality_checks",
        "staging_quality_checks",
    )

    staging_quality_complete = EmptyOperator(
        task_id="staging_quality_complete"
    )

    intermediate = trigger(
        "trigger_intermediate_transformations",
        "intermediate_transformations",
    )

    intermediate_complete = EmptyOperator(
        task_id="intermediate_layer_complete"
    )

    dimensions = trigger(
        "trigger_dimensional_transformations",
        "dimensional_transformations",
    )

    dimensions_complete = EmptyOperator(
        task_id="dimensions_complete"
    )

    facts = trigger(
        "trigger_fact_transformations",
        "fact_transformations",
    )

    facts_complete = EmptyOperator(
        task_id="facts_complete"
    )

    marts = trigger(
        "trigger_mart_transformations",
        "mart_transformations",
    )

    marts_complete = EmptyOperator(
        task_id="marts_complete"
    )

    analytics_quality = trigger(
        "trigger_analytics_quality_checks",
        "analytics_quality_checks",
    )

    platform_complete = EmptyOperator(
        task_id="platform_complete"
    )

    start >> [
        marketplace,
        commercial,
        pricing,
        engagement,
        sales_targets,
    ]

    [
        marketplace,
        commercial,
        pricing,
        engagement,
        sales_targets,
    ] >> raw_ingestion_complete

    raw_ingestion_complete >> raw_quality >> raw_quality_complete
    raw_quality_complete >> staging >> staging_complete
    staging_complete >> staging_quality >> staging_quality_complete
    staging_quality_complete >> intermediate >> intermediate_complete
    intermediate_complete >> dimensions >> dimensions_complete
    dimensions_complete >> facts >> facts_complete
    facts_complete >> marts >> marts_complete
    marts_complete >> analytics_quality >> platform_complete
