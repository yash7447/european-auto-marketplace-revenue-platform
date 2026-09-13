from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(

    dag_id="daily_platform_orchestration",

    description=(
        "Master daily orchestration for the "
        "European Auto Marketplace data platform"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="UTC",
    ),

    # Run every day at 02:00 UTC
    schedule="0 2 * * *",

    catchup=False,

    max_active_runs=1,

    default_args={
        "retries": 1,
        "retry_delay": timedelta(
            minutes=2
        ),
    },

    tags=[
        "platform",
        "orchestration",
        "daily",
    ],

) as dag:


    # ========================================================
    # START
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )


    # ========================================================
    # MARKETPLACE RAW
    # ========================================================

    trigger_marketplace = TriggerDagRunOperator(

        task_id=(
            "trigger_marketplace_raw_ingestion"
        ),

        trigger_dag_id=(
            "marketplace_raw_ingestion"
        ),

        wait_for_completion=True,

        poke_interval=20,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],
    )


    # ========================================================
    # COMMERCIAL RAW
    # ========================================================

    trigger_commercial = TriggerDagRunOperator(

        task_id=(
            "trigger_commercial_raw_ingestion"
        ),

        trigger_dag_id=(
            "commercial_raw_ingestion"
        ),

        wait_for_completion=True,

        poke_interval=20,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],
    )


    # ========================================================
    # PRICING RAW
    # ========================================================

    trigger_pricing = TriggerDagRunOperator(

        task_id=(
            "trigger_pricing_raw_ingestion"
        ),

        trigger_dag_id=(
            "pricing_raw_ingestion"
        ),

        wait_for_completion=True,

        poke_interval=20,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],
    )


    # ========================================================
    # ENGAGEMENT RAW
    # ========================================================

    trigger_engagement = TriggerDagRunOperator(

        task_id=(
            "trigger_engagement_raw_ingestion"
        ),

        trigger_dag_id=(
            "engagement_raw_ingestion"
        ),

        wait_for_completion=True,

        poke_interval=20,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],
    )


    # ========================================================
    # SALES TARGETS RAW
    # ========================================================

    trigger_sales_targets = TriggerDagRunOperator(

        task_id=(
            "trigger_sales_targets_raw_ingestion"
        ),

        trigger_dag_id=(
            "sales_targets_raw_ingestion"
        ),

        wait_for_completion=True,

        poke_interval=20,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],
    )


    # ========================================================
    # RAW INGESTION COMPLETE
    # ========================================================

    raw_ingestion_complete = EmptyOperator(
        task_id="raw_ingestion_complete"
    )


    # ========================================================
    # RAW QUALITY CHECKS
    # ========================================================

    trigger_raw_quality_checks = TriggerDagRunOperator(

        task_id=(
            "trigger_raw_quality_checks"
        ),

        trigger_dag_id=(
            "raw_quality_checks"
        ),

        wait_for_completion=True,

        poke_interval=20,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed"
        ],
    )


    # ========================================================
    # RAW QUALITY COMPLETE
    # ========================================================

    raw_quality_complete = EmptyOperator(
        task_id="raw_quality_complete"
    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    start >> [
        trigger_marketplace,
        trigger_commercial,
        trigger_pricing,
        trigger_engagement,
        trigger_sales_targets,
    ]


    [
        trigger_marketplace,
        trigger_commercial,
        trigger_pricing,
        trigger_engagement,
        trigger_sales_targets,
    ] >> raw_ingestion_complete


    raw_ingestion_complete >> (
        trigger_raw_quality_checks
    )


    trigger_raw_quality_checks >> (
        raw_quality_complete
    )