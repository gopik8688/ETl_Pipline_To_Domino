"""
GPS/Davnic → Domino Flows Migration POC

This DAG demonstrates migration of a legacy multi-step ETL workflow
to Domino Flows (Apache Airflow).

Pipeline:

Extract
    ↓
Transform
    ↓
Validate
    ↓
Incremental Load
    ↓
Load
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from domino_flows.operators import (
    extract,
    transform,
    validate,
    incremental_load,
    load,
)

from domino_flows.utils.alerts import task_failure_alert


# ==========================================================
# Default Configuration
# ==========================================================

default_args = {
    "owner": "Domino Migration Team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# ==========================================================
# DAG Definition
# ==========================================================

with DAG(

    dag_id="sample_pipeline",

    description="""
    Migration of a legacy GPS/Davnic ETL workflow
    into Domino Flows (Apache Airflow).
    """,

    default_args=default_args,

    start_date=datetime(2026, 1, 1),

    # Daily at 2 AM
    schedule_interval="0 2 * * *",

    catchup=False,

    on_failure_callback=task_failure_alert,

    tags=[
        "domino",
        "migration",
        "etl",
        "airflow",
        "poc",
    ],

) as dag:

    # ======================================================
    # Extract
    # ======================================================

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        op_kwargs={
            "source": None,
        },
        doc_md="""
        ### Extract Stage

        Reads data from the source system and writes it
        into the Domino staging area.

        Output:
        - extracted.parquet
        """,
    )

    # ======================================================
    # Transform
    # ======================================================

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        doc_md="""
        ### Transform Stage

        Applies business transformations.

        Output:
        - transformed.parquet
        """,
    )

    # ======================================================
    # Validate
    # ======================================================

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        doc_md="""
        ### Validation Stage

        Performs:

        - Schema Validation
        - Null Validation
        - Duplicate Validation
        - Data Type Validation
        """,
    )

    # ======================================================
    # Incremental Load
    # ======================================================

    t_incremental = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load,
        doc_md="""
        ### Incremental Load

        Uses watermark-based loading.

        Output:
        - incremental.parquet
        """,
    )

    # ======================================================
    # Load
    # ======================================================

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        doc_md="""
        ### Load Stage

        Loads processed data into the destination system.

        Current implementation uses a placeholder.
        """,
    )

    # ======================================================
    # Workflow Dependency
    # ======================================================

    (
        t_extract
        >> t_transform
        >> t_validate
        >> t_incremental
        >> t_load
    )