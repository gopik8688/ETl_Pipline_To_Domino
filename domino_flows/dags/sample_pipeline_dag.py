from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from domino_flows.operators import extract, transform, validate, incremental_load, load
from domino_flows.utils.alerts import task_failure_alert

default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sample_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=['domino', 'migration'],
) as dag:

    t_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={'source': None},
    )

    t_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    t_validate = PythonOperator(
        task_id='validate',
        python_callable=validate,
    )

    t_incremental = PythonOperator(
        task_id='incremental_load',
        python_callable=incremental_load,
    )

    t_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    t_extract >> t_transform >> t_validate >> t_incremental >> t_load
