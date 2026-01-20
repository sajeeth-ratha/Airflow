from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.walmart_pipeline import task_data_quality, task_transform, task_load

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="walmart_etl_orchestration",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "data-engineering"],
) as dag:

    dq = PythonOperator(
        task_id="data_quality",
        python_callable=task_data_quality,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    dq >> transform >> load
