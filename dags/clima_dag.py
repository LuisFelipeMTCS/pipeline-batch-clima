"""DAG Pipeline Clima (INMET)"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.ingestion.pipelines.clima import ingest_data


with DAG(
    dag_id="pipeline_clima",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["clima", "clima"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_clima",
        python_callable=ingest_data,
    )
