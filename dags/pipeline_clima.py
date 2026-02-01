from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.ingest import ingest_data

def run_ingest():
    ingest_data()

with DAG(
    dag_id="pipeline_clima",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["clima", "inmet"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_inmet",
        python_callable=run_ingest,
    )

    ingest_task
