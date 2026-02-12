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
    tags=["clima", "inmet"],
    params={"year_filter": None, "max_workers": 10},
) as dag:

    def run_ingest(**context):
        p = context["params"]
        return ingest_data(
            year_filter=p.get("year_filter"),
            max_workers=p.get("max_workers", 10),
        )

    PythonOperator(
        task_id="ingest_clima",
        python_callable=run_ingest,
    )