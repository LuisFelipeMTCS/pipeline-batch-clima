"""DAG Pipeline Agricultura [TODO]"""
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG("pipeline_agricultura", start_date=datetime(2024,1,1), schedule_interval="@monthly", catchup=False, tags=["agricultura"]) as dag:
    DummyOperator(task_id="not_implemented")
