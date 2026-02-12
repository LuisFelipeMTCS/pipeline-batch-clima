"""DAG Pipeline Solos [TODO]"""
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG("pipeline_solos", start_date=datetime(2024,1,1), schedule_interval="@weekly", catchup=False, tags=["solos"]) as dag:
    DummyOperator(task_id="not_implemented")
