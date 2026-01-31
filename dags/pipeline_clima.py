from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.ingest import ingest_data

default_args = {
    'owner': 'Luis Felipe',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_clima',
    default_args=default_args,
    description='Pipeline de ingestão de dados climáticos do INMET',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['clima', 'inmet'],
) as dag:

    ingest = PythonOperator(
        task_id='ingest_dados_inmet',
        python_callable=ingest_data
    )

    ingest
