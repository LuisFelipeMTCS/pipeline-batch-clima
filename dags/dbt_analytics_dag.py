"""
DAG dbt: Transformações Analytics
====================================

Executa os modelos dbt após o load no PostgreSQL.
Gera as camadas staging, intermediate e marts no schema analytics_*.

Aguarda a conclusão da DAG load_postgres antes de executar.

Fluxo:
    load_postgres → dbt_run → dbt_test
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os

# Caminho absoluto para a pasta dbt dentro do container
DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "dbt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='dbt_analytics',
    default_args=default_args,
    description='Executa modelos dbt: staging → intermediate → marts',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'analytics', 'marts'],
) as dag:

    # Aguarda o load no PostgreSQL estar concluído
    aguardar_load = ExternalTaskSensor(
        task_id='aguardar_load_postgres',
        external_dag_id='load_dag',
        external_task_id='validar_load',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )

    # Executa todos os modelos dbt em ordem (staging → intermediate → marts)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --profiles-dir .",
    )

    # Roda os testes de qualidade definidos nos schema.yml
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --profiles-dir .",
    )

    aguardar_load >> dbt_run >> dbt_test
