"""
DAG Load: Bronze S3 → PostgreSQL
==================================

Carrega os arquivos Parquet da camada bronze no S3
para o schema `bronze` no PostgreSQL.

Aguarda a conclusão da DAG transform_bronze antes de executar.

Fluxo:
    transform_bronze → load_clima (paralelo) → validar_load
                     → load_culturas (paralelo) ↗
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# =========================
# TASK WRAPPERS
# =========================

def load_clima_task(**context):
    """Carrega bronze/clima do S3 → PostgreSQL bronze.clima"""
    import logging
    logger = logging.getLogger(__name__)

    from src.load.targets.postgres import carregar_clima, criar_engine, criar_schema

    logger.info("Carregando S3 bronze/clima → PostgreSQL bronze.clima...")

    engine = criar_engine()
    criar_schema(engine, schema="bronze")
    carregar_clima(engine)

    logger.info("bronze.clima carregada com sucesso!")


def load_culturas_task(**context):
    """Carrega bronze/culturas do S3 → PostgreSQL bronze.culturas"""
    import logging
    logger = logging.getLogger(__name__)

    from src.load.targets.postgres import carregar_culturas, criar_engine, criar_schema

    logger.info("Carregando S3 bronze/culturas → PostgreSQL bronze.culturas...")

    engine = criar_engine()
    criar_schema(engine, schema="bronze")
    carregar_culturas(engine)

    logger.info("bronze.culturas carregada com sucesso!")


def validar_load_task(**context):
    """Valida que as tabelas foram criadas e têm registros"""
    import logging
    from sqlalchemy import create_engine, text

    logger = logging.getLogger(__name__)

    PG_HOST     = os.getenv("PG_HOST",     "postgres")
    PG_PORT     = os.getenv("PG_PORT",     "5432")
    PG_USER     = os.getenv("PG_USER",     "airflow")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")
    PG_DB       = os.getenv("PG_DB",       "datalake")

    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(url)

    with engine.connect() as conn:
        for tabela in ["bronze.clima", "bronze.culturas"]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {tabela}"))
            count = result.scalar()
            if count == 0:
                raise ValueError(f"Tabela {tabela} está vazia após o load!")
            logger.info(f"{tabela}: {count:,} registros")

    logger.info("Load validado com sucesso!")


# =========================
# DAG DEFINITION
# =========================

with DAG(
    dag_id='load_dag',
    default_args=default_args,
    description='Carrega S3 Bronze → PostgreSQL (schema bronze)',
    schedule_interval='@daily',
    catchup=False,
    tags=['load', 'bronze', 'postgres'],
) as dag:

    # Aguarda o bronze estar pronto no S3
    aguardar_bronze = ExternalTaskSensor(
        task_id='aguardar_transform_bronze',
        external_dag_id='transform_dag',
        external_task_id='validar_bronze',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )

    load_clima = PythonOperator(
        task_id='load_clima',
        python_callable=load_clima_task,
    )

    load_culturas = PythonOperator(
        task_id='load_culturas',
        python_callable=load_culturas_task,
    )

    validacao = PythonOperator(
        task_id='validar_load',
        python_callable=validar_load_task,
    )

    # Fluxo: aguarda bronze → carrega clima e culturas em paralelo → valida
    aguardar_bronze >> [load_clima, load_culturas] >> validacao
