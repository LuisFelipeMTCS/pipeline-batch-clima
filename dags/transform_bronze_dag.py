"""
DAG Transformação: RAW → BRONZE
================================

Processa dados RAW (parquet do S3) para camada BRONZE:
- Limpeza de dados
- Remoção de duplicados
- Padronização de tipos
- Validação de qualidade

Paraleliza por fonte: Clima, Agricultura, Solos
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os

from src.transformation.pipelines.bronze import (
    validar_camada_bronze,
    enviar_metricas,
    gerar_resumo_final
)
from src.common.observability import ObservabilityManager


# =========================
# CONFIGURAÇÕES
# =========================

observability = ObservabilityManager(
    pipeline_name="transform_bronze",
    namespace=os.getenv("METRIC_NAMESPACE_BRONZE", "pipeline_bronze"),
    log_group=os.getenv("LOG_GROUP_NAME_BRONZE", "/transformacao/bronze"),
    sns_topic_arn=os.getenv("SNS_ALERT_TOPIC_ARN", ""),
    aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
)


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

def transformar_raw_to_bronze_clima(**context):
    """Executa transformação RAW→BRONZE para Clima e salva stats no XCom"""
    import logging
    logger = logging.getLogger(__name__)

    try:
        from src.transformation.spark.jobs.bronze_job import processar_clima_bronze
        from src.transformation.spark.spark_session import criar_spark_session

        logger.info("🌤️  Transformando RAW → BRONZE (Clima)...")

        spark = criar_spark_session("bronze-clima")

        # Paths
        raw_path = "s3a://datalake-lab1/raw/clima/year=*/"
        bronze_path = "s3a://datalake-lab1/bronze/clima/"

        # Processa
        df = processar_clima_bronze(spark, raw_path, bronze_path)

        stats = {
            'fonte': 'clima',
            'registros': df.count() if df else 0,
            'camada': 'bronze'
        }

        context['ti'].xcom_push(key='stats_bronze_clima', value=stats)
        logger.info(f"✅ BRONZE Clima: {stats['registros']} registros")
        return stats

    except Exception as e:
        logger.error(f"❌ Erro RAW→BRONZE Clima: {e}")
        raise


def transformar_raw_to_bronze_culturas(**context):
    """Executa transformação RAW→BRONZE para Culturas e salva stats no XCom"""
    import logging
    logger = logging.getLogger(__name__)

    try:
        from src.transformation.spark.jobs.bronze_job import processar_culturas_bronze
        from src.transformation.spark.spark_session import criar_spark_session

        logger.info("🌾 Transformando RAW → BRONZE (Culturas)...")

        spark = criar_spark_session("bronze-culturas")

        # Agrupa agricultura e solos (ambos são culturas)
        raw_agricultura = "s3a://datalake-lab1/raw/agricultura/year=*/"
        raw_solos = "s3a://datalake-lab1/raw/solos/year=*/"
        bronze_path = "s3a://datalake-lab1/bronze/culturas/"

        # Passar ambos os paths como tupla
        df = processar_culturas_bronze(spark, (raw_agricultura, raw_solos), bronze_path)

        stats = {
            'fonte': 'culturas',
            'registros': df.count() if df else 0,
            'camada': 'bronze'
        }

        context['ti'].xcom_push(key='stats_bronze_culturas', value=stats)
        logger.info(f"✅ BRONZE Culturas: {stats['registros']} registros")
        return stats

    except Exception as e:
        logger.error(f"❌ Erro RAW→BRONZE Culturas: {e}")
        raise


def validar_bronze(**context):
    """Valida transformações e salva resultado no XCom"""
    ti = context['ti']
    stats_clima = ti.xcom_pull(key='stats_bronze_clima', task_ids='bronze_clima')
    stats_culturas = ti.xcom_pull(key='stats_bronze_culturas', task_ids='bronze_culturas')

    result = validar_camada_bronze(stats_clima, stats_culturas)
    context['ti'].xcom_push(key='validacao_resultado', value=result)
    return result


def enviar_metricas_cloudwatch(**context):
    """Envia métricas para CloudWatch"""
    ti = context['ti']
    resultado = ti.xcom_pull(key='validacao_resultado', task_ids='validar_bronze')
    enviar_metricas(resultado, observability)


def enviar_resumo_final_wrapper(**context):
    """Envia resumo final (sucesso ou erro)"""
    gerar_resumo_final(
        ti=context['ti'],
        dag_run=context['dag_run'],
        task_ids=['bronze_clima', 'bronze_culturas', 'validar_bronze'],
        observability=observability,
        pipeline_name="transform_bronze"
    )


# =========================
# DAG DEFINITION
# =========================

with DAG(
    dag_id='transform_bronze',
    default_args=default_args,
    description='Transformação RAW → BRONZE (limpeza e padronização)',
    schedule_interval='@daily',
    catchup=False,
    tags=['transformation', 'bronze', 'limpeza'],
) as dag:

    # ==========================================
    # SENSORES: Aguardam ingestões
    # ==========================================

    aguardar_clima = ExternalTaskSensor(
        task_id='aguardar_ingestao_clima',
        external_dag_id='pipeline_clima',
        external_task_id='ingest_clima',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )

    aguardar_agricultura = ExternalTaskSensor(
        task_id='aguardar_ingestao_agricultura',
        external_dag_id='pipeline_agricultura',
        external_task_id='ingest_agricultura',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )

    aguardar_solos = ExternalTaskSensor(
        task_id='aguardar_ingestao_solos',
        external_dag_id='pipeline_solos',
        external_task_id='ingest_solos',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )

    # ==========================================
    # TRANSFORMAÇÕES RAW → BRONZE
    # ==========================================

    bronze_clima = PythonOperator(
        task_id='bronze_clima',
        python_callable=transformar_raw_to_bronze_clima,
        on_failure_callback=observability.criar_callback_erro(),
    )

    bronze_culturas = PythonOperator(
        task_id='bronze_culturas',
        python_callable=transformar_raw_to_bronze_culturas,
        on_failure_callback=observability.criar_callback_erro(),
    )

    # ==========================================
    # VALIDAÇÃO
    # ==========================================

    validacao = PythonOperator(
        task_id='validar_bronze',
        python_callable=validar_bronze,
        on_failure_callback=observability.criar_callback_erro(),
    )

    # ==========================================
    # MÉTRICAS E ALERTAS
    # ==========================================

    metricas = PythonOperator(
        task_id='enviar_metricas_cloudwatch',
        python_callable=enviar_metricas_cloudwatch,
        trigger_rule='all_success',
    )

    resumo = PythonOperator(
        task_id='enviar_resumo_final',
        python_callable=enviar_resumo_final_wrapper,
        trigger_rule='all_done',
    )

    # ==========================================
    # FLUXO
    # ==========================================

    aguardar_clima >> bronze_clima
    [aguardar_agricultura, aguardar_solos] >> bronze_culturas

    [bronze_clima, bronze_culturas] >> validacao
    validacao >> [metricas, resumo]
