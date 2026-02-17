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
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def transformar_raw_to_bronze_clima(**context):
    """RAW → BRONZE para CLIMA"""
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
    """RAW → BRONZE para CULTURAS (Agricultura + Solos)"""
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


def validar_camada_bronze(**context):
    """Valida que todas as transformações BRONZE foram OK"""
    logger = logging.getLogger(__name__)
    
    ti = context['ti']
    
    stats_clima = ti.xcom_pull(key='stats_bronze_clima', task_ids='bronze_clima')
    stats_culturas = ti.xcom_pull(key='stats_bronze_culturas', task_ids='bronze_culturas')
    
    logger.info("=" * 70)
    logger.info("🥉 VALIDAÇÃO CAMADA BRONZE")
    logger.info("=" * 70)
    logger.info(f"🌤️  Clima: {stats_clima}")
    logger.info(f"🌾 Culturas: {stats_culturas}")
    logger.info("=" * 70)
    
    if not all([stats_clima, stats_culturas]):
        raise Exception("❌ Uma ou mais transformações BRONZE falharam!")
    
    total_registros = (
        stats_clima.get('registros', 0) + 
        stats_culturas.get('registros', 0)
    )
    
    logger.info(f"✅ BRONZE validado: {total_registros:,} registros totais")
    
    return {
        'clima': stats_clima,
        'culturas': stats_culturas,
        'total_registros': total_registros,
        'camada': 'bronze',
        'status': 'success'
    }


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
    )
    
    bronze_culturas = PythonOperator(
        task_id='bronze_culturas',
        python_callable=transformar_raw_to_bronze_culturas,
    )
    
    # ==========================================
    # VALIDAÇÃO
    # ==========================================
    
    validacao = PythonOperator(
        task_id='validar_bronze',
        python_callable=validar_camada_bronze,
    )
    
    # ==========================================
    # FLUXO
    # ==========================================
    
    aguardar_clima >> bronze_clima
    [aguardar_agricultura, aguardar_solos] >> bronze_culturas
    
    [bronze_clima, bronze_culturas] >> validacao
