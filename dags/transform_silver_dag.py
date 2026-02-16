"""
DAG Transformação: BRONZE → SILVER
===================================

Processa dados BRONZE para camada SILVER:
- Agregações diárias/mensais
- Cálculos de métricas
- Enriquecimento de dados
- Criação de dimensões

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


def transformar_bronze_to_silver_clima(**context):
    """BRONZE → SILVER para CLIMA (agregações diárias, mensais, estacionais)"""
    logger = logging.getLogger(__name__)
    
    try:
        from src.transformation.spark.jobs.silver_job import (
            criar_dim_tempo, 
            agregar_clima_diario,
            agregar_clima_mensal,
            agregar_clima_estacional
        )
        from src.transformation.spark.spark_session import criar_spark_session
        
        logger.info("🌤️  Transformando BRONZE → SILVER (Clima)...")
        
        spark = criar_spark_session("silver-clima")
        
        bronze_path = "s3://datalake-lab1/bronze/clima/"
        silver_base = "s3://datalake-lab1/silver/"
        
        # 1. Criar dimensão tempo
        logger.info("📅 Criando dimensão tempo...")
        df_bronze = spark.read.parquet(bronze_path)
        df_dim_tempo = criar_dim_tempo(spark, df_bronze, f"{silver_base}dim_tempo/")
        
        # 2. Agregação diária
        logger.info("📊 Agregando clima diário...")
        df_diario = agregar_clima_diario(spark, bronze_path, f"{silver_base}clima_diario/")
        
        # 3. Agregação mensal
        logger.info("📊 Agregando clima mensal...")
        df_mensal = agregar_clima_mensal(spark, f"{silver_base}clima_diario/", f"{silver_base}clima_mensal/")
        
        # 4. Agregação estacional
        logger.info("📊 Agregando clima estacional...")
        df_estacional = agregar_clima_estacional(spark, f"{silver_base}clima_mensal/", f"{silver_base}clima_estacional/")
        
        stats = {
            'fonte': 'clima',
            'dim_tempo': df_dim_tempo.count() if df_dim_tempo else 0,
            'diario': df_diario.count() if df_diario else 0,
            'mensal': df_mensal.count() if df_mensal else 0,
            'estacional': df_estacional.count() if df_estacional else 0,
            'camada': 'silver'
        }
        
        context['ti'].xcom_push(key='stats_silver_clima', value=stats)
        
        logger.info(f"✅ SILVER Clima: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Erro BRONZE→SILVER Clima: {e}")
        raise


def transformar_bronze_to_silver_culturas(**context):
    """BRONZE → SILVER para CULTURAS (enriquecimento)"""
    logger = logging.getLogger(__name__)
    
    try:
        from src.transformation.spark.jobs.silver_job import enriquecer_culturas
        from src.transformation.spark.spark_session import criar_spark_session
        
        logger.info("🌾 Transformando BRONZE → SILVER (Culturas)...")
        
        spark = criar_spark_session("silver-culturas")
        
        bronze_path = "s3://datalake-lab1/bronze/culturas/"
        silver_path = "s3://datalake-lab1/silver/culturas_enriquecidas/"
        
        df = enriquecer_culturas(spark, bronze_path, silver_path)
        
        stats = {
            'fonte': 'culturas',
            'registros': df.count() if df else 0,
            'camada': 'silver'
        }
        
        context['ti'].xcom_push(key='stats_silver_culturas', value=stats)
        
        logger.info(f"✅ SILVER Culturas: {stats['registros']} registros")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Erro BRONZE→SILVER Culturas: {e}")
        raise


def validar_camada_silver(**context):
    """Valida que todas as transformações SILVER foram OK"""
    logger = logging.getLogger(__name__)
    
    ti = context['ti']
    
    stats_clima = ti.xcom_pull(key='stats_silver_clima', task_ids='silver_clima')
    stats_culturas = ti.xcom_pull(key='stats_silver_culturas', task_ids='silver_culturas')
    
    logger.info("=" * 70)
    logger.info("🥈 VALIDAÇÃO CAMADA SILVER")
    logger.info("=" * 70)
    logger.info(f"🌤️  Clima: {stats_clima}")
    logger.info(f"🌾 Culturas: {stats_culturas}")
    logger.info("=" * 70)
    
    if not all([stats_clima, stats_culturas]):
        raise Exception("❌ Uma ou mais transformações SILVER falharam!")
    
    logger.info(f"✅ SILVER validado com sucesso!")
    
    return {
        'clima': stats_clima,
        'culturas': stats_culturas,
        'camada': 'silver',
        'status': 'success'
    }


with DAG(
    dag_id='transform_silver',
    default_args=default_args,
    description='Transformação BRONZE → SILVER (agregações e métricas)',
    schedule_interval='@daily',
    catchup=False,
    tags=['transformation', 'silver', 'agregacao'],
) as dag:
    
    # ==========================================
    # SENSORES: Aguardam BRONZE
    # ==========================================
    
    aguardar_bronze = ExternalTaskSensor(
        task_id='aguardar_bronze',
        external_dag_id='transform_bronze',
        external_task_id='validar_bronze',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )
    
    # ==========================================
    # TRANSFORMAÇÕES BRONZE → SILVER
    # ==========================================
    
    silver_clima = PythonOperator(
        task_id='silver_clima',
        python_callable=transformar_bronze_to_silver_clima,
    )
    
    silver_culturas = PythonOperator(
        task_id='silver_culturas',
        python_callable=transformar_bronze_to_silver_culturas,
    )
    
    # ==========================================
    # VALIDAÇÃO
    # ==========================================
    
    validacao = PythonOperator(
        task_id='validar_silver',
        python_callable=validar_camada_silver,
    )
    
    # ==========================================
    # FLUXO
    # ==========================================
    
    aguardar_bronze >> [silver_clima, silver_culturas]
    
    [silver_clima, silver_culturas] >> validacao
