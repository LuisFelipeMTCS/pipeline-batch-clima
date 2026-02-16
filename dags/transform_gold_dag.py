"""
DAG Transformação: SILVER → GOLD
=================================

Processa dados SILVER para camada GOLD:
- Métricas de negócio
- KPIs consolidados
- Relatórios analíticos
- Dados prontos para consumo

Agrega todas as fontes em métricas finais
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


def gerar_metricas_gold(**context):
    """SILVER → GOLD: Métricas consolidadas - Fato de Risco Climático"""
    logger = logging.getLogger(__name__)
    
    try:
        from src.transformation.spark.jobs.gold_job import gerar_fato_risco_climatico, gerar_relatorio_resumido
        from src.transformation.spark.spark_session import criar_spark_session
        
        logger.info("🥇 Gerando FATO DE RISCO CLIMÁTICO...")
        
        spark = criar_spark_session("gold-risco-climatico")
        
        # Paths Silver (entrada)
        clima_estacional = "s3://datalake-lab1/silver/clima_estacional/"
        culturas = "s3://datalake-lab1/silver/culturas_enriquecidas/"
        
        # Path Gold (saída)
        gold_fato = "s3://datalake-lab1/gold/fato_risco_climatico/"
        gold_relatorio = "s3://datalake-lab1/gold/relatorio_risco/"
        
        # 1. Gera fato de risco climático
        logger.info("📊 Gerando fato: Cultura × Estação × Risco...")
        df_fato = gerar_fato_risco_climatico(
            spark,
            clima_estacional,
            culturas,
            gold_fato
        )
        
        # 2. Gera relatório resumido
        logger.info("📋 Gerando relatório consolidado...")
        df_relatorio = gerar_relatorio_resumido(spark, gold_fato, gold_relatorio)
        
        stats = {
            'fato_registros': df_fato.count() if df_fato else 0,
            'relatorio_linhas': df_relatorio.count() if df_relatorio else 0,
            'camada': 'gold'
        }
        
        context['ti'].xcom_push(key='stats_gold', value=stats)
        
        logger.info(f"✅ GOLD: {stats['fato_registros']} combinações cultura×estação geradas")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Erro SILVER→GOLD: {e}")
        raise


def validar_camada_gold(**context):
    """Valida métricas GOLD"""
    logger = logging.getLogger(__name__)
    
    ti = context['ti']
    stats_gold = ti.xcom_pull(key='stats_gold', task_ids='gerar_metricas')
    
    logger.info("=" * 70)
    logger.info("🥇 VALIDAÇÃO CAMADA GOLD")
    logger.info("=" * 70)
    logger.info(f"📊 Fato Risco Climático: {stats_gold}")
    logger.info("=" * 70)
    
    if not stats_gold or stats_gold.get('fato_registros', 0) == 0:
        raise Exception("❌ Nenhum registro GOLD foi gerado!")
    
    logger.info(f"✅ GOLD validado: {stats_gold['fato_registros']} registros")
    
    return {
        'gold': stats_gold,
        'camada': 'gold',
        'status': 'success'
    }


def relatorio_pipeline_completo(**context):
    """Relatório final consolidado de todo o pipeline"""
    logger = logging.getLogger(__name__)
    
    ti = context['ti']
    validacao = ti.xcom_pull(task_ids='validar_gold')
    
    logger.info("=" * 80)
    logger.info("🎉 PIPELINE MEDALLION COMPLETO!")
    logger.info("=" * 80)
    logger.info(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("✅ CAMADAS PROCESSADAS:")
    logger.info("   🥉 BRONZE: Limpeza e padronização")
    logger.info("   🥈 SILVER: Agregações (diária, mensal, estacional)")
    logger.info("   🥇 GOLD: Fato de Risco Climático (Cultura × Estação)")
    logger.info("")
    logger.info("📂 DADOS DISPONÍVEIS:")
    logger.info("   s3://datalake-lab1/bronze/clima/")
    logger.info("   s3://datalake-lab1/bronze/culturas/")
    logger.info("   s3://datalake-lab1/silver/clima_[diario|mensal|estacional]/")
    logger.info("   s3://datalake-lab1/silver/culturas_enriquecidas/")
    logger.info("   s3://datalake-lab1/gold/fato_risco_climatico/")
    logger.info("   s3://datalake-lab1/gold/relatorio_risco/")
    logger.info("")
    logger.info(f"📊 Combinações Geradas: {validacao.get('gold', {}).get('fato_registros', 0)}")
    logger.info("")
    logger.info("💡 PERGUNTA RESPONDIDA:")
    logger.info("   'Qual o risco climático de cada cultura por estação do ano?'")
    logger.info("")
    logger.info("=" * 80)
    logger.info("✨ Dados prontos para dashboards e análises!")
    logger.info("=" * 80)
    
    return validacao


with DAG(
    dag_id='transform_gold',
    default_args=default_args,
    description='Transformação SILVER → GOLD (métricas de negócio)',
    schedule_interval='@daily',
    catchup=False,
    tags=['transformation', 'gold', 'metricas', 'kpi'],
) as dag:
    
    # ==========================================
    # SENSOR: Aguarda SILVER
    # ==========================================
    
    aguardar_silver = ExternalTaskSensor(
        task_id='aguardar_silver',
        external_dag_id='transform_silver',
        external_task_id='validar_silver',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=7200,
    )
    
    # ==========================================
    # TRANSFORMAÇÃO SILVER → GOLD
    # ==========================================
    
    gerar_metricas = PythonOperator(
        task_id='gerar_metricas',
        python_callable=gerar_metricas_gold,
    )
    
    # ==========================================
    # VALIDAÇÃO
    # ==========================================
    
    validacao = PythonOperator(
        task_id='validar_gold',
        python_callable=validar_camada_gold,
    )
    
    # ==========================================
    # RELATÓRIO FINAL
    # ==========================================
    
    relatorio = PythonOperator(
        task_id='relatorio_final',
        python_callable=relatorio_pipeline_completo,
    )
    
    # ==========================================
    # FLUXO
    # ==========================================
    
    aguardar_silver >> gerar_metricas >> validacao >> relatorio
