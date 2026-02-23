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
from datetime import datetime, timedelta, timezone
import logging
import os

# Observabilidade centralizada
from src.common.observability import ObservabilityManager


# =========================
# CONFIGURAÇÕES
# =========================

# Criar gerenciador de observabilidade
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
    
    result = {
        'clima': stats_clima,
        'culturas': stats_culturas,
        'total_registros': total_registros,
        'camada': 'bronze',
        'status': 'success',
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Salva para próxima task (resumo)
    context['ti'].xcom_push(key='validacao_resultado', value=result)
    
    return result


def enviar_metricas_cloudwatch(**context):
    """Envia métricas para CloudWatch após validação bem-sucedida"""
    logger = logging.getLogger(__name__)
    
    # Busca resultado da validação
    ti = context['ti']
    resultado = ti.xcom_pull(key='validacao_resultado', task_ids='validar_bronze')
    
    if not resultado:
        logger.warning("⚠️ Nenhum resultado encontrado para enviar métricas")
        return
    
    # Enviar métricas usando ObservabilityManager
    observability.enviar_metricas(
        metricas={
            'RegistrosProcessados': resultado['total_registros'],
            'RegistrosClima': resultado['clima']['registros'],
            'RegistrosCulturas': resultado['culturas']['registros'],
            'TransformacaoSucesso': 1
        },
        dimensoes={'Camada': 'Bronze'}
    )
    
    logger.info(f"✅ {resultado['total_registros']:,} registros processados")


def enviar_resumo_final(**context):
    """
    Envia resumo final SEMPRE (sucesso ou erro).
    Usa trigger_rule='all_done' para executar mesmo se tarefas anteriores falharem.
    """
    logger = logging.getLogger(__name__)
    ti = context['ti']
    dag_run = context['dag_run']
    execution_date = context['execution_date']
    
    # Calcular tempo de execução total
    start_time = dag_run.start_date
    end_time = datetime.now(timezone.utc)
    
    # Garantir que ambos sejam timezone-aware para evitar TypeError
    if start_time:
        if start_time.tzinfo is None:
            # Se start_time é naive, torná-lo aware (assume UTC)
            start_time = start_time.replace(tzinfo=timezone.utc)
        duracao_segundos = (end_time - start_time).total_seconds()
        duracao_minutos = duracao_segundos / 60
    else:
        duracao_segundos = 0
        duracao_minutos = 0
    
    # Verifica estado de cada task crítica
    task_states = {}
    for task_id in ['bronze_clima', 'bronze_culturas', 'validar_bronze']:
        task_instance = dag_run.get_task_instance(task_id)
        task_states[task_id] = task_instance.state if task_instance else 'unknown'
    
    # Determina status geral
    all_success = all(state == 'success' for state in task_states.values())
    any_failed = any(state == 'failed' for state in task_states.values())
    
    if all_success:
        # Busca resultado da validação
        resultado = ti.xcom_pull(key='validacao_resultado', task_ids='validar_bronze')

        if resultado:
            # Buscar tempo individual de cada task
            task_durations = {}
            for task_id in ['bronze_clima', 'bronze_culturas', 'validar_bronze']:
                task_instance = dag_run.get_task_instance(task_id)
                if task_instance and task_instance.start_date and task_instance.end_date:
                    task_start = task_instance.start_date
                    task_end = task_instance.end_date
                    # Garantir timezone-aware
                    if task_start.tzinfo is None:
                        task_start = task_start.replace(tzinfo=timezone.utc)
                    if task_end.tzinfo is None:
                        task_end = task_end.replace(tzinfo=timezone.utc)
                    duration = (task_end - task_start).total_seconds() / 60
                    task_durations[task_id] = duration
                else:
                    task_durations[task_id] = 0

            # Informações específicas: tempo, arquivos, status de cada operação
            total_arquivos = resultado['clima']['registros'] + resultado['culturas']['registros']

            detalhes = {
                "⏱️ Tempo Total de Execução": f"{duracao_minutos:.2f} minutos",
                "📦 Total de Arquivos Processados": f"{total_arquivos:,}",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━": "",
                "🌤️ Bronze Clima": f"✅ {resultado['clima']['registros']:,} arquivos processados",
                "  ⏱️ Tempo Clima": f"{task_durations.get('bronze_clima', 0):.2f} minutos",
                "  📊 Taxa Clima": f"{resultado['clima']['registros'] / max(task_durations.get('bronze_clima', 1), 0.01):.0f} arquivos/min",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ ": "",
                "🌾 Bronze Culturas": f"✅ {resultado['culturas']['registros']:,} arquivos processados",
                "  ⏱️ Tempo Culturas": f"{task_durations.get('bronze_culturas', 0):.2f} minutos",
                "  📊 Taxa Culturas": f"{resultado['culturas']['registros'] / max(task_durations.get('bronze_culturas', 1), 0.01):.0f} arquivos/min",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ": "",
                "🔍 Validação Bronze": f"✅ Concluída",
                "  ⏱️ Tempo Validação": f"{task_durations.get('validar_bronze', 0):.2f} minutos",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   ": "",
                "✅ Status Final": "Sucesso - todas operações concluídas"
            }

            titulo = f"Bronze OK - {total_arquivos:,} arquivos em {duracao_minutos:.1f}min"
        else:
            # Fallback se não encontrou resultado
            detalhes = {
                "⏱️ Tempo de execução": f"{duracao_minutos:.2f} minutos",
                "🌤️ Bronze Clima": "✅ Concluído",
                "🌾 Bronze Culturas": "✅ Concluído",
                "✅ Status Final": "Sucesso"
            }
            titulo = f"Bronze OK - {duracao_minutos:.1f}min"

        logger.info(f"📧 Enviando alerta de sucesso: {titulo}")

        # Enviar para CloudWatch Logs
        observability.enviar_log(
            mensagem=f"✅ Transformação Bronze concluída com sucesso",
            nivel="INFO",
            dados={
                "pipeline": "transform_bronze",
                "status": "success",
                **detalhes
            }
        )

        # Enviar alerta SNS
        observability.enviar_alerta_sucesso(titulo=titulo, detalhes=detalhes)
    
    elif any_failed:
        # Pipeline falhou - conta sucessos e falhas
        sucessos = sum(1 for state in task_states.values() if state == 'success')
        falhas = sum(1 for state in task_states.values() if state == 'failed')
        
        detalhes = {
            "⏱️ Tempo até falha": f"{duracao_minutos:.2f} minutos",
            "✅ Operações bem-sucedidas": f"{sucessos}",
            "❌ Operações com falha": f"{falhas}",
            "🌤️ Bronze Clima": f"{'✅ OK' if task_states.get('bronze_clima') == 'success' else '❌ FALHA'}",
            "🌾 Bronze Culturas": f"{'✅ OK' if task_states.get('bronze_culturas') == 'success' else '❌ FALHA'}",
            "🔍 Validação": f"{'✅ OK' if task_states.get('validar_bronze') == 'success' else '❌ FALHA'}"
        }
        
        titulo = f"Bronze ERRO - {falhas} operação(ões) falhou(aram)"
        
        logger.info(f"📧 Enviando alerta de erro: {titulo}")

        # Enviar para CloudWatch Logs
        observability.enviar_log(
            mensagem=f"❌ Transformação Bronze falhou - {falhas} operação(ões) com erro",
            nivel="ERROR",
            dados={
                "pipeline": "transform_bronze",
                "status": "failed",
                "falhas": falhas,
                "sucessos": sucessos,
                **detalhes
            }
        )

        # Enviar alerta SNS
        observability.enviar_alerta_erro(
            titulo="Transformação Bronze Falhou",
            erro=f"{falhas} de 3 operações falharam",
            contexto=detalhes
        )
    
    else:
        # Estado parcial/desconhecido
        logger.warning(f"⚠️ Estado parcial do pipeline: {task_states}")
        detalhes = {
            "⏱️ Tempo": f"{duracao_minutos:.2f} minutos",
            "⚠️ Status": "Parcial - algumas tasks não completaram",
            "🌤️ Bronze Clima": task_states.get('bronze_clima', 'unknown'),
            "🌾 Bronze Culturas": task_states.get('bronze_culturas', 'unknown'),
            "🔍 Validação": task_states.get('validar_bronze', 'unknown')
        }

        # Enviar para CloudWatch Logs
        observability.enviar_log(
            mensagem="⚠️ Pipeline em estado parcial",
            nivel="WARNING",
            dados={
                "pipeline": "transform_bronze",
                "status": "partial",
                **detalhes
            }
        )
    
    logger.info("✅ Resumo final enviado com sucesso")


# Callback de erro usando ObservabilityManager
# (atribuído diretamente ao criar as tasks)
enviar_alerta_erro = observability.criar_callback_erro()


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
        on_failure_callback=enviar_alerta_erro,  # ← Alerta se falhar
    )
    
    bronze_culturas = PythonOperator(
        task_id='bronze_culturas',
        python_callable=transformar_raw_to_bronze_culturas,
        on_failure_callback=enviar_alerta_erro,  # ← Alerta se falhar
    )
    
    # ==========================================
    # VALIDAÇÃO
    # ==========================================
    
    validacao = PythonOperator(
        task_id='validar_bronze',
        python_callable=validar_camada_bronze,
        on_failure_callback=enviar_alerta_erro,  # ← Alerta se validação falhar
    )
    
    # ==========================================
    # MÉTRICAS E ALERTAS
    # ==========================================
    
    enviar_metricas = PythonOperator(
        task_id='enviar_metricas_cloudwatch',
        python_callable=enviar_metricas_cloudwatch,
        trigger_rule='all_success',  # ← Só envia métricas se TUDO deu certo
    )
    
    enviar_resumo = PythonOperator(
        task_id='enviar_resumo_final',
        python_callable=enviar_resumo_final,
        trigger_rule='all_done',  # ← SEMPRE executa (sucesso ou erro)
    )
    
    # ==========================================
    # FLUXO
    # ==========================================
    
    aguardar_clima >> bronze_clima
    [aguardar_agricultura, aguardar_solos] >> bronze_culturas
    
    [bronze_clima, bronze_culturas] >> validacao
    validacao >> [enviar_metricas, enviar_resumo]  # ← Paralelo após validação
