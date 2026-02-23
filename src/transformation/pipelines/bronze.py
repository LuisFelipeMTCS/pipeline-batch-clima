"""
Transformação RAW → BRONZE: Workflow Functions
================================================

Funções de orquestração para transformação da camada Bronze:
- Transformação de dados RAW para BRONZE
- Validação de qualidade
- Envio de métricas
- Geração de resumos
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional


def transformar_raw_to_bronze_clima(spark_session_creator, processar_clima_bronze_func):
    """
    Factory function que retorna a função de transformação RAW→BRONZE para Clima.

    Args:
        spark_session_creator: Função que cria spark session
        processar_clima_bronze_func: Função que processa clima bronze

    Returns:
        Função de transformação configurada
    """
    def _transform(**context):
        """RAW → BRONZE para CLIMA"""
        logger = logging.getLogger(__name__)

        try:
            logger.info("🌤️  Transformando RAW → BRONZE (Clima)...")

            spark = spark_session_creator("bronze-clima")

            # Paths
            raw_path = "s3a://datalake-lab1/raw/clima/year=*/"
            bronze_path = "s3a://datalake-lab1/bronze/clima/"

            # Processa
            df = processar_clima_bronze_func(spark, raw_path, bronze_path)

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

    return _transform


def transformar_raw_to_bronze_culturas(spark_session_creator, processar_culturas_bronze_func):
    """
    Factory function que retorna a função de transformação RAW→BRONZE para Culturas.

    Args:
        spark_session_creator: Função que cria spark session
        processar_culturas_bronze_func: Função que processa culturas bronze

    Returns:
        Função de transformação configurada
    """
    def _transform(**context):
        """RAW → BRONZE para CULTURAS (Agricultura + Solos)"""
        logger = logging.getLogger(__name__)

        try:
            logger.info("🌾 Transformando RAW → BRONZE (Culturas)...")

            spark = spark_session_creator("bronze-culturas")

            # Agrupa agricultura e solos (ambos são culturas)
            raw_agricultura = "s3a://datalake-lab1/raw/agricultura/year=*/"
            raw_solos = "s3a://datalake-lab1/raw/solos/year=*/"
            bronze_path = "s3a://datalake-lab1/bronze/culturas/"

            # Passar ambos os paths como tupla
            df = processar_culturas_bronze_func(spark, (raw_agricultura, raw_solos), bronze_path)

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

    return _transform


def validar_camada_bronze(stats_clima: Dict[str, Any], stats_culturas: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida que todas as transformações BRONZE foram OK.

    Args:
        stats_clima: Estatísticas da transformação de clima
        stats_culturas: Estatísticas da transformação de culturas

    Returns:
        Resultado da validação com todas as estatísticas

    Raises:
        Exception: Se alguma transformação falhou
    """
    logger = logging.getLogger(__name__)

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

    return result


def enviar_metricas(resultado: Dict[str, Any], observability) -> None:
    """
    Envia métricas para CloudWatch após validação bem-sucedida.

    Args:
        resultado: Resultado da validação com estatísticas
        observability: Gerenciador de observabilidade
    """
    logger = logging.getLogger(__name__)

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


def gerar_resumo_final(
    ti,
    dag_run,
    task_ids: list,
    observability,
    pipeline_name: str = "transform_bronze"
):
    """
    Gera e envia resumo final do pipeline (sucesso ou erro).
    Executa SEMPRE, mesmo se tarefas anteriores falharem.

    Args:
        ti: TaskInstance do Airflow
        dag_run: DagRun do Airflow
        task_ids: Lista de task_ids para validar status
        observability: Gerenciador de observabilidade
        pipeline_name: Nome do pipeline
    """
    logger = logging.getLogger(__name__)

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
    for task_id in task_ids:
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
            for task_id in task_ids:
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
                "pipeline": pipeline_name,
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
                "pipeline": pipeline_name,
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
                "pipeline": pipeline_name,
                "status": "partial",
                **detalhes
            }
        )

    logger.info("✅ Resumo final enviado com sucesso")
