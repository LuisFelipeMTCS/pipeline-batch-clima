"""
DAG Pipeline Solos (INMET)
===========================

Ingestão de dados de solos do INMET:
- Download de dados de solos
- Upload para S3 (camada RAW)
- Validação de qualidade
- Monitoramento e alertas

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

from src.ingestion.pipelines.solos import (
    ingest_data,
    validar_ingestao,
    enviar_metricas
)
from src.common.observability import ObservabilityManager


# =========================
# CONFIGURAÇÕES
# =========================

observability = ObservabilityManager(
    pipeline_name="pipeline_solos",
    namespace=os.getenv("METRIC_NAMESPACE_INGESTAO", "pipeline_ingestao"),
    log_group=os.getenv("LOG_GROUP_NAME_INGESTAO", "/ingestao/solos"),
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

def ingerir_solos(**context):
    """Executa ingestão e salva stats no XCom"""
    p = context["params"]
    resultado = ingest_data(
        year_filter=p.get("year_filter"),
        max_workers=p.get("max_workers", 10),
        send_success_alert=False,  # Desabilita alerta automático do engine
        send_error_alert=False,    # Controlamos manualmente na DAG
    )

    stats = {
        'fonte': 'solos',
        'arquivos_baixados': resultado.get('arquivos_baixados', 0),
        'arquivos_enviados_s3': resultado.get('arquivos_enviados_s3', 0),
        'camada': 'raw',
        'resultado_completo': resultado  # Guarda resultado completo para usar no alerta
    }

    context['ti'].xcom_push(key='stats_ingestao_solos', value=stats)
    return stats


def validar_ingestao_solos(**context):
    """Valida ingestão e salva resultado no XCom"""
    ti = context['ti']
    stats = ti.xcom_pull(key='stats_ingestao_solos', task_ids='ingest_solos')

    result = validar_ingestao(stats)
    context['ti'].xcom_push(key='validacao_resultado', value=result)
    return result


def enviar_metricas_cloudwatch(**context):
    """Envia métricas para CloudWatch"""
    ti = context['ti']
    resultado = ti.xcom_pull(key='validacao_resultado', task_ids='validar_ingestao')
    enviar_metricas(resultado, observability)


def enviar_alerta_ingestao(**context):
    """Envia alerta personalizado de sucesso/erro da ingestão"""
    from datetime import datetime, timezone

    ti = context['ti']
    stats = ti.xcom_pull(key='stats_ingestao_solos', task_ids='ingest_solos')

    if not stats:
        return

    resultado = stats.get('resultado_completo', {})

    # Formatar data/hora
    data_hora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Montar detalhes do alerta
    arquivos_sucesso = resultado.get('arquivos_baixados', 0)
    arquivos_erro = resultado.get('arquivos_erro', 0)
    tempo_segundos = resultado.get('tempo_total_segundos', 0)
    total_linhas = resultado.get('total_linhas', 0)
    total_bytes = resultado.get('total_bytes', 0)

    # Calcular taxa
    taxa = arquivos_sucesso / tempo_segundos if tempo_segundos > 0 else 0

    # Formatar bytes
    bytes_formatados = f"{total_bytes:,}" if total_bytes > 0 else "0"

    detalhes = {
        "📅 Data/Hora": data_hora,
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━": "",
        "📊 RESUMO": "",
        "• Arquivos": f"{arquivos_sucesso} sucesso | {arquivos_erro} erros",
        "• Tempo": f"{tempo_segundos:.1f}s ({taxa:.2f} arq/s)",
        "• Linhas": f"{total_linhas:,}",
        "• Bytes": bytes_formatados,
    }

    if arquivos_erro == 0:
        titulo = "✅ INGESTÃO CONCLUÍDA COM SUCESSO"
        observability.enviar_alerta_sucesso(titulo=titulo, detalhes=detalhes)
    else:
        titulo = f"❌ INGESTÃO COM {arquivos_erro} ERRO(S)"
        observability.enviar_alerta_erro(
            titulo=titulo,
            erro=f"{arquivos_erro} arquivo(s) falharam",
            contexto=detalhes
        )


# =========================
# DAG DEFINITION
# =========================

with DAG(
    dag_id="pipeline_solos",
    default_args=default_args,
    description='Ingestão de dados de solos (INMET) para camada RAW',
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestao", "solos", "inmet", "raw"],
    params={"year_filter": None, "max_workers": 10},
) as dag:

    ingestao = PythonOperator(
        task_id="ingest_solos",
        python_callable=ingerir_solos,
        on_failure_callback=observability.criar_callback_erro(),
    )

    validacao = PythonOperator(
        task_id='validar_ingestao',
        python_callable=validar_ingestao_solos,
        on_failure_callback=observability.criar_callback_erro(),
    )

    metricas = PythonOperator(
        task_id='enviar_metricas_cloudwatch',
        python_callable=enviar_metricas_cloudwatch,
        trigger_rule='all_success',
    )

    alerta = PythonOperator(
        task_id='enviar_alerta_ingestao',
        python_callable=enviar_alerta_ingestao,
        trigger_rule='all_done',  # Executa sempre (sucesso ou erro)
    )

    # Fluxo
    ingestao >> validacao >> [metricas, alerta]
