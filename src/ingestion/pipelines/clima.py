"""
Pipeline de Ingestão - CLIMA (INMET)
====================================

Fonte: Google Drive (CSVs do INMET)
Destino: S3 raw (Parquet)

Este arquivo contém APENAS a lógica específica do INMET:
- Conexão com Google Drive
- Listagem de pastas por ano
- Download de CSVs
- Conversão CSV → Parquet
- Upload para S3

A lógica genérica (logs, métricas, alertas, paralelismo) está em engine.py
"""

import os
import io
import ssl
import socket
import time
import tempfile
import pandas as pd
import boto3
from pathlib import Path
from threading import local
from typing import List, Dict, Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

from src.ingestion.engine import (
    DataSource,
    IngestionEngine,
    IngestionConfig,
    PROJECT_ROOT,
    AWS_REGION,
)
from src.validation.pipelines.clima import validate_dataframe

# =========================
# CONFIG ESPECÍFICA CLIMA
# =========================

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID_CLIMA")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-lab1")

# CloudWatch/SNS (específico deste pipeline)
LOG_GROUP_NAME = os.getenv("LOG_GROUP_NAME_CLIMA", "/inmet/ingestao/raw")
METRIC_NAMESPACE_CLIMA = os.getenv("METRIC_NAMESPACE_CLIMA", "inmet_raw")
SNS_TOPIC_ARN = os.getenv("SNS_ALERT_TOPIC_ARN", "")

# Performance
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

# Credenciais Google
CREDENTIALS_PATH = Path(os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    str(PROJECT_ROOT / "keys" / "credentials.json")
))

# Diretório temporário
if os.environ.get("AIRFLOW_HOME"):
    TEMP_DIR = "/opt/airflow/tmp_ingest"
    os.makedirs(TEMP_DIR, exist_ok=True)
else:
    TEMP_DIR = tempfile.gettempdir()


# =========================
# SSL FIX
# =========================

def _fix_ssl():
    try:
        import certifi
        os.environ['SSL_CERT_FILE'] = certifi.where()
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    except ImportError:
        pass
    socket.setdefaulttimeout(60)

_fix_ssl()


# =========================
# THREAD-LOCAL RESOURCES
# =========================

_credentials = None
_thread_local = local()


def _get_credentials():
    global _credentials
    if _credentials is None:
        if not CREDENTIALS_PATH.exists():
            raise FileNotFoundError(f"Credenciais não encontradas: {CREDENTIALS_PATH}")
        _credentials = service_account.Credentials.from_service_account_file(
            str(CREDENTIALS_PATH), scopes=SCOPES
        )
    return _credentials


def _get_drive_service():
    if not hasattr(_thread_local, 'drive_service'):
        _thread_local.drive_service = build(
            "drive", "v3", credentials=_get_credentials(), cache_discovery=False
        )
    return _thread_local.drive_service


def _get_s3_client():
    if not hasattr(_thread_local, 's3_client'):
        _thread_local.s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _thread_local.s3_client


def _reset_thread_local():
    for attr in ['drive_service', 's3_client']:
        if hasattr(_thread_local, attr):
            delattr(_thread_local, attr)


# =========================
# GOOGLE DRIVE SOURCE
# =========================

class GoogleDriveSource(DataSource):
    """
    Fonte de dados: Google Drive com CSVs do INMET.
    
    Estrutura esperada no Drive:
        FOLDER_ID/
        ├── 2020/
        │   ├── estacao1.csv
        │   └── estacao2.csv
        ├── 2021/
        │   └── ...
        └── 2022/
            └── ...
    """
    
    def __init__(self, year_filter: Optional[str] = None):
        """
        Args:
            year_filter: Processa apenas este ano (ex: "2023")
        """
        self.year_filter = year_filter
        self.folder_id = FOLDER_ID
    
    def connect(self) -> None:
        """Valida credenciais e conexão."""
        if not self.folder_id:
            raise ValueError("FOLDER_ID não configurado no .env")
        _get_credentials()
        _get_drive_service()
    
    def list_files(self) -> List[Dict[str, Any]]:
        """Lista todos os CSVs organizados por ano."""
        service = _get_drive_service()
        
        # 1. Lista pastas de ano
        year_folders = self._list_year_folders(service)
        
        # 2. Aplica filtro se houver
        if self.year_filter:
            year_folders = [f for f in year_folders if f["name"] == self.year_filter]
        
        # 3. Lista CSVs em cada pasta
        all_files = []
        for folder in year_folders:
            year = folder["name"]
            csvs = self._list_csv_files(service, folder["id"])
            
            for csv_file in csvs:
                all_files.append({
                    "id": csv_file["id"],
                    "name": csv_file["name"],
                    "metadata": {
                        "year": year,
                        "size": int(csv_file.get("size", 0)),
                        "folder_id": folder["id"],
                    }
                })
        
        return all_files
    
    def _list_year_folders(self, service) -> List[Dict]:
        """Lista pastas de ano no Drive."""
        query = f"'{self.folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
        folders = []
        page_token = None
        
        while True:
            response = service.files().list(
                q=query, pageSize=1000,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token
            ).execute()
            folders.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        
        return sorted(folders, key=lambda x: x["name"])
    
    def _list_csv_files(self, service, folder_id: str) -> List[Dict]:
        """Lista CSVs em uma pasta."""
        query = f"'{folder_id}' in parents and name contains '.csv'"
        files = []
        page_token = None
        
        while True:
            response = service.files().list(
                q=query, pageSize=1000,
                fields="nextPageToken, files(id, name, size)",
                pageToken=page_token
            ).execute()
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        
        return files
    
    def download_file(self, file_info: Dict) -> str:
        """Baixa CSV do Drive com retry."""
        file_id = file_info["id"]
        filename = file_info["name"]
        
        safe_name = f"{file_id}_{filename.replace(' ', '_')}"
        filepath = os.path.join(TEMP_DIR, safe_name)
        
        max_retries = 5
        last_error = None
        
        for attempt in range(max_retries):
            try:
                service = _get_drive_service()
                request = service.files().get_media(fileId=file_id)
                
                with io.FileIO(filepath, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                
                return filepath
                
            except HttpError as e:
                last_error = e
                if e.resp.status in [429, 500, 502, 503]:
                    wait = min(2 ** (attempt + 2), 60)
                    time.sleep(wait)
                    _reset_thread_local()
                else:
                    raise
                    
            except (ssl.SSLError, socket.timeout, ConnectionError) as e:
                last_error = e
                wait = 2 ** (attempt + 1)
                time.sleep(wait)
                _reset_thread_local()
                
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        
        raise last_error
    
    def process_file(self, local_path: str, file_info: Dict) -> Dict:
        """Converte CSV → Parquet e faz upload para S3."""
        result = {
            "success": False,
            "rows": 0,
            "bytes": file_info["metadata"].get("size", 0),
            "time_transform": 0,
            "time_upload": 0,
            "time_validation": 0,
            "error": None,
            "validation_warnings": 0,
        }
        
        parquet_path = None
        
        try:
            year = file_info["metadata"]["year"]
            filename = file_info["name"]
            
            # 1. Transform: CSV → DataFrame
            start = time.perf_counter()
            
            df = pd.read_csv(
                local_path,
                sep=";",
                encoding="latin1",
                skiprows=8,
                on_bad_lines="skip"
            )
            result["rows"] = len(df)
            
            # 2. VALIDAÇÃO
            start_validation = time.perf_counter()
            validation_result = validate_dataframe(df, fail_on_error=False)
            result["time_validation"] = time.perf_counter() - start_validation
            result["validation_warnings"] = validation_result.warning_count
            
            if not validation_result.is_valid:
                # Pega primeiro erro para mensagem
                first_error = validation_result.errors[0] if validation_result.errors else None
                error_msg = first_error.message if first_error else "Validação falhou"
                result["error"] = f"Validação: {error_msg}"
                return result
            
            # 3. DataFrame → Parquet (forçar todos os tipos como STRING para consistência)
            base_name = os.path.splitext(filename)[0]
            parquet_name = f"{base_name}.parquet"
            parquet_path = os.path.join(TEMP_DIR, f"{file_info['id']}_{parquet_name}")
            
            # Converter todas as colunas para string (evita incompatibilidade de schema no Spark)
            df = df.astype(str)
            
            df.to_parquet(parquet_path, engine="pyarrow", index=False)
            result["time_transform"] = time.perf_counter() - start
            
            # 4. Upload: S3
            start = time.perf_counter()
            s3 = _get_s3_client()
            s3_key = f"raw/clima/year={year}/{parquet_name}"
            s3.upload_file(parquet_path, BUCKET_NAME, s3_key)
            result["time_upload"] = time.perf_counter() - start
            
            result["success"] = True
            
        except Exception as e:
            result["error"] = str(e)
            
        finally:
            # Cleanup parquet temp
            if parquet_path and os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except:
                    pass
        
        return result


# =========================
# FUNÇÃO PRINCIPAL
# =========================

def ingest_data(
    max_workers: int = MAX_WORKERS,
    year_filter: Optional[str] = None,
    send_metrics: bool = True,
    send_success_alert: bool = True,
    send_error_alert: bool = True,
) -> Dict[str, Any]:
    """
    Executa ingestão de dados climáticos do INMET.

    Args:
        max_workers: Threads paralelas para download
        year_filter: Processa apenas este ano (ex: "2023")
        send_metrics: Envia métricas para CloudWatch
        send_success_alert: Envia email de sucesso
        send_error_alert: Envia email de erro

    Returns:
        Dicionário com estatísticas da ingestão:
            - arquivos_baixados: Número de arquivos baixados
            - arquivos_enviados_s3: Número de arquivos enviados para S3
            - arquivos_erro: Número de erros
            - total_linhas: Total de linhas processadas
            - resumo: String resumida (ex: "5 sucesso | 0 erros | 10.2s")

    Exemplo:
        # Processa todos os anos
        stats = ingest_data()

        # Processa só 2023
        stats = ingest_data(year_filter="2023")
        
        # Mais rápido, 20 threads
        ingest_data(max_workers=20)
    """
    
    # Configura a engine
    config = IngestionConfig(
        pipeline_name="INMET Clima",
        log_group=LOG_GROUP_NAME,
        metric_namespace=METRIC_NAMESPACE_CLIMA, 
        sns_topic_arn=SNS_TOPIC_ARN,
        max_workers=max_workers,
        send_metrics=send_metrics,
        send_success_alert=send_success_alert,
        send_error_alert=send_error_alert,
    )

    # Cria source e engine
    source = GoogleDriveSource(year_filter=year_filter)
    engine = IngestionEngine(config)
    
    # Executa
    return engine.run(source)


# =========================
# FUNÇÕES DE WORKFLOW (DAG)
# =========================

def validar_ingestao(stats: Dict[str, int]) -> Dict[str, Any]:
    """
    Valida que a ingestão foi bem-sucedida.

    Args:
        stats: Estatísticas da ingestão (retornadas por ingerir_clima)

    Returns:
        Resultado da validação com timestamp

    Raises:
        Exception: Se nenhum arquivo foi enviado ao S3
    """
    from datetime import datetime, timezone
    import logging

    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("✅ VALIDAÇÃO INGESTÃO CLIMA")
    logger.info("=" * 70)
    logger.info(f"🌤️  Clima: {stats}")
    logger.info("=" * 70)

    if not stats or stats.get('arquivos_enviados_s3', 0) == 0:
        raise Exception("❌ Nenhum arquivo foi enviado ao S3!")

    logger.info(f"✅ Ingestão validada: {stats['arquivos_enviados_s3']:,} arquivos no S3")

    result = {
        'clima': stats,
        'total_arquivos': stats.get('arquivos_enviados_s3', 0),
        'camada': 'raw',
        'status': 'success',
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

    return result


def enviar_metricas(resultado: Dict[str, Any], observability) -> None:
    """
    Envia métricas para CloudWatch após validação bem-sucedida.

    Args:
        resultado: Resultado da validação
        observability: Instância do ObservabilityManager
    """
    import logging

    logger = logging.getLogger(__name__)

    if not resultado:
        logger.warning("⚠️ Nenhum resultado encontrado para enviar métricas")
        return

    # Enviar métricas usando ObservabilityManager
    observability.enviar_metricas(
        metricas={
            'ArquivosIngeridos': resultado['total_arquivos'],
            'IngestaoSucesso': 1
        },
        dimensoes={'Camada': 'Raw', 'Fonte': 'Clima'}
    )

    logger.info(f"✅ {resultado['total_arquivos']:,} arquivos ingeridos")


def gerar_resumo_final(
    ti,
    dag_run,
    task_ids: List[str],
    observability,
    pipeline_name: str,
    fonte_emoji: str = "🌤️",
    fonte_nome: str = "Clima"
) -> None:
    """
    Gera e envia resumo final do pipeline (sucesso ou erro).

    Args:
        ti: TaskInstance do Airflow
        dag_run: DagRun do Airflow
        task_ids: Lista de task_ids para monitorar ['ingest_clima', 'validar_ingestao']
        observability: Instância do ObservabilityManager
        pipeline_name: Nome do pipeline (ex: "pipeline_clima")
        fonte_emoji: Emoji da fonte (ex: "🌤️")
        fonte_nome: Nome da fonte (ex: "Clima")
    """
    from datetime import datetime, timezone
    import logging

    logger = logging.getLogger(__name__)

    # Calcular tempo de execução total
    start_time = dag_run.start_date
    end_time = datetime.now(timezone.utc)

    # Garantir que ambos sejam timezone-aware para evitar TypeError
    if start_time:
        if start_time.tzinfo is None:
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
        resultado = ti.xcom_pull(key='validacao_resultado', task_ids=task_ids[1])  # segundo task_id é validação

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

            # Informações específicas
            total_arquivos = resultado['total_arquivos']

            detalhes = {
                "⏱️ Tempo Total de Execução": f"{duracao_minutos:.2f} minutos",
                "📦 Total de Arquivos Ingeridos": f"{total_arquivos:,}",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━": "",
                f"{fonte_emoji} Ingestão {fonte_nome}": f"✅ {total_arquivos:,} arquivos baixados e enviados ao S3",
                f"  ⏱️ Tempo Ingestão": f"{task_durations.get(task_ids[0], 0):.2f} minutos",
                f"  📊 Taxa de Ingestão": f"{total_arquivos / max(task_durations.get(task_ids[0], 1), 0.01):.0f} arquivos/min",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ ": "",
                "🔍 Validação": f"✅ Concluída",
                "  ⏱️ Tempo Validação": f"{task_durations.get(task_ids[1], 0):.2f} minutos",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ": "",
                "✅ Status Final": "Sucesso - ingestão concluída"
            }

            titulo = f"{fonte_nome} OK - {total_arquivos:,} arquivos em {duracao_minutos:.1f}min"
        else:
            # Fallback se não encontrou resultado
            detalhes = {
                "⏱️ Tempo de execução": f"{duracao_minutos:.2f} minutos",
                f"{fonte_emoji} Ingestão {fonte_nome}": "✅ Concluído",
                "✅ Status Final": "Sucesso"
            }
            titulo = f"{fonte_nome} OK - {duracao_minutos:.1f}min"

        logger.info(f"📧 Enviando alerta de sucesso: {titulo}")

        # Enviar para CloudWatch Logs
        observability.enviar_log(
            mensagem=f"✅ Ingestão {fonte_nome} concluída com sucesso",
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
        # Pipeline falhou
        sucessos = sum(1 for state in task_states.values() if state == 'success')
        falhas = sum(1 for state in task_states.values() if state == 'failed')

        # Buscar tempo individual de cada task e detalhes
        task_durations = {}
        task_details = {}

        for task_id in task_ids:
            task_instance = dag_run.get_task_instance(task_id)
            state = task_states.get(task_id, 'unknown')

            # Calcular duração se a task executou
            if task_instance and task_instance.start_date:
                task_start = task_instance.start_date
                task_end = task_instance.end_date or datetime.now(timezone.utc)

                # Garantir timezone-aware
                if task_start.tzinfo is None:
                    task_start = task_start.replace(tzinfo=timezone.utc)
                if task_end.tzinfo is None:
                    task_end = task_end.replace(tzinfo=timezone.utc)

                duration = (task_end - task_start).total_seconds() / 60
                task_durations[task_id] = duration
            else:
                task_durations[task_id] = 0

            # Status detalhado
            if state == 'success':
                status_icon = "✅"
            elif state == 'failed':
                status_icon = "❌"
            else:
                status_icon = "⚠️"

            task_details[task_id] = {
                'status': state,
                'icon': status_icon,
                'duration': task_durations[task_id]
            }

        # Buscar estatísticas (se disponível)
        stats = ti.xcom_pull(key=f'stats_ingestao_{fonte_nome.lower()}', task_ids=task_ids[0])

        detalhes = {
            "⏱️ Tempo Total até Falha": f"{duracao_minutos:.2f} minutos",
            "✅ Operações Bem-sucedidas": f"{sucessos} de {len(task_ids)}",
            "❌ Operações com Falha": f"{falhas} de {len(task_ids)}",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━": "",
            f"{fonte_emoji} Ingestão {fonte_nome}": f"{task_details[task_ids[0]]['icon']} {task_details[task_ids[0]]['status'].upper()}",
            "  ⏱️ Tempo Ingestão": f"{task_details[task_ids[0]]['duration']:.2f} minutos",
        }

        if stats and task_details[task_ids[0]]['status'] == 'success':
            detalhes["  📦 Arquivos Ingeridos"] = f"{stats.get('arquivos_enviados_s3', 0):,}"

        detalhes.update({
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ ": "",
            "🔍 Validação": f"{task_details[task_ids[1]]['icon']} {task_details[task_ids[1]]['status'].upper()}",
            "  ⏱️ Tempo Validação": f"{task_details[task_ids[1]]['duration']:.2f} minutos",
        })

        titulo = f"{fonte_nome} ERRO - {falhas} operação(ões) falhou(aram)"

        logger.info(f"📧 Enviando alerta de erro: {titulo}")

        # Enviar para CloudWatch Logs
        observability.enviar_log(
            mensagem=f"❌ Ingestão {fonte_nome} falhou - {falhas} operação(ões) com erro",
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
            titulo=f"Ingestão {fonte_nome} Falhou",
            erro=f"{falhas} de {len(task_ids)} operações falharam",
            contexto=detalhes
        )

    else:
        # Estado parcial/desconhecido
        logger.warning(f"⚠️ Estado parcial do pipeline: {task_states}")
        detalhes = {
            "⏱️ Tempo": f"{duracao_minutos:.2f} minutos",
            "⚠️ Status": "Parcial - algumas tasks não completaram",
        }
        for task_id in task_ids:
            detalhes[task_id] = task_states.get(task_id, 'unknown')

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


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    import sys

    workers = int(sys.argv[1]) if len(sys.argv) > 1 else MAX_WORKERS
    year = sys.argv[2] if len(sys.argv) > 2 else None

    print(ingest_data(max_workers=workers, year_filter=year))
