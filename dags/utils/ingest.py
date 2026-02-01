"""
Módulo de ingestão de dados INMET - Google Drive → S3 (Bronze)
Otimizado para performance com processamento paralelo e tratamento de SSL.

Uso:
    from dags.utils.ingest import ingest_data
    resultado = ingest_data()
"""

import os
import io
import pandas as pd
import logging
import tempfile
import boto3
import time
import ssl
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from contextlib import contextmanager
from threading import local
from pathlib import Path

from dotenv import load_dotenv
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
import httplib2

# =========================
# CONFIG
# =========================

# Carrega .env da raiz do projeto
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-lab1")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

METRIC_NAMESPACE = "inmet_bronze"

# Performance configs
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

# Path para credentials
CREDENTIALS_PATH = PROJECT_ROOT / "keys" / "credentials.json"

# =========================
# SSL FIX PARA WINDOWS
# =========================

def fix_ssl_for_windows():
    """
    Corrige problemas de SSL em Windows com antivírus/proxy.
    """
    # Tenta usar certifi se disponível
    try:
        import certifi
        os.environ['SSL_CERT_FILE'] = certifi.where()
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    except ImportError:
        pass
    
    # Aumenta timeout de conexão
    socket.setdefaulttimeout(60)


# Aplica fix ao importar o módulo
fix_ssl_for_windows()

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("inmet_ingestion")

# Reduz logs verbosos
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)

# =========================
# MÉTRICAS DE PERFORMANCE
# =========================

@dataclass
class IngestionMetrics:
    """Coleta métricas de performance durante a ingestão."""
    start_time: float = field(default_factory=time.time)
    total_files: int = 0
    success_count: int = 0
    error_count: int = 0
    total_bytes_processed: int = 0
    total_rows_processed: int = 0
    
    time_download: float = 0.0
    time_transform: float = 0.0
    time_upload: float = 0.0
    
    errors: List[Dict] = field(default_factory=list)
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def files_per_second(self) -> float:
        if self.elapsed_time == 0:
            return 0
        return self.success_count / self.elapsed_time
    
    @property
    def avg_time_per_file(self) -> float:
        if self.success_count == 0:
            return 0
        return self.elapsed_time / self.success_count
    
    def to_cloudwatch_metrics(self) -> List[Dict]:
        return [
            {"MetricName": "ArquivosIngeridosTotal", "Value": self.success_count, "Unit": "Count"},
            {"MetricName": "ArquivosComErroTotal", "Value": self.error_count, "Unit": "Count"},
            {"MetricName": "TempoTotalSegundos", "Value": round(self.elapsed_time, 2), "Unit": "Seconds"},
            {"MetricName": "ArquivosPorSegundo", "Value": round(self.files_per_second, 3), "Unit": "Count/Second"},
            {"MetricName": "BytesProcessados", "Value": self.total_bytes_processed, "Unit": "Bytes"},
            {"MetricName": "LinhasProcessadas", "Value": self.total_rows_processed, "Unit": "Count"},
            {"MetricName": "TempoDownloadTotal", "Value": round(self.time_download, 2), "Unit": "Seconds"},
            {"MetricName": "TempoTransformTotal", "Value": round(self.time_transform, 2), "Unit": "Seconds"},
            {"MetricName": "TempoUploadTotal", "Value": round(self.time_upload, 2), "Unit": "Seconds"},
        ]
    
    def log_summary(self):
        logger.info("=" * 60)
        logger.info("RESUMO DA INGESTÃO")
        logger.info("=" * 60)
        logger.info(f"Total de arquivos: {self.total_files}")
        logger.info(f"Sucesso: {self.success_count} | Erros: {self.error_count}")
        logger.info(f"Tempo total: {self.elapsed_time:.2f}s")
        logger.info(f"Velocidade: {self.files_per_second:.2f} arquivos/segundo")
        logger.info(f"Tempo médio por arquivo: {self.avg_time_per_file:.2f}s")
        logger.info(f"Bytes processados: {self.total_bytes_processed:,}")
        logger.info(f"Linhas processadas: {self.total_rows_processed:,}")
        logger.info("-" * 60)
        total_time = max(self.elapsed_time, 1)
        logger.info(f"Tempo em download: {self.time_download:.2f}s ({self.time_download/total_time*100:.1f}%)")
        logger.info(f"Tempo em transformação: {self.time_transform:.2f}s ({self.time_transform/total_time*100:.1f}%)")
        logger.info(f"Tempo em upload: {self.time_upload:.2f}s ({self.time_upload/total_time*100:.1f}%)")
        logger.info("=" * 60)


@contextmanager
def timer():
    start = time.perf_counter()
    yield lambda: time.perf_counter() - start


# =========================
# THREAD-LOCAL RESOURCES
# =========================

_credentials = None
_thread_local = local()


def get_credentials():
    """Carrega credenciais do Google (uma vez só)."""
    global _credentials
    if _credentials is None:
        if not CREDENTIALS_PATH.exists():
            raise FileNotFoundError(
                f"Arquivo de credenciais não encontrado: {CREDENTIALS_PATH}\n"
                "Certifique-se de que o arquivo credentials.json está em keys/"
            )
        
        _credentials = service_account.Credentials.from_service_account_file(
            str(CREDENTIALS_PATH), 
            scopes=SCOPES
        )
        logger.info(f"Credenciais carregadas de: {CREDENTIALS_PATH}")
    return _credentials


def get_drive_service():
    """
    Retorna um service do Google Drive thread-safe.
    Cada thread terá sua própria instância.
    """
    if not hasattr(_thread_local, 'drive_service'):
        creds = get_credentials()
        _thread_local.drive_service = build(
            "drive", "v3", 
            credentials=creds,
            cache_discovery=False
        )
    return _thread_local.drive_service


def get_s3_client():
    """
    Retorna um cliente S3 thread-safe.
    """
    if not hasattr(_thread_local, 's3_client'):
        _thread_local.s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _thread_local.s3_client


def reset_thread_local():
    """Reseta recursos da thread atual (útil após erro SSL)."""
    if hasattr(_thread_local, 'drive_service'):
        delattr(_thread_local, 'drive_service')
    if hasattr(_thread_local, 's3_client'):
        delattr(_thread_local, 's3_client')


# =========================
# GOOGLE DRIVE OPERATIONS
# =========================

def list_year_folders(parent_folder_id: str) -> List[Dict]:
    """Lista pastas de ano dentro da pasta INMET."""
    service = get_drive_service()
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    folders = []
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()
        folders.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return sorted(folders, key=lambda x: x["name"])


def list_csv_files(folder_id: str) -> List[Dict]:
    """Lista arquivos CSV em uma pasta."""
    service = get_drive_service()
    query = f"'{folder_id}' in parents and name contains '.csv'"
    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name, size)",
            pageToken=page_token
        ).execute()
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return files


def download_csv_with_retry(file_id: str, filename: str, max_retries: int = 3) -> str:
    """
    Download com retry e tratamento de erros SSL/conexão.
    """
    tmp_dir = tempfile.gettempdir()
    # Usa file_id para evitar colisão de nomes
    safe_filename = f"{file_id}_{filename.replace(' ', '_')}"
    filepath = os.path.join(tmp_dir, safe_filename)
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            service = get_drive_service()
            request = service.files().get_media(fileId=file_id)
            
            with io.FileIO(filepath, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            
            return filepath
            
        except (ssl.SSLError, socket.timeout, ConnectionError) as e:
            last_error = e
            wait_time = 2 ** attempt  # 1, 2, 4 segundos
            logger.warning(
                f"Erro de conexão (tentativa {attempt + 1}/{max_retries}): {type(e).__name__}. "
                f"Aguardando {wait_time}s..."
            )
            reset_thread_local()  # Força recriação do service
            time.sleep(wait_time)
            
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                logger.warning(f"Erro no download (tentativa {attempt + 1}): {e}")
                time.sleep(1)
            else:
                raise
    
    raise last_error


# =========================
# PROCESSAMENTO INDIVIDUAL
# =========================

def process_single_file(file_info: Dict, year_name: str) -> Dict:
    """
    Processa um único arquivo: download → transform → upload.
    """
    filename = file_info["name"]
    file_id = file_info["id"]
    file_size = int(file_info.get("size", 0))
    
    result = {
        "filename": filename,
        "year": year_name,
        "success": False,
        "error": None,
        "rows": 0,
        "bytes": file_size,
        "time_download": 0,
        "time_transform": 0,
        "time_upload": 0,
    }
    
    local_path = None
    parquet_path = None
    
    try:
        # 1. Download (com retry)
        with timer() as t:
            local_path = download_csv_with_retry(file_id, filename)
        result["time_download"] = t()
        
        # 2. Transform CSV → Parquet
        with timer() as t:
            df = pd.read_csv(
                local_path,
                sep=";",
                encoding="latin1",
                skiprows=8,
                on_bad_lines="skip"
            )
            result["rows"] = len(df)
            
            # Gera nome do parquet
            base_name = os.path.splitext(filename)[0]
            parquet_name = f"{base_name}.parquet"
            parquet_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{parquet_name}")
            
            df.to_parquet(parquet_path, engine="pyarrow", index=False)
        result["time_transform"] = t()
        
        # 3. Upload to S3
        with timer() as t:
            s3 = get_s3_client()
            s3_key = f"bronze/inmet/year={year_name}/{parquet_name}"
            s3.upload_file(parquet_path, BUCKET_NAME, s3_key)
        result["time_upload"] = t()
        
        result["success"] = True
        
    except Exception as e:
        result["error"] = str(e)
        # Log já é feito no nível superior
        
    finally:
        # Cleanup arquivos temporários
        for path in [local_path, parquet_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
    
    return result


# =========================
# INGESTÃO PRINCIPAL
# =========================

def ingest_data(
    max_workers: int = MAX_WORKERS,
    send_metrics: bool = True,
    year_filter: Optional[str] = None
) -> str:
    """
    Ingestão paralela com métricas de performance.
    
    Args:
        max_workers: Número de threads paralelas
        send_metrics: Se True, envia métricas para CloudWatch
        year_filter: Se especificado, processa apenas o ano indicado (ex: "2025")
    
    Returns:
        String com resumo da execução
    """
    logger.info("=" * 60)
    logger.info("INICIANDO INGESTÃO INMET")
    logger.info(f"Workers paralelos: {max_workers}")
    logger.info(f"Bucket S3: {BUCKET_NAME}")
    logger.info(f"Região: {AWS_REGION}")
    if year_filter:
        logger.info(f"Filtro de ano: {year_filter}")
    logger.info("=" * 60)
    
    metrics = IngestionMetrics()
    
    # Valida configuração
    if not FOLDER_ID:
        raise ValueError("FOLDER_ID não configurado no .env")
    
    # Inicializa credenciais
    get_credentials()
    
    # Lista pastas de ano
    year_folders = list_year_folders(FOLDER_ID)
    if not year_folders:
        logger.warning("Nenhuma pasta de ano encontrada!")
        return "Nenhuma pasta de ano encontrada."
    
    # Aplica filtro de ano se especificado
    if year_filter:
        year_folders = [f for f in year_folders if f["name"] == year_filter]
        if not year_folders:
            return f"Ano {year_filter} não encontrado."
    
    logger.info(f"Pastas de ano: {[f['name'] for f in year_folders]}")
    
    # Coleta todos os arquivos
    all_files = []
    for year_folder in year_folders:
        year_name = year_folder["name"]
        csv_files = list_csv_files(year_folder["id"])
        logger.info(f"year={year_name}: {len(csv_files)} arquivos")
        
        for f in csv_files:
            all_files.append({
                "file": f,
                "year_name": year_name
            })
    
    metrics.total_files = len(all_files)
    logger.info(f"Total de arquivos para processar: {metrics.total_files}")
    
    if not all_files:
        logger.warning("Nenhum arquivo CSV encontrado!")
        return "Nenhum arquivo para processar."
    
    # Processamento paralelo
    processed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(
                process_single_file,
                item["file"],
                item["year_name"]
            ): item
            for item in all_files
        }
        
        for future in as_completed(future_to_file):
            item = future_to_file[future]
            result = future.result()
            processed += 1
            
            if result["success"]:
                metrics.success_count += 1
                metrics.total_rows_processed += result["rows"]
                metrics.total_bytes_processed += result["bytes"]
                metrics.time_download += result["time_download"]
                metrics.time_transform += result["time_transform"]
                metrics.time_upload += result["time_upload"]
                
                # Log de progresso a cada 50 arquivos
                if processed % 50 == 0 or processed == metrics.total_files:
                    logger.info(
                        f"PROGRESS: {processed}/{metrics.total_files} "
                        f"({processed/metrics.total_files*100:.1f}%) "
                        f"| {metrics.files_per_second:.1f} files/s"
                    )
            else:
                metrics.error_count += 1
                metrics.errors.append({
                    "file": result["filename"],
                    "year": result["year"],
                    "error": result["error"]
                })
                logger.error(
                    f"ERRO: year={result['year']} file={result['filename']}: {result['error']}"
                )
    
    # Log resumo final
    metrics.log_summary()
    
    # Envia métricas para CloudWatch
    if send_metrics:
        try:
            cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
            cloudwatch.put_metric_data(
                Namespace=METRIC_NAMESPACE,
                MetricData=metrics.to_cloudwatch_metrics()
            )
            logger.info("Métricas enviadas para CloudWatch")
        except Exception as e:
            logger.warning(f"Falha ao enviar métricas CloudWatch: {e}")
    
    # Log de erros detalhados
    if metrics.errors:
        logger.warning(f"\nArquivos com erro ({len(metrics.errors)}):")
        for err in metrics.errors[:10]:
            logger.warning(f"  - {err['year']}/{err['file']}: {err['error']}")
        if len(metrics.errors) > 10:
            logger.warning(f"  ... e mais {len(metrics.errors) - 10} erros")
    
    resumo = f"{metrics.success_count} sucesso | {metrics.error_count} erro(s) | {metrics.elapsed_time:.1f}s"
    logger.info(f"RESULTADO: {resumo}")
    
    return resumo


# =========================
# MODO SEQUENCIAL (DEBUG)
# =========================

def ingest_data_sequential(year_filter: Optional[str] = None) -> str:
    """
    Versão sequencial para debug de problemas de SSL/conexão.
    Mais lenta, mas mais estável.
    """
    return ingest_data(max_workers=1, year_filter=year_filter)


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    import sys
    
    # Argumentos: [workers] [year_filter]
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else MAX_WORKERS
    year_filter = sys.argv[2] if len(sys.argv) > 2 else None
    
    print(ingest_data(max_workers=workers, year_filter=year_filter))