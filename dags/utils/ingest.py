"""
Módulo de ingestão de dados INMET - Google Drive → S3 (Bronze)
Com logs no CloudWatch, alertas por email (SNS) e tratamento de erros.

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
from threading import local, Lock
from pathlib import Path

from dotenv import load_dotenv
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# =========================
# CONFIG
# =========================

PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-lab1")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

# CloudWatch Logs config
LOG_GROUP_NAME = os.getenv("LOG_GROUP_NAME", "/inmet/ingestao/bronze")
LOG_RETENTION_DAYS = 30

# CloudWatch Metrics config
METRIC_NAMESPACE = os.getenv("METRIC_NAMESPACE", "inmet_bronze")

# SNS para alertas - CONFIGURE NO .env
SNS_TOPIC_ARN = os.getenv("SNS_ALERT_TOPIC_ARN", "")

# Performance configs
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
LOG_FLUSH_INTERVAL = 100  # Flush logs a cada N arquivos

# Path para credentials (use variável de ambiente em produção)
CREDENTIALS_PATH = Path(os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    str(PROJECT_ROOT / "keys" / "credentials.json")
))

# =========================
# SSL FIX PARA WINDOWS
# =========================

def fix_ssl_for_windows():
    try:
        import certifi
        os.environ['SSL_CERT_FILE'] = certifi.where()
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    except ImportError:
        pass
    socket.setdefaulttimeout(60)

fix_ssl_for_windows()

# =========================
# CLOUDWATCH LOGS HANDLER
# =========================

class CloudWatchLogsHandler:
    """
    Gerencia envio de logs para CloudWatch Logs.
    - Cria log group e stream automaticamente
    - Flush periódico para não perder logs
    - Thread-safe
    """
    
    def __init__(self, log_group: str, region: str):
        self.log_group = log_group
        self.region = region
        self.client = boto3.client("logs", region_name=region)
        self.log_stream = None
        self.sequence_token = None
        self.log_buffer = []
        self.lock = Lock()  # Thread-safe
        self.flush_count = 0
        
    def setup(self):
        """Cria log group e stream para esta execução."""
        # Cria log group se não existir
        try:
            self.client.create_log_group(logGroupName=self.log_group)
            self.client.put_retention_policy(
                logGroupName=self.log_group,
                retentionInDays=LOG_RETENTION_DAYS
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
        except Exception as e:
            logging.warning(f"Erro ao criar log group: {e}")
        
        # Cria log stream com timestamp único
        self.log_stream = f"exec-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        try:
            self.client.create_log_stream(
                logGroupName=self.log_group,
                logStreamName=self.log_stream
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
        except Exception as e:
            logging.warning(f"Erro ao criar log stream: {e}")
        
        return self
    
    def log(self, message: str, level: str = "INFO"):
        """Adiciona mensagem ao buffer (thread-safe)."""
        timestamp_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        formatted_message = f"[{timestamp_str}] [{level}] {message}"
        
        with self.lock:
            self.log_buffer.append({
                "timestamp": int(time.time() * 1000),
                "message": formatted_message
            })
        
        # Log local também
        if level == "ERROR":
            logging.error(message)
        elif level == "WARNING":
            logging.warning(message)
        else:
            logging.info(message)
    
    def info(self, message: str):
        self.log(message, "INFO")
    
    def error(self, message: str):
        self.log(message, "ERROR")
    
    def warning(self, message: str):
        self.log(message, "WARNING")
    
    def flush(self, force: bool = False):
        """
        Envia logs para CloudWatch.
        - Flush automático a cada LOG_FLUSH_INTERVAL chamadas
        - force=True força o envio imediato
        """
        self.flush_count += 1
        
        # Flush periódico ou forçado
        if not force and self.flush_count % LOG_FLUSH_INTERVAL != 0:
            return
        
        with self.lock:
            if not self.log_buffer or not self.log_stream:
                return
            
            buffer_to_send = self.log_buffer.copy()
            self.log_buffer = []
        
        try:
            # Obtém sequence token
            response = self.client.describe_log_streams(
                logGroupName=self.log_group,
                logStreamNamePrefix=self.log_stream,
                limit=1
            )
            
            streams = response.get("logStreams", [])
            if streams and "uploadSequenceToken" in streams[0]:
                self.sequence_token = streams[0]["uploadSequenceToken"]
            
            # Envia logs (ordenados por timestamp)
            kwargs = {
                "logGroupName": self.log_group,
                "logStreamName": self.log_stream,
                "logEvents": sorted(buffer_to_send, key=lambda x: x["timestamp"])
            }
            
            if self.sequence_token:
                kwargs["sequenceToken"] = self.sequence_token
            
            response = self.client.put_log_events(**kwargs)
            self.sequence_token = response.get("nextSequenceToken")
            
        except Exception as e:
            logging.error(f"Falha ao enviar logs para CloudWatch: {e}")
            # Recoloca no buffer para tentar novamente
            with self.lock:
                self.log_buffer = buffer_to_send + self.log_buffer
    
    def get_log_url(self) -> str:
        """Retorna URL para visualizar os logs no console AWS."""
        encoded_group = self.log_group.replace('/', '$252F')
        return (
            f"https://{self.region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={self.region}#logsV2:log-groups/log-group/"
            f"{encoded_group}/log-events/{self.log_stream}"
        )


# =========================
# ALERTAS SNS (EMAIL/SMS)
# =========================

class AlertManager:
    """
    Gerencia alertas via SNS.
    Pode enviar para: Email, SMS, Slack, Lambda, etc.
    """
    
    def __init__(self, topic_arn: str, region: str):
        self.topic_arn = topic_arn
        self.region = region
        self.sns = None
        
        if topic_arn:
            try:
                self.sns = boto3.client("sns", region_name=region)
            except Exception as e:
                logging.warning(f"Falha ao inicializar SNS: {e}")
    
    def is_configured(self) -> bool:
        return bool(self.sns and self.topic_arn)
    
    def send_alert(self, subject: str, message: str) -> bool:
        """Envia alerta via SNS."""
        if not self.is_configured():
            logging.info("SNS não configurado. Configure SNS_ALERT_TOPIC_ARN no .env")
            return False
        
        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject[:100],  # SNS limita subject a 100 chars
                Message=message
            )
            logging.info(f"✅ Alerta enviado: {subject}")
            return True
        except Exception as e:
            logging.error(f"❌ Falha ao enviar alerta SNS: {e}")
            return False
    
    def alert_success(self, metrics, log_url: str):
        """Envia alerta de SUCESSO."""
        subject = f"✅ INMET Ingestão OK: {metrics.success_count} arquivos"
        
        message = f"""
✅ INGESTÃO CONCLUÍDA COM SUCESSO

📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 RESUMO:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Arquivos processados: {metrics.success_count:,}
• Erros: {metrics.error_count}
• Tempo total: {metrics.elapsed_time:.1f} segundos
• Velocidade: {metrics.files_per_second:.2f} arquivos/segundo
• Linhas processadas: {metrics.total_rows_processed:,}
• Bytes processados: {metrics.total_bytes_processed:,}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⏱️ TEMPO POR ETAPA:
• Download: {metrics.time_download:.1f}s
• Transformação: {metrics.time_transform:.1f}s
• Upload: {metrics.time_upload:.1f}s

🔗 Ver logs completos:
{log_url}

---
Pipeline de Ingestão INMET - Camada Bronze
"""
        return self.send_alert(subject, message)
    
    def alert_error(self, metrics, log_url: str):
        """Envia alerta de ERRO."""
        if metrics.error_count == 0:
            return False
        
        error_rate = (metrics.error_count / metrics.total_files) * 100
        
        # Lista primeiros erros
        error_details = "\n".join([
            f"  • {e['year']}/{e['file']}: {e['error'][:80]}"
            for e in metrics.errors[:10]
        ])
        
        subject = f"🚨 INMET Ingestão: {metrics.error_count} ERRO(S) ({error_rate:.1f}%)"
        
        message = f"""
🚨 ALERTA DE ERRO NA INGESTÃO INMET

📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 RESUMO:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Total de arquivos: {metrics.total_files:,}
• Sucesso: {metrics.success_count:,}
• ❌ ERROS: {metrics.error_count} ({error_rate:.1f}%)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

❌ ARQUIVOS COM ERRO:
{error_details}
{"... e mais " + str(len(metrics.errors) - 10) + " erros" if len(metrics.errors) > 10 else ""}

🔗 Ver logs completos:
{log_url}

⚠️ AÇÃO NECESSÁRIA: Verifique os logs para identificar a causa dos erros.

---
Pipeline de Ingestão INMET - Camada Bronze
"""
        return self.send_alert(subject, message)
    
    def alert_fatal(self, error: str, log_url: str):
        """Envia alerta de ERRO FATAL (crash)."""
        subject = "💀 INMET Ingestão: ERRO FATAL"
        
        message = f"""
💀 ERRO FATAL NA INGESTÃO INMET

📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

❌ ERRO:
{error}

🔗 Ver logs completos:
{log_url}

⚠️ O pipeline foi interrompido. Ação imediata necessária!

---
Pipeline de Ingestão INMET - Camada Bronze
"""
        return self.send_alert(subject, message)


# =========================
# LOGGING LOCAL
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("inmet_ingestion")
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
    
    @property
    def success_rate(self) -> float:
        if self.total_files == 0:
            return 0
        return (self.success_count / self.total_files) * 100
    
    def to_cloudwatch_metrics(self) -> List[Dict]:
        return [
            {"MetricName": "ArquivosIngeridosTotal", "Value": self.success_count, "Unit": "Count"},
            {"MetricName": "ArquivosComErroTotal", "Value": self.error_count, "Unit": "Count"},
            {"MetricName": "TaxaSucesso", "Value": round(self.success_rate, 2), "Unit": "Percent"},
            {"MetricName": "TempoTotalSegundos", "Value": round(self.elapsed_time, 2), "Unit": "Seconds"},
            {"MetricName": "ArquivosPorSegundo", "Value": round(self.files_per_second, 3), "Unit": "Count/Second"},
            {"MetricName": "BytesProcessados", "Value": self.total_bytes_processed, "Unit": "Bytes"},
            {"MetricName": "LinhasProcessadas", "Value": self.total_rows_processed, "Unit": "Count"},
            {"MetricName": "TempoDownloadTotal", "Value": round(self.time_download, 2), "Unit": "Seconds"},
            {"MetricName": "TempoTransformTotal", "Value": round(self.time_transform, 2), "Unit": "Seconds"},
            {"MetricName": "TempoUploadTotal", "Value": round(self.time_upload, 2), "Unit": "Seconds"},
        ]
    
    def get_summary_lines(self) -> List[str]:
        total_time = max(self.elapsed_time, 1)
        return [
            "=" * 60,
            "RESUMO DA INGESTÃO",
            "=" * 60,
            f"Total de arquivos: {self.total_files}",
            f"Sucesso: {self.success_count} | Erros: {self.error_count}",
            f"Taxa de sucesso: {self.success_rate:.1f}%",
            f"Tempo total: {self.elapsed_time:.2f}s",
            f"Velocidade: {self.files_per_second:.2f} arquivos/segundo",
            f"Bytes processados: {self.total_bytes_processed:,}",
            f"Linhas processadas: {self.total_rows_processed:,}",
            "-" * 60,
            f"Download: {self.time_download:.1f}s ({self.time_download/total_time*100:.1f}%)",
            f"Transform: {self.time_transform:.1f}s ({self.time_transform/total_time*100:.1f}%)",
            f"Upload: {self.time_upload:.1f}s ({self.time_upload/total_time*100:.1f}%)",
            "=" * 60,
        ]


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
    global _credentials
    if _credentials is None:
        if not CREDENTIALS_PATH.exists():
            raise FileNotFoundError(
                f"Credenciais não encontradas: {CREDENTIALS_PATH}\n"
                "Configure GOOGLE_CREDENTIALS_PATH no .env"
            )
        _credentials = service_account.Credentials.from_service_account_file(
            str(CREDENTIALS_PATH), scopes=SCOPES
        )
    return _credentials


def get_drive_service():
    if not hasattr(_thread_local, 'drive_service'):
        creds = get_credentials()
        _thread_local.drive_service = build(
            "drive", "v3", credentials=creds, cache_discovery=False
        )
    return _thread_local.drive_service


def get_s3_client():
    if not hasattr(_thread_local, 's3_client'):
        _thread_local.s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _thread_local.s3_client


def reset_thread_local():
    if hasattr(_thread_local, 'drive_service'):
        delattr(_thread_local, 'drive_service')
    if hasattr(_thread_local, 's3_client'):
        delattr(_thread_local, 's3_client')


# =========================
# GOOGLE DRIVE OPERATIONS
# =========================

def list_year_folders(parent_folder_id: str) -> List[Dict]:
    service = get_drive_service()
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
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


def list_csv_files(folder_id: str) -> List[Dict]:
    service = get_drive_service()
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


def download_csv_with_retry(file_id: str, filename: str, max_retries: int = 3) -> str:
    """Download com retry, backoff exponencial e tratamento de rate limit."""
    tmp_dir = tempfile.gettempdir()
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
            
        except HttpError as e:
            last_error = e
            # Rate limit (429) ou erro de servidor (5xx)
            if e.resp.status in [429, 500, 502, 503]:
                wait_time = min(2 ** (attempt + 2), 60)  # 4s, 8s, 16s... max 60s
                logging.warning(f"Rate limit/Server error. Aguardando {wait_time}s...")
                time.sleep(wait_time)
                reset_thread_local()
            else:
                raise
                
        except (ssl.SSLError, socket.timeout, ConnectionError) as e:
            last_error = e
            wait_time = 2 ** attempt
            reset_thread_local()
            time.sleep(wait_time)
            
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                raise
    
    raise last_error


# =========================
# PROCESSAMENTO INDIVIDUAL
# =========================

def process_single_file(file_info: Dict, year_name: str) -> Dict:
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
        # 1. Download
        with timer() as t:
            local_path = download_csv_with_retry(file_id, filename)
        result["time_download"] = t()
        
        # 2. Transform
        with timer() as t:
            df = pd.read_csv(
                local_path, sep=";", encoding="latin1",
                skiprows=8, on_bad_lines="skip"
            )
            result["rows"] = len(df)
            
            base_name = os.path.splitext(filename)[0]
            parquet_name = f"{base_name}.parquet"
            parquet_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{parquet_name}")
            df.to_parquet(parquet_path, engine="pyarrow", index=False)
        result["time_transform"] = t()
        
        # 3. Upload
        with timer() as t:
            s3 = get_s3_client()
            s3_key = f"bronze/inmet/year={year_name}/{parquet_name}"
            s3.upload_file(parquet_path, BUCKET_NAME, s3_key)
        result["time_upload"] = t()
        
        result["success"] = True
        
    except Exception as e:
        result["error"] = str(e)
        
    finally:
        for path in [local_path, parquet_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass
    
    return result


# =========================
# INGESTÃO PRINCIPAL
# =========================

def ingest_data(
    max_workers: int = MAX_WORKERS,
    send_metrics: bool = True,
    year_filter: Optional[str] = None,
    send_success_alert: bool = True,
    send_error_alert: bool = True
) -> str:
    """
    Ingestão paralela com logs no CloudWatch e alertas por email.
    
    Args:
        max_workers: Número de threads paralelas
        send_metrics: Envia métricas para CloudWatch Metrics
        year_filter: Processa apenas o ano especificado
        send_success_alert: Envia email quando finalizar com sucesso
        send_error_alert: Envia email quando houver erros
    
    Returns:
        String com resumo da execução
    """
    
    # Inicializa handlers
    cw_logs = CloudWatchLogsHandler(LOG_GROUP_NAME, AWS_REGION).setup()
    alerts = AlertManager(SNS_TOPIC_ARN, AWS_REGION)
    metrics = IngestionMetrics()
    
    # Log inicial
    cw_logs.info("=" * 60)
    cw_logs.info("INICIANDO INGESTÃO INMET")
    cw_logs.info(f"Workers: {max_workers} | Bucket: {BUCKET_NAME} | Região: {AWS_REGION}")
    cw_logs.info(f"SNS configurado: {'Sim' if alerts.is_configured() else 'Não'}")
    if year_filter:
        cw_logs.info(f"Filtro de ano: {year_filter}")
    cw_logs.info("=" * 60)
    
    try:
        # Validações
        if not FOLDER_ID:
            raise ValueError("FOLDER_ID não configurado no .env")
        
        get_credentials()
        cw_logs.info(f"Credenciais OK: {CREDENTIALS_PATH}")
        
        # Lista pastas de ano
        year_folders = list_year_folders(FOLDER_ID)
        if not year_folders:
            cw_logs.warning("Nenhuma pasta de ano encontrada!")
            cw_logs.flush(force=True)
            return "Nenhuma pasta de ano encontrada."
        
        if year_filter:
            year_folders = [f for f in year_folders if f["name"] == year_filter]
            if not year_folders:
                cw_logs.warning(f"Ano {year_filter} não encontrado.")
                cw_logs.flush(force=True)
                return f"Ano {year_filter} não encontrado."
        
        cw_logs.info(f"Pastas de ano: {[f['name'] for f in year_folders]}")
        
        # Coleta arquivos
        all_files = []
        for year_folder in year_folders:
            year_name = year_folder["name"]
            csv_files = list_csv_files(year_folder["id"])
            cw_logs.info(f"year={year_name}: {len(csv_files)} arquivos")
            for f in csv_files:
                all_files.append({"file": f, "year_name": year_name})
        
        metrics.total_files = len(all_files)
        cw_logs.info(f"Total para processar: {metrics.total_files}")
        
        if not all_files:
            cw_logs.warning("Nenhum arquivo CSV encontrado!")
            cw_logs.flush(force=True)
            return "Nenhum arquivo para processar."
        
        # Processamento paralelo
        processed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(process_single_file, item["file"], item["year_name"]): item
                for item in all_files
            }
            
            for future in as_completed(future_to_file):
                result = future.result()
                processed += 1
                
                if result["success"]:
                    metrics.success_count += 1
                    metrics.total_rows_processed += result["rows"]
                    metrics.total_bytes_processed += result["bytes"]
                    metrics.time_download += result["time_download"]
                    metrics.time_transform += result["time_transform"]
                    metrics.time_upload += result["time_upload"]
                else:
                    metrics.error_count += 1
                    metrics.errors.append({
                        "file": result["filename"],
                        "year": result["year"],
                        "error": result["error"]
                    })
                    cw_logs.error(f"ERRO: {result['year']}/{result['filename']}: {result['error']}")
                
                # Log de progresso
                if processed % 50 == 0 or processed == metrics.total_files:
                    cw_logs.info(
                        f"PROGRESS: {processed}/{metrics.total_files} "
                        f"({processed/metrics.total_files*100:.1f}%) "
                        f"| {metrics.files_per_second:.1f} files/s"
                    )
                    # Flush periódico
                    cw_logs.flush()
        
        # Log resumo
        for line in metrics.get_summary_lines():
            cw_logs.info(line)
        
        # Envia métricas para CloudWatch Metrics
        if send_metrics:
            try:
                cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
                cloudwatch.put_metric_data(
                    Namespace=METRIC_NAMESPACE,
                    MetricData=metrics.to_cloudwatch_metrics()
                )
                cw_logs.info("Métricas enviadas para CloudWatch")
            except Exception as e:
                cw_logs.warning(f"Falha ao enviar métricas: {e}")
        
        # Envia alertas por email
        log_url = cw_logs.get_log_url()
        
        if metrics.error_count > 0 and send_error_alert:
            alerts.alert_error(metrics, log_url)
            cw_logs.info("Alerta de ERRO enviado")
        elif metrics.error_count == 0 and send_success_alert:
            alerts.alert_success(metrics, log_url)
            cw_logs.info("Alerta de SUCESSO enviado")
        
        resumo = f"{metrics.success_count} sucesso | {metrics.error_count} erro(s) | {metrics.elapsed_time:.1f}s"
        cw_logs.info(f"RESULTADO: {resumo}")
        cw_logs.info(f"Ver logs: {log_url}")
        
        return resumo
        
    except Exception as e:
        error_msg = str(e)
        cw_logs.error(f"ERRO FATAL: {error_msg}")
        
        # Alerta de erro fatal
        if send_error_alert:
            alerts.alert_fatal(error_msg, cw_logs.get_log_url())
        
        raise
        
    finally:
        # SEMPRE envia os logs restantes
        cw_logs.flush(force=True)


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    import sys
    
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else MAX_WORKERS
    year_filter = sys.argv[2] if len(sys.argv) > 2 else None
    
    print(ingest_data(max_workers=workers, year_filter=year_filter))