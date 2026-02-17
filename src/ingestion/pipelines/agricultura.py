"""
Pipeline de Ingestão - agricultura
============================

Fonte: Google Drive (CSVs de agricultura)
Destino: S3 Raw (Parquet)
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
from src.validation.pipelines.agricultura import validate_dataframe

# =========================
# CONFIG ESPECÍFICA agricultura
# =========================

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID_AGRICULTURA")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-lab1")

# CloudWatch/SNS
LOG_GROUP_NAME = os.getenv("LOG_GROUP_NAME_AGRICULTURA", "/ingestao/raw")
METRIC_NAMESPACE_AGRICULTURA = os.getenv("METRIC_NAMESPACE_AGRICULTURA", "agricultura_bronze")
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
    Fonte de dados: Google Drive com CSVs de agricultura.
    
    Estrutura esperada no Drive:
        FOLDER_ID_AGRICULTURA/
        ├── 2020/
        │   ├── amostra1.csv
        │   └── amostra2.csv
        └── 2021/
            └── ...
    """
    
    def __init__(self, year_filter: Optional[str] = None):
        self.year_filter = year_filter
        self.folder_id = FOLDER_ID
    
    def connect(self) -> None:
        if not self.folder_id:
            raise ValueError("FOLDER_ID_AGRICULTURA não configurado no .env")
        _get_credentials()
        _get_drive_service()
    
    def list_files(self) -> List[Dict[str, Any]]:
        service = _get_drive_service()
        
        # Tenta listar pastas de ano primeiro
        year_folders = self._list_year_folders(service)
        
        all_files = []
        
        if year_folders:
            # Estrutura com pastas de ano
            if self.year_filter:
                year_folders = [f for f in year_folders if f["name"] == self.year_filter]
            
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
        else:
            # Arquivos direto na raiz (sem pastas de ano)
            csvs = self._list_csv_files(service, self.folder_id)
            
            for csv_file in csvs:
                all_files.append({
                    "id": csv_file["id"],
                    "name": csv_file["name"],
                    "metadata": {
                        "year": "raw",  # Usa "raw" como partição
                        "size": int(csv_file.get("size", 0)),
                        "folder_id": self.folder_id,
                    }
                })
        
        return all_files

    def _list_year_folders(self, service) -> List[Dict]:
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
                sep=",",
                encoding="utf-8",
                on_bad_lines="skip"
            )
            result["rows"] = len(df)
            
            # 2. VALIDAÇÃO
            start_validation = time.perf_counter()
            validation_result = validate_dataframe(df, fail_on_error=False)
            result["time_validation"] = time.perf_counter() - start_validation
            result["validation_warnings"] = validation_result.warning_count
            
            if not validation_result.is_valid:
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
            s3_key = f"raw/agricultura/year={year}/{parquet_name}"
            s3.upload_file(parquet_path, BUCKET_NAME, s3_key)
            result["time_upload"] = time.perf_counter() - start
            
            result["success"] = True
            
        except Exception as e:
            result["error"] = str(e)
            
        finally:
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
) -> str:
    """
    Executa ingestão de dados de agricultura.
    
    Args:
        max_workers: Threads paralelas para download
        year_filter: Processa apenas este ano (ex: "2023")
        send_metrics: Envia métricas para CloudWatch
        send_success_alert: Envia email de sucesso
        send_error_alert: Envia email de erro
    
    Returns:
        String com resumo: "X sucesso | Y erros | Zs"
    """
    config = IngestionConfig(
        pipeline_name="agricultura",
        log_group=LOG_GROUP_NAME,
        metric_namespace=METRIC_NAMESPACE_AGRICULTURA,
        sns_topic_arn=SNS_TOPIC_ARN,
        max_workers=max_workers,
        send_metrics=send_metrics,
        send_success_alert=send_success_alert,
        send_error_alert=send_error_alert,
    )
    
    source = GoogleDriveSource(year_filter=year_filter)
    engine = IngestionEngine(config)
    
    return engine.run(source)


if __name__ == "__main__":
    import sys
    
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else MAX_WORKERS
    year = sys.argv[2] if len(sys.argv) > 2 else None
    
    print(ingest_data(max_workers=workers, year_filter=year))