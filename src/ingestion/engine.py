"""
Engine de Ingestão - Lógica Genérica Reutilizável
=================================================

Responsabilidades:
- CloudWatch Logs (setup, buffer, flush)
- Alertas SNS (sucesso, erro, fatal)
- Métricas de performance
- Processamento paralelo
- Retry com backoff

Uso:
    from src.ingestion.engine import IngestionEngine, DataSource
    
    class MinhaFonte(DataSource):
        def list_files(self): ...
        def download_file(self, file_info): ...
        def process_file(self, local_path, file_info): ...
    
    engine = IngestionEngine(config)
    engine.run(MinhaFonte())
"""

import os
import time
import logging
import boto3
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
from threading import Lock
from pathlib import Path
from dotenv import load_dotenv

# =========================
# CONFIG BASE
# =========================

if os.environ.get("AIRFLOW_HOME"):
    PROJECT_ROOT = Path("/opt/airflow")
else:
    PROJECT_ROOT = Path(__file__).parent.parent.parent

load_dotenv(PROJECT_ROOT / ".env")

AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

# Logging local
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =========================
# CLOUDWATCH LOGS HANDLER
# =========================

class CloudWatchLogsHandler:
    """
    Gerencia envio de logs para CloudWatch Logs.
    - Thread-safe com buffer
    - Flush automático periódico
    - Cria log group/stream automaticamente
    """
    
    def __init__(self, log_group: str, region: str = AWS_REGION, retention_days: int = 30):
        self.log_group = log_group
        self.region = region
        self.retention_days = retention_days
        self.client = boto3.client("logs", region_name=region)
        self.log_stream = None
        self.sequence_token = None
        self.log_buffer = []
        self.lock = Lock()
        self.flush_count = 0
        self.flush_interval = 100
        
    def setup(self) -> "CloudWatchLogsHandler":
        """Cria log group e stream."""
        # Log group
        try:
            self.client.create_log_group(logGroupName=self.log_group)
            self.client.put_retention_policy(
                logGroupName=self.log_group,
                retentionInDays=self.retention_days
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
        except Exception as e:
            logging.warning(f"Erro ao criar log group: {e}")
        
        # Log stream
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
        """Adiciona ao buffer (thread-safe)."""
        timestamp_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        formatted = f"[{timestamp_str}] [{level}] {message}"
        
        with self.lock:
            self.log_buffer.append({
                "timestamp": int(time.time() * 1000),
                "message": formatted
            })
        
        # Log local
        getattr(logging, level.lower(), logging.info)(message)
    
    def info(self, msg: str): self.log(msg, "INFO")
    def error(self, msg: str): self.log(msg, "ERROR")
    def warning(self, msg: str): self.log(msg, "WARNING")
    
    def flush(self, force: bool = False):
        """Envia buffer para CloudWatch."""
        self.flush_count += 1
        if not force and self.flush_count % self.flush_interval != 0:
            return
        
        with self.lock:
            if not self.log_buffer or not self.log_stream:
                return
            buffer_to_send = self.log_buffer.copy()
            self.log_buffer = []
        
        try:
            response = self.client.describe_log_streams(
                logGroupName=self.log_group,
                logStreamNamePrefix=self.log_stream,
                limit=1
            )
            streams = response.get("logStreams", [])
            if streams and "uploadSequenceToken" in streams[0]:
                self.sequence_token = streams[0]["uploadSequenceToken"]
            
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
            logging.error(f"Falha ao enviar logs CloudWatch: {e}")
            with self.lock:
                self.log_buffer = buffer_to_send + self.log_buffer
    
    def get_log_url(self) -> str:
        """URL do console AWS."""
        encoded = self.log_group.replace('/', '$252F')
        return (
            f"https://{self.region}.console.aws.amazon.com/cloudwatch/home"
            f"?region={self.region}#logsV2:log-groups/log-group/"
            f"{encoded}/log-events/{self.log_stream}"
        )


# =========================
# ALERTAS SNS
# =========================

class AlertManager:
    """Envia alertas via SNS (email, SMS, Slack, etc)."""
    
    def __init__(self, topic_arn: str, region: str = AWS_REGION, pipeline_name: str = "Pipeline"):
        self.topic_arn = topic_arn
        self.region = region
        self.pipeline_name = pipeline_name
        self.sns = None
        
        if topic_arn:
            try:
                self.sns = boto3.client("sns", region_name=region)
            except Exception as e:
                logging.warning(f"Falha ao inicializar SNS: {e}")
    
    def is_configured(self) -> bool:
        return bool(self.sns and self.topic_arn)
    
    def send(self, subject: str, message: str) -> bool:
        if not self.is_configured():
            return False
        try:
            self.sns.publish(TopicArn=self.topic_arn, Subject=subject[:100], Message=message)
            return True
        except Exception as e:
            logging.error(f"Falha SNS: {e}")
            return False
    
    def alert_success(self, metrics: "IngestionMetrics", log_url: str):
        subject = f"✅ {self.pipeline_name} OK: {metrics.success_count} arquivos"
        message = f"""
✅ INGESTÃO CONCLUÍDA COM SUCESSO

📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 RESUMO:
• Arquivos: {metrics.success_count:,} sucesso | {metrics.error_count} erros
• Tempo: {metrics.elapsed_time:.1f}s ({metrics.files_per_second:.2f} arq/s)
• Linhas: {metrics.total_rows_processed:,}
• Bytes: {metrics.total_bytes_processed:,}

🔗 Logs: {log_url}
"""
        return self.send(subject, message)
    
    def alert_error(self, metrics: "IngestionMetrics", log_url: str):
        if metrics.error_count == 0:
            return False
        
        error_rate = (metrics.error_count / max(metrics.total_files, 1)) * 100
        errors_sample = "\n".join([
            f"  • {e.get('year', '?')}/{e.get('file', '?')}: {str(e.get('error', ''))[:60]}"
            for e in metrics.errors[:5]
        ])
        
        subject = f"🚨 {self.pipeline_name}: {metrics.error_count} ERRO(S)"
        message = f"""
🚨 ERROS NA INGESTÃO

📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 RESUMO:
• Total: {metrics.total_files} | Sucesso: {metrics.success_count} | ❌ Erros: {metrics.error_count} ({error_rate:.1f}%)

❌ ERROS:
{errors_sample}

🔗 Logs: {log_url}
"""
        return self.send(subject, message)
    
    def alert_fatal(self, error: str, log_url: str):
        subject = f"💀 {self.pipeline_name}: ERRO FATAL"
        message = f"""
💀 ERRO FATAL

📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

❌ {error}

🔗 Logs: {log_url}
"""
        return self.send(subject, message)


# =========================
# MÉTRICAS
# =========================

@dataclass
class IngestionMetrics:
    """Métricas de performance da ingestão."""
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
        return self.success_count / max(self.elapsed_time, 0.001)
    
    @property
    def success_rate(self) -> float:
        return (self.success_count / max(self.total_files, 1)) * 100
    
    def to_cloudwatch(self, namespace: str) -> List[Dict]:
        return [
            {"MetricName": "ArquivosIngeridos", "Value": self.success_count, "Unit": "Count"},
            {"MetricName": "ArquivosComErro", "Value": self.error_count, "Unit": "Count"},
            {"MetricName": "TaxaSucesso", "Value": round(self.success_rate, 2), "Unit": "Percent"},
            {"MetricName": "TempoTotal", "Value": round(self.elapsed_time, 2), "Unit": "Seconds"},
            {"MetricName": "ArquivosPorSegundo", "Value": round(self.files_per_second, 3), "Unit": "Count/Second"},
            {"MetricName": "BytesProcessados", "Value": self.total_bytes_processed, "Unit": "Bytes"},
            {"MetricName": "LinhasProcessadas", "Value": self.total_rows_processed, "Unit": "Count"},
        ]
    
    def log_summary(self, logger: CloudWatchLogsHandler):
        logger.info("=" * 50)
        logger.info("RESUMO DA INGESTÃO")
        logger.info(f"Total: {self.total_files} | Sucesso: {self.success_count} | Erros: {self.error_count}")
        logger.info(f"Taxa: {self.success_rate:.1f}% | Tempo: {self.elapsed_time:.1f}s")
        logger.info(f"Velocidade: {self.files_per_second:.2f} arq/s")
        logger.info(f"Linhas: {self.total_rows_processed:,} | Bytes: {self.total_bytes_processed:,}")
        logger.info("=" * 50)


# =========================
# DATA SOURCE (INTERFACE)
# =========================

class DataSource(ABC):
    """
    Interface para fontes de dados.
    
    Cada pipeline implementa essa classe:
    - clima.py → GoogleDriveSource
    - solos.py → SolosAPISource
    - agricultura.py → IBGESource
    """
    
    @abstractmethod
    def connect(self) -> None:
        """Inicializa conexão com a fonte."""
        pass
    
    @abstractmethod
    def list_files(self) -> List[Dict[str, Any]]:
        """
        Lista arquivos disponíveis.
        
        Retorna lista de dicts com pelo menos:
        - 'id': identificador único
        - 'name': nome do arquivo
        - 'metadata': dict com info extra (year, size, etc)
        """
        pass
    
    @abstractmethod
    def download_file(self, file_info: Dict) -> str:
        """
        Baixa arquivo e retorna path local.
        
        Args:
            file_info: Dict retornado por list_files()
        
        Returns:
            Caminho do arquivo local baixado
        """
        pass
    
    @abstractmethod
    def process_file(self, local_path: str, file_info: Dict) -> Dict:
        """
        Processa arquivo e faz upload para destino.
        
        Args:
            local_path: Caminho do arquivo baixado
            file_info: Dict com metadados
        
        Returns:
            Dict com: success, rows, bytes, error (se houver)
        """
        pass
    
    def cleanup_file(self, local_path: str) -> None:
        """Remove arquivo temporário."""
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except:
                pass


# =========================
# INGESTION ENGINE
# =========================

@dataclass
class IngestionConfig:
    """Configuração da engine."""
    pipeline_name: str
    log_group: str
    metric_namespace: str
    sns_topic_arn: str = ""
    max_workers: int = 10
    send_metrics: bool = True
    send_success_alert: bool = True
    send_error_alert: bool = True


class IngestionEngine:
    """
    Engine genérica de ingestão.
    
    Gerencia:
    - Logging (CloudWatch)
    - Métricas
    - Alertas
    - Processamento paralelo
    
    Uso:
        config = IngestionConfig(
            pipeline_name="INMET Clima",
            log_group="/inmet/ingestao/bronze",
            metric_namespace="inmet_bronze"
        )
        
        engine = IngestionEngine(config)
        result = engine.run(GoogleDriveSource())
    """
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.logger = None
        self.alerts = None
        self.metrics = None
    
    def run(self, source: DataSource, file_filter: Optional[callable] = None) -> str:
        """
        Executa ingestão completa.
        
        Args:
            source: Implementação de DataSource
            file_filter: Função opcional para filtrar arquivos (recebe file_info, retorna bool)
        
        Returns:
            String com resumo da execução
        """
        # Setup
        self.logger = CloudWatchLogsHandler(self.config.log_group).setup()
        self.alerts = AlertManager(
            self.config.sns_topic_arn, 
            pipeline_name=self.config.pipeline_name
        )
        self.metrics = IngestionMetrics()
        
        self.logger.info("=" * 50)
        self.logger.info(f"INICIANDO {self.config.pipeline_name}")
        self.logger.info(f"Workers: {self.config.max_workers}")
        self.logger.info("=" * 50)
        
        try:
            # Conecta na fonte
            source.connect()
            self.logger.info("Conexão OK")
            
            # Lista arquivos
            all_files = source.list_files()
            self.logger.info(f"Arquivos encontrados: {len(all_files)}")
            
            # Aplica filtro se houver
            if file_filter:
                all_files = [f for f in all_files if file_filter(f)]
                self.logger.info(f"Após filtro: {len(all_files)}")
            
            if not all_files:
                self.logger.warning("Nenhum arquivo para processar")
                self.logger.flush(force=True)
                return "Nenhum arquivo para processar"
            
            self.metrics.total_files = len(all_files)
            
            # Processamento paralelo
            self._process_files(source, all_files)
            
            # Resumo
            self.metrics.log_summary(self.logger)
            
            # Métricas CloudWatch
            if self.config.send_metrics:
                self._send_cloudwatch_metrics()
            
            # Alertas
            log_url = self.logger.get_log_url()
            if self.metrics.error_count > 0 and self.config.send_error_alert:
                self.alerts.alert_error(self.metrics, log_url)
            elif self.metrics.error_count == 0 and self.config.send_success_alert:
                self.alerts.alert_success(self.metrics, log_url)
            
            result = f"{self.metrics.success_count} sucesso | {self.metrics.error_count} erros | {self.metrics.elapsed_time:.1f}s"
            self.logger.info(f"RESULTADO: {result}")
            self.logger.info(f"Logs: {log_url}")
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"ERRO FATAL: {error_msg}")
            
            if self.config.send_error_alert:
                self.alerts.alert_fatal(error_msg, self.logger.get_log_url())
            
            raise
            
        finally:
            self.logger.flush(force=True)
    
    def _process_files(self, source: DataSource, files: List[Dict]):
        """Processa arquivos em paralelo."""
        processed = 0
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._process_single, source, f): f 
                for f in files
            }
            
            for future in as_completed(futures):
                result = future.result()
                processed += 1
                
                if result["success"]:
                    self.metrics.success_count += 1
                    self.metrics.total_rows_processed += result.get("rows", 0)
                    self.metrics.total_bytes_processed += result.get("bytes", 0)
                    self.metrics.time_download += result.get("time_download", 0)
                    self.metrics.time_transform += result.get("time_transform", 0)
                    self.metrics.time_upload += result.get("time_upload", 0)
                else:
                    self.metrics.error_count += 1
                    self.metrics.errors.append({
                        "file": result.get("filename", "?"),
                        "year": result.get("year", "?"),
                        "error": result.get("error", "?")
                    })
                    self.logger.error(f"ERRO: {result.get('filename')}: {result.get('error')}")
                
                # Log progresso
                if processed % 50 == 0 or processed == self.metrics.total_files:
                    pct = processed / self.metrics.total_files * 100
                    self.logger.info(f"PROGRESS: {processed}/{self.metrics.total_files} ({pct:.1f}%)")
                    self.logger.flush()
    
    def _process_single(self, source: DataSource, file_info: Dict) -> Dict:
        """Processa um arquivo."""
        result = {
            "filename": file_info.get("name", "?"),
            "year": file_info.get("metadata", {}).get("year", "?"),
            "success": False,
            "error": None,
            "rows": 0,
            "bytes": 0,
            "time_download": 0,
            "time_transform": 0,
            "time_upload": 0,
        }
        
        local_path = None
        
        try:
            # Download
            start = time.perf_counter()
            local_path = source.download_file(file_info)
            result["time_download"] = time.perf_counter() - start
            
            # Process + Upload
            start = time.perf_counter()
            process_result = source.process_file(local_path, file_info)
            result["time_transform"] = process_result.get("time_transform", 0)
            result["time_upload"] = process_result.get("time_upload", time.perf_counter() - start)
            
            result["rows"] = process_result.get("rows", 0)
            result["bytes"] = process_result.get("bytes", 0)
            result["success"] = process_result.get("success", True)
            result["error"] = process_result.get("error")
            
        except Exception as e:
            result["error"] = str(e)
            
        finally:
            source.cleanup_file(local_path)
        
        return result
    
    def _send_cloudwatch_metrics(self):
        """Envia métricas para CloudWatch."""
        try:
            cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
            cloudwatch.put_metric_data(
                Namespace=self.config.metric_namespace,
                MetricData=self.metrics.to_cloudwatch(self.config.metric_namespace)
            )
            self.logger.info("Métricas enviadas para CloudWatch")
        except Exception as e:
            self.logger.warning(f"Falha ao enviar métricas: {e}")
