"""
Módulo de Observabilidade
==========================

Gerencia métricas (CloudWatch), logs (CloudWatch Logs) e alertas (SNS)
de forma centralizada e reutilizável em todos os DAGs do pipeline.

Uso:
    from src.common.observability import ObservabilityManager
    
    obs = ObservabilityManager(
        pipeline_name="transform_bronze",
        namespace="pipeline_bronze",
        log_group="/transformacao/bronze"
    )
    
    # Enviar métricas
    obs.enviar_metricas({"RegistrosProcessados": 1000})
    
    # Enviar alerta de sucesso
    obs.enviar_alerta_sucesso("Bronze completo!", detalhes={"registros": 1000})
    
    # Enviar alerta de erro
    obs.enviar_alerta_erro("Task falhou", erro="Exception details")
"""

import os
import logging
import boto3
from datetime import datetime
from typing import Dict, Optional, Any


class ObservabilityManager:
    """
    Gerenciador centralizado de observabilidade para pipelines.
    
    Funcionalidades:
    - CloudWatch Metrics: Envio de métricas customizadas
    - CloudWatch Logs: Registro estruturado de eventos
    - SNS Alerts: Notificações por email (sucesso/erro)
    """
    
    def __init__(
        self,
        pipeline_name: str,
        namespace: Optional[str] = None,
        log_group: Optional[str] = None,
        sns_topic_arn: Optional[str] = None,
        aws_region: Optional[str] = None
    ):
        """
        Inicializa o gerenciador de observabilidade.
        
        Args:
            pipeline_name: Nome do pipeline/DAG (ex: "transform_bronze")
            namespace: Namespace CloudWatch Metrics (padrão: env METRIC_NAMESPACE)
            log_group: CloudWatch Log Group (padrão: /pipeline/{pipeline_name})
            sns_topic_arn: ARN do tópico SNS (padrão: env SNS_ALERT_TOPIC_ARN)
            aws_region: Região AWS (padrão: env AWS_DEFAULT_REGION)
        """
        self.logger = logging.getLogger(__name__)
        self.pipeline_name = pipeline_name
        
        # Configurações AWS (env vars como fallback)
        self.aws_region = aws_region or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.namespace = namespace or os.getenv("METRIC_NAMESPACE", "pipeline_metrics")
        self.log_group = log_group or f"/pipeline/{pipeline_name}"
        self.sns_topic_arn = sns_topic_arn or os.getenv("SNS_ALERT_TOPIC_ARN", "")
        
        # Clientes AWS (lazy initialization)
        self._cloudwatch = None
        self._logs = None
        self._sns = None
    
    @property
    def cloudwatch(self):
        """Cliente CloudWatch (lazy)"""
        if self._cloudwatch is None:
            self._cloudwatch = boto3.client("cloudwatch", region_name=self.aws_region)
        return self._cloudwatch
    
    @property
    def logs(self):
        """Cliente CloudWatch Logs (lazy)"""
        if self._logs is None:
            self._logs = boto3.client("logs", region_name=self.aws_region)
        return self._logs
    
    @property
    def sns(self):
        """Cliente SNS (lazy)"""
        if self._sns is None:
            self._sns = boto3.client("sns", region_name=self.aws_region)
        return self._sns
    
    # ========================================================================
    # CLOUDWATCH METRICS
    # ========================================================================
    
    def enviar_metricas(self, metricas: Dict[str, float], dimensoes: Optional[Dict[str, str]] = None):
        """
        Envia métricas customizadas para CloudWatch.
        
        Args:
            metricas: Dict com nome_metrica: valor (ex: {"RegistrosProcessados": 1000})
            dimensoes: Dimensões adicionais (ex: {"Fonte": "Clima"})
        
        Exemplo:
            obs.enviar_metricas(
                {"RegistrosProcessados": 1000, "Duracao": 120.5},
                dimensoes={"Camada": "Bronze", "Fonte": "Clima"}
            )
        """
        try:
            # Dimensões padrão
            dims = [{"Name": "Pipeline", "Value": self.pipeline_name}]
            
            # Adicionar dimensões customizadas
            if dimensoes:
                dims.extend([{"Name": k, "Value": str(v)} for k, v in dimensoes.items()])
            
            # Preparar métricas
            metric_data = []
            timestamp = datetime.utcnow()
            
            for nome, valor in metricas.items():
                metric_data.append({
                    "MetricName": nome,
                    "Value": float(valor),
                    "Timestamp": timestamp,
                    "Unit": "None",
                    "Dimensions": dims
                })
            
            # Enviar para CloudWatch
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=metric_data
            )
            
            self.logger.info(f"📊 {len(metricas)} métricas enviadas para CloudWatch: {list(metricas.keys())}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao enviar métricas para CloudWatch: {e}")
    
    # ========================================================================
    # CLOUDWATCH LOGS
    # ========================================================================
    
    def enviar_log(self, mensagem: str, nivel: str = "INFO", dados: Optional[Dict[str, Any]] = None):
        """
        Envia log estruturado para CloudWatch Logs.
        
        Args:
            mensagem: Mensagem principal do log
            nivel: Nível do log (INFO, WARNING, ERROR)
            dados: Dados adicionais para incluir no log (dict)
        
        Exemplo:
            obs.enviar_log(
                "Transformação Bronze concluída",
                nivel="INFO",
                dados={"registros": 1000, "duracao": 120.5}
            )
        """
        try:
            # Criar log group se não existir
            self._criar_log_group_se_necessario()
            
            # Log stream por data
            log_stream_name = datetime.now().strftime("%Y-%m-%d")
            self._criar_log_stream_se_necessario(log_stream_name)
            
            # Montar mensagem estruturada
            log_message = f"[{nivel}] {mensagem}"
            if dados:
                import json
                log_message += f"\n{json.dumps(dados, indent=2, default=str)}"
            
            # Obter sequence token
            sequence_token = self._obter_sequence_token(log_stream_name)
            
            # Enviar log
            log_event = {
                'logGroupName': self.log_group,
                'logStreamName': log_stream_name,
                'logEvents': [{
                    'timestamp': int(datetime.now().timestamp() * 1000),
                    'message': log_message
                }]
            }
            
            if sequence_token:
                log_event['sequenceToken'] = sequence_token
            
            self.logs.put_log_events(**log_event)
            self.logger.info(f"📝 Log enviado para CloudWatch: {self.log_group}/{log_stream_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao enviar log para CloudWatch: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _criar_log_group_se_necessario(self):
        """Cria log group se não existir"""
        try:
            self.logger.info(f"🔍 Verificando log group: {self.log_group}")
            self.logs.create_log_group(logGroupName=self.log_group)
            self.logs.put_retention_policy(
                logGroupName=self.log_group,
                retentionInDays=30
            )
            self.logger.info(f"✅ Log group criado: {self.log_group}")
        except self.logs.exceptions.ResourceAlreadyExistsException:
            self.logger.info(f"ℹ️ Log group já existe: {self.log_group}")
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar log group {self.log_group}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _criar_log_stream_se_necessario(self, log_stream_name: str):
        """Cria log stream se não existir"""
        try:
            self.logger.info(f"🔍 Verificando log stream: {log_stream_name}")
            self.logs.create_log_stream(
                logGroupName=self.log_group,
                logStreamName=log_stream_name
            )
            self.logger.info(f"✅ Log stream criado: {log_stream_name}")
        except self.logs.exceptions.ResourceAlreadyExistsException:
            self.logger.info(f"ℹ️ Log stream já existe: {log_stream_name}")
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar log stream {log_stream_name}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _obter_sequence_token(self, log_stream_name: str) -> Optional[str]:
        """Obtém sequence token para envio de log"""
        try:
            response = self.logs.describe_log_streams(
                logGroupName=self.log_group,
                logStreamNamePrefix=log_stream_name
            )
            if response['logStreams']:
                return response['logStreams'][0].get('uploadSequenceToken')
        except Exception:
            pass
        return None
    
    # ========================================================================
    # SNS ALERTS
    # ========================================================================
    
    def enviar_alerta_sucesso(
        self,
        titulo: str,
        detalhes: Optional[Dict[str, Any]] = None,
        enviar_email: bool = True,
        enviar_log: bool = True
    ):
        """
        Envia alerta de sucesso (SNS + CloudWatch Logs).
        
        Args:
            titulo: Título do alerta
            detalhes: Dados adicionais (métricas, contadores, etc)
            enviar_email: Enviar via SNS (email)
            enviar_log: Enviar para CloudWatch Logs
        
        Exemplo:
            obs.enviar_alerta_sucesso(
                "Transformação Bronze Concluída",
                detalhes={
                    "registros_clima": 1000,
                    "registros_culturas": 500,
                    "duracao": "2.5 min"
                }
            )
        """
        subject = f"✅ {self.pipeline_name} - {titulo}"
        
        message = f"""
✅ PIPELINE EXECUTADO COM SUCESSO

📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🔄 Pipeline: {self.pipeline_name}
📋 Status: SUCESSO
"""
        
        if detalhes:
            message += "\n📊 DETALHES:\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            for key, value in detalhes.items():
                message += f"  • {key}: {value}\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        message += f"\n🔗 Monitoramento: http://localhost:8080/dags/{self.pipeline_name}/grid\n"
        
        # Enviar via SNS (email)
        if enviar_email and self.sns_topic_arn:
            try:
                self.sns.publish(
                    TopicArn=self.sns_topic_arn,
                    Subject=subject[:100],
                    Message=message
                )
                self.logger.info(f"📧 Alerta de sucesso enviado via SNS")
            except Exception as e:
                self.logger.error(f"❌ Erro ao enviar alerta via SNS: {e}")
        elif enviar_email:
            self.logger.info("ℹ️ SNS não configurado (SNS_ALERT_TOPIC_ARN vazio)")
        
        # Enviar para CloudWatch Logs
        if enviar_log:
            self.enviar_log(titulo, nivel="INFO", dados=detalhes)
    
    def enviar_alerta_erro(
        self,
        titulo: str,
        erro: str,
        task_id: Optional[str] = None,
        contexto: Optional[Dict[str, Any]] = None,
        enviar_email: bool = True,
        enviar_log: bool = True
    ):
        """
        Envia alerta de erro (SNS + CloudWatch Logs).
        
        Args:
            titulo: Título do erro
            erro: Mensagem de erro
            task_id: ID da task que falhou
            contexto: Contexto adicional (dados da execução)
            enviar_email: Enviar via SNS (email)
            enviar_log: Enviar para CloudWatch Logs
        
        Exemplo:
            obs.enviar_alerta_erro(
                "Falha na transformação Bronze",
                erro="FileNotFoundError: arquivo não encontrado",
                task_id="bronze_clima",
                contexto={"dag_run": "manual__2026-02-23T00:00:00"}
            )
        """
        task_info = f" - {task_id}" if task_id else ""
        subject = f"❌ {self.pipeline_name}{task_info} - {titulo}"
        
        message = f"""
❌ FALHA NO PIPELINE

📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
🔄 Pipeline: {self.pipeline_name}
"""
        
        if task_id:
            message += f"📋 Task: {task_id}\n"
        
        message += f"""
🔥 ERRO:
{erro[:500]}
"""
        
        if contexto:
            message += "\n📊 CONTEXTO:\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            for key, value in contexto.items():
                message += f"  • {key}: {value}\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        message += f"""
🔗 Logs: http://localhost:8080/dags/{self.pipeline_name}/grid

⚠️ Ação necessária: Verificar logs e corrigir antes de reprocessar.
"""
        
        # Enviar via SNS (email)
        if enviar_email and self.sns_topic_arn:
            try:
                self.sns.publish(
                    TopicArn=self.sns_topic_arn,
                    Subject=subject[:100],
                    Message=message
                )
                self.logger.info(f"📧 Alerta de erro enviado via SNS")
            except Exception as e:
                self.logger.error(f"❌ Erro ao enviar alerta via SNS: {e}")
        elif enviar_email:
            self.logger.info("ℹ️ SNS não configurado (SNS_ALERT_TOPIC_ARN vazio)")
        
        # Enviar para CloudWatch Logs
        if enviar_log:
            dados = {"erro": erro}
            if contexto:
                dados.update(contexto)
            self.enviar_log(titulo, nivel="ERROR", dados=dados)
    
    # ========================================================================
    # HELPERS PARA DAGs
    # ========================================================================
    
    def criar_callback_erro(self):
        """
        Cria função callback para on_failure_callback do Airflow.
        
        Uso no DAG:
            obs = ObservabilityManager("transform_bronze")
            
            task = PythonOperator(
                task_id="minha_task",
                python_callable=...,
                on_failure_callback=obs.criar_callback_erro()
            )
        """
        def callback(context):
            task_instance = context.get('task_instance')
            exception = context.get('exception', 'Erro desconhecido')
            
            self.enviar_alerta_erro(
                titulo=f"Falha na task {task_instance.task_id}",
                erro=str(exception),
                task_id=task_instance.task_id,
                contexto={
                    "dag_id": task_instance.dag_id,
                    "execution_date": str(context.get('execution_date'))
                }
            )
        
        return callback
    
    def monitorar_dag_run(self, dag_run, task_ids: list):
        """
        Analisa resultado do DAG run e envia alerta apropriado.
        
        Args:
            dag_run: Objeto DagRun do Airflow
            task_ids: Lista de task_ids principais para verificar status
        
        Uso:
            def enviar_resumo_final(**context):
                obs = ObservabilityManager("transform_bronze")
                obs.monitorar_dag_run(
                    context['dag_run'],
                    ['bronze_clima', 'bronze_culturas', 'validar_bronze']
                )
        """
        task_states = {}
        for task_id in task_ids:
            ti = dag_run.get_task_instance(task_id)
            if ti:
                task_states[task_id] = ti.state
        
        # Verifica se todas tasks sucederam
        all_success = all(state == 'success' for state in task_states.values())
        any_failed = any(state == 'failed' for state in task_states.values())
        
        if all_success:
            # Sucesso total
            detalhes = {f"Task {k}": "✅ OK" for k in task_states.keys()}
            self.enviar_alerta_sucesso("Pipeline concluído com sucesso", detalhes=detalhes)
        
        elif any_failed:
            # Tem falhas
            failed_tasks = [task for task, state in task_states.items() if state == 'failed']
            erro_msg = f"{len(failed_tasks)} task(s) falharam: {', '.join(failed_tasks)}"
            
            contexto = {f"Task {k}": v for k, v in task_states.items()}
            self.enviar_alerta_erro(
                titulo="Pipeline falhou",
                erro=erro_msg,
                contexto=contexto
            )
        
        else:
            # Estado parcial/desconhecido
            self.logger.warning(f"⚠️ Estado parcial do pipeline: {task_states}")
            self.enviar_log(
                "Pipeline em estado parcial",
                nivel="WARNING",
                dados={"task_states": task_states}
            )
