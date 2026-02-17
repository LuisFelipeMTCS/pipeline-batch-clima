"""
SPARK SESSION - Configuração da Sessão Spark
=============================================

O QUE É SPARK?
--------------
Apache Spark é um motor de processamento de dados distribuído.
Pense nele como um "super pandas" que pode processar MUITOS dados
dividindo o trabalho entre várias máquinas (ou cores do seu PC).

CONCEITOS IMPORTANTES:
----------------------
1. SparkSession: É o "ponto de entrada" para usar o Spark. 
   Pense como abrir o Excel - você precisa abrir o programa antes de usar.

2. DataFrame: Similar ao pandas DataFrame, mas distribuído.
   O Spark divide os dados em "partições" que podem ser processadas em paralelo.

3. Transformações vs Ações:
   - Transformações (lazy): filter, select, groupBy - NÃO executam imediatamente
   - Ações (eager): show, count, write - EXECUTAM o processamento
   
   Isso permite ao Spark otimizar o plano de execução antes de rodar.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
import os


def criar_spark_session(app_name: str = "pipeline-clima-dw") -> SparkSession:
    """
    Cria e configura uma sessão Spark.
    
    PARÂMETROS EXPLICADOS:
    ----------------------
    - master("local[*]"): Usa todos os cores da máquina local
      * "local[1]" = 1 core
      * "local[4]" = 4 cores
      * "local[*]" = todos os cores disponíveis
      * "spark://host:port" = cluster Spark remoto
    
    - config("spark.sql.adaptive.enabled", "true"): 
      Permite ao Spark ajustar o plano de execução durante a execução
      
    - config("spark.driver.memory", "2g"):
      Memória alocada para o driver (o "coordenador")
    
    Returns:
        SparkSession: Sessão configurada e pronta para uso
    """
    
    # Detectar se está em Docker ou local
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    
    # Credenciais AWS
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    
    # Caminho dos JARs S3 (em Docker: /home/airflow/.ivy2/jars)
    jars_path = "/home/airflow/.ivy2/jars/hadoop-aws-3.3.4.jar,/home/airflow/.ivy2/jars/aws-java-sdk-bundle-1.12.262.jar"
    
    # Criar a sessão Spark
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(spark_master) \
        .config("spark.jars", jars_path) \
        .config("spark.driver.extraClassPath", jars_path) \
        .config("spark.executor.extraClassPath", jars_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "false") \
        .getOrCreate()
    
    # Reduzir verbosidade dos logs
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"""
    ╔════════════════════════════════════════════════════════════════╗
    ║              🚀 SPARK SESSION INICIADA                         ║
    ╠════════════════════════════════════════════════════════════════╣
    ║  App Name    : {app_name:<45} ║
    ║  Master      : {spark.sparkContext.master:<45} ║
    ║  Spark UI    : http://localhost:4040                           ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    return spark


def get_schema_clima_raw() -> StructType:
    """
    Define o schema (estrutura) dos dados de clima RAW.
    
    POR QUE DEFINIR SCHEMA?
    -----------------------
    1. Performance: Spark não precisa inferir os tipos (mais rápido)
    2. Controle: Você define exatamente o que espera
    3. Erros: Detecta problemas de dados mais cedo
    
    TIPOS DE DADOS SPARK:
    ---------------------
    - StringType(): Texto
    - IntegerType(): Números inteiros
    - FloatType(): Números decimais
    - DateType(): Datas
    - TimestampType(): Data + hora
    - BooleanType(): True/False
    """
    return StructType([
        StructField("data", StringType(), True),
        StructField("hora_utc", StringType(), True),
        StructField("precipitacao_mm", FloatType(), True),
        StructField("pressao_mb", FloatType(), True),
        StructField("temperatura_c", FloatType(), True),
        StructField("temp_max_c", FloatType(), True),
        StructField("temp_min_c", FloatType(), True),
        StructField("umidade_pct", FloatType(), True),
        StructField("vento_velocidade_ms", FloatType(), True),
        StructField("radiacao_kj_m2", FloatType(), True),
    ])


def get_schema_culturas_raw() -> StructType:
    """
    Define o schema dos dados de culturas agrícolas RAW.
    """
    return StructType([
        StructField("cultura", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("clima_ideal", StringType(), True),
        StructField("necessidade_agua", StringType(), True),
        StructField("solo_recomendado", StringType(), True),
        StructField("observacoes", StringType(), True),
    ])


# Exemplo de uso (para testar)
if __name__ == "__main__":
    # Criar sessão
    spark = criar_spark_session("teste-spark-session")
    
    # Mostrar versão
    print(f"Spark Version: {spark.version}")
    
    # Parar sessão
    spark.stop()
    print("✅ Sessão encerrada com sucesso!")
