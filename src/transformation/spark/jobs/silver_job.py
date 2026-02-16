"""
JOB SILVER - Transformação Bronze → Silver
==========================================

O QUE FAZ A CAMADA SILVER?
--------------------------
A Silver é a camada de "preparação". Ela:
1. Agrega dados (horário → diário → mensal)
2. Calcula métricas derivadas
3. Enriquece dados com cálculos
4. Prepara para análises (mas ainda não responde perguntas de negócio)

ANALOGIA: É como preparar os ingredientes antes de cozinhar.
Você lava, corta, porciona - mas ainda não cozinha.

CONCEITOS SPARK USADOS:
-----------------------
1. .groupBy() - Agrupa dados por uma ou mais colunas
2. .agg() - Aplica funções de agregação (sum, avg, min, max)
3. .withColumn() - Cria novas colunas calculadas
4. UDF (User Defined Function) - Funções customizadas em Python
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, avg, sum as spark_sum, min as spark_min, 
    max as spark_max, count, when, lit, current_timestamp,
    round as spark_round, concat, expr
)
from pyspark.sql.window import Window
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.transformation.spark.spark_session import criar_spark_session


def criar_dim_tempo(spark: SparkSession, df_clima: DataFrame, output_path: str) -> DataFrame:
    """
    Cria a DIMENSÃO TEMPO a partir dos dados de clima.
    
    O QUE É UMA DIMENSÃO?
    ---------------------
    Em Data Warehouse, uma "dimensão" é uma tabela que descreve
    as características de algo. A dimensão tempo descreve cada data:
    - Que mês é? Que ano? Que estação?
    
    Isso permite fazer análises como:
    "Vendas por MÊS" ou "Temperatura por ESTAÇÃO"
    
    FUNÇÃO DAS ESTAÇÕES (Hemisfério Sul):
    -------------------------------------
    - Verão: Dezembro, Janeiro, Fevereiro
    - Outono: Março, Abril, Maio
    - Inverno: Junho, Julho, Agosto
    - Primavera: Setembro, Outubro, Novembro
    """
    print("\n" + "="*60)
    print("📅 SILVER: Criando DIMENSÃO TEMPO")
    print("="*60)
    
    # Extrair datas únicas
    df_datas = df_clima.select("data").distinct()
    
    # Adicionar atributos de tempo
    df_tempo = df_datas \
        .withColumn("ano", year(col("data"))) \
        .withColumn("mes", month(col("data"))) \
        .withColumn("dia", dayofmonth(col("data"))) \
        .withColumn("trimestre", ((month(col("data")) - 1) / 3 + 1).cast("int")) \
        .withColumn(
            "estacao",
            when(col("mes").isin(12, 1, 2), "Verão")
            .when(col("mes").isin(3, 4, 5), "Outono")
            .when(col("mes").isin(6, 7, 8), "Inverno")
            .otherwise("Primavera")
        ) \
        .withColumn(
            "nome_mes",
            when(col("mes") == 1, "Janeiro")
            .when(col("mes") == 2, "Fevereiro")
            .when(col("mes") == 3, "Março")
            .when(col("mes") == 4, "Abril")
            .when(col("mes") == 5, "Maio")
            .when(col("mes") == 6, "Junho")
            .when(col("mes") == 7, "Julho")
            .when(col("mes") == 8, "Agosto")
            .when(col("mes") == 9, "Setembro")
            .when(col("mes") == 10, "Outubro")
            .when(col("mes") == 11, "Novembro")
            .otherwise("Dezembro")
        )
    
    # Salvar
    df_tempo.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ Dimensão tempo criada com {df_tempo.count()} datas")
    df_tempo.show(5)
    
    return df_tempo


def agregar_clima_diario(spark: SparkSession, bronze_path: str, output_path: str) -> DataFrame:
    """
    Agrega dados horários de clima para DIÁRIO.
    
    POR QUE AGREGAR?
    ----------------
    Os dados RAW são HORÁRIOS (24 registros por dia).
    Para análise de risco climático, precisamos de médias diárias.
    
    FUNÇÕES DE AGREGAÇÃO:
    ---------------------
    - avg(): Média
    - sum(): Soma
    - min(): Mínimo
    - max(): Máximo
    - count(): Contagem
    """
    print("\n" + "="*60)
    print("🌡️ SILVER: Agregando clima DIÁRIO")
    print("="*60)
    
    # Ler dados Bronze
    print("\n📖 Lendo dados Bronze...")
    df_bronze = spark.read.parquet(bronze_path)
    
    # Agregar por dia
    # groupBy("data") = "para cada data única, faça..."
    # .agg() = aplique essas funções
    
    print("\n📊 Agregando métricas diárias...")
    
    df_diario = df_bronze.groupBy("data", "estacao_codigo", "estacao_nome", "uf") \
        .agg(
            # Temperatura: média, mínima e máxima do dia
            spark_round(avg("temperatura_c"), 1).alias("temp_media"),
            spark_round(spark_min("temp_min_c"), 1).alias("temp_min"),
            spark_round(spark_max("temp_max_c"), 1).alias("temp_max"),
            
            # Precipitação: soma do dia (mm total)
            spark_round(spark_sum("precipitacao_mm"), 1).alias("precipitacao_mm"),
            
            # Umidade: média do dia
            spark_round(avg("umidade_pct"), 1).alias("umidade_media"),
            
            # Radiação: soma do dia
            spark_round(spark_sum("radiacao_kj_m2"), 0).alias("radiacao_total"),
            
            # Contagem de registros (para validação)
            count("*").alias("qtd_registros")
        )
    
    # Adicionar metadados
    df_diario = df_diario \
        .withColumn("processado_em", current_timestamp()) \
        .withColumn("camada", lit("silver"))
    
    # Salvar
    print(f"\n💾 Salvando em: {output_path}")
    df_diario.write.mode("overwrite").parquet(output_path)
    
    print(f"\n✅ Agregação diária concluída!")
    print(f"   Total de dias: {df_diario.count()}")
    
    df_diario.orderBy("data").show(10)
    
    return df_diario


def agregar_clima_mensal(spark: SparkSession, diario_path: str, output_path: str) -> DataFrame:
    """
    Agrega dados diários de clima para MENSAL.
    
    MÉTRICAS CALCULADAS:
    --------------------
    - Temperatura média do mês
    - Temperatura mínima absoluta (dia mais frio)
    - Temperatura máxima absoluta (dia mais quente)
    - Precipitação total do mês
    - Dias com chuva (precipitação > 0)
    """
    print("\n" + "="*60)
    print("📊 SILVER: Agregando clima MENSAL")
    print("="*60)
    
    # Ler dados diários
    df_diario = spark.read.parquet(diario_path)
    
    # Adicionar ano e mês
    df_diario = df_diario \
        .withColumn("ano", year(col("data"))) \
        .withColumn("mes", month(col("data")))
    
    # Agregar por mês
    df_mensal = df_diario.groupBy("ano", "mes", "estacao_codigo") \
        .agg(
            spark_round(avg("temp_media"), 1).alias("temp_media_mensal"),
            spark_round(spark_min("temp_min"), 1).alias("temp_min_absoluta"),
            spark_round(spark_max("temp_max"), 1).alias("temp_max_absoluta"),
            spark_round(spark_sum("precipitacao_mm"), 1).alias("precipitacao_total_mm"),
            spark_round(avg("umidade_media"), 1).alias("umidade_media_mensal"),
            
            # Dias com chuva: contar dias onde precipitação > 0
            spark_sum(when(col("precipitacao_mm") > 0, 1).otherwise(0)).alias("dias_chuva"),
            
            # Total de dias no mês (para validação)
            count("*").alias("dias_no_mes")
        )
    
    # Adicionar estação do ano
    df_mensal = df_mensal.withColumn(
        "estacao",
        when(col("mes").isin(12, 1, 2), "Verão")
        .when(col("mes").isin(3, 4, 5), "Outono")
        .when(col("mes").isin(6, 7, 8), "Inverno")
        .otherwise("Primavera")
    )
    
    # Salvar
    df_mensal.write.mode("overwrite").parquet(output_path)
    
    print(f"\n✅ Agregação mensal concluída!")
    df_mensal.orderBy("ano", "mes").show(12)
    
    return df_mensal


def agregar_clima_estacional(spark: SparkSession, mensal_path: str, output_path: str) -> DataFrame:
    """
    Agrega dados mensais para ESTACIONAL (por estação do ano).
    
    ESTA É A AGREGAÇÃO CHAVE para responder:
    "Qual o risco climático por ESTAÇÃO?"
    
    RESULTADO:
    ----------
    Uma linha por estação (Verão, Outono, Inverno, Primavera)
    com as médias climáticas de cada uma.
    """
    print("\n" + "="*60)
    print("🍂 SILVER: Agregando clima ESTACIONAL")
    print("="*60)
    
    # Ler dados mensais
    df_mensal = spark.read.parquet(mensal_path)
    
    # Agregar por estação
    df_estacional = df_mensal.groupBy("ano", "estacao") \
        .agg(
            spark_round(avg("temp_media_mensal"), 1).alias("temp_media"),
            spark_round(spark_min("temp_min_absoluta"), 1).alias("temp_min"),
            spark_round(spark_max("temp_max_absoluta"), 1).alias("temp_max"),
            spark_round(spark_sum("precipitacao_total_mm"), 1).alias("precipitacao_total_mm"),
            spark_round(avg("umidade_media_mensal"), 1).alias("umidade_media"),
            spark_sum("dias_chuva").alias("dias_chuva")
        )
    
    # Salvar
    df_estacional.write.mode("overwrite").parquet(output_path)
    
    print(f"\n✅ Agregação estacional concluída!")
    print("\n📊 RESUMO CLIMÁTICO POR ESTAÇÃO:")
    df_estacional.orderBy(
        when(col("estacao") == "Verão", 1)
        .when(col("estacao") == "Outono", 2)
        .when(col("estacao") == "Inverno", 3)
        .otherwise(4)
    ).show()
    
    return df_estacional


def enriquecer_culturas(spark: SparkSession, bronze_path: str, output_path: str) -> DataFrame:
    """
    Enriquece dados de culturas com classificações adicionais.
    
    ENRIQUECIMENTOS:
    ----------------
    1. Perfil de temperatura (frio, ameno, quente, muito quente)
    2. Amplitude térmica tolerada
    3. Score de rusticidade (quanto maior, mais resistente)
    """
    print("\n" + "="*60)
    print("🌱 SILVER: Enriquecendo dados de CULTURAS")
    print("="*60)
    
    # Ler dados Bronze
    df_bronze = spark.read.parquet(bronze_path)
    
    # Calcular temperatura média ideal
    df_enriched = df_bronze.withColumn(
        "temp_media_ideal",
        (col("temp_min_ideal") + col("temp_max_ideal")) / 2
    )
    
    # Classificar perfil de temperatura
    df_enriched = df_enriched.withColumn(
        "perfil_temperatura",
        when(col("temp_media_ideal") < 15, "frio")
        .when(col("temp_media_ideal") < 22, "ameno")
        .when(col("temp_media_ideal") < 28, "quente")
        .otherwise("muito_quente")
    )
    
    # Calcular amplitude térmica tolerada
    df_enriched = df_enriched.withColumn(
        "amplitude_termica",
        col("temp_max_ideal") - col("temp_min_ideal")
    )
    
    # Calcular score de rusticidade
    # Quanto maior, mais resistente a condições adversas
    df_enriched = df_enriched.withColumn(
        "score_rusticidade",
        (
            when(col("tolerante_seca") == True, 2).otherwise(0) +
            when(col("tolerante_frio") == True, 2).otherwise(0) +
            (4 - col("necessidade_agua_score"))  # Baixa necessidade = +3
        )
    )
    
    # Salvar
    df_enriched.write.mode("overwrite").parquet(output_path)
    
    print(f"\n✅ Enriquecimento concluído!")
    df_enriched.select(
        "cultura", "categoria", "perfil_temperatura", 
        "amplitude_termica", "score_rusticidade"
    ).show(10)
    
    return df_enriched


# =========================================================
# EXECUÇÃO PRINCIPAL
# =========================================================
if __name__ == "__main__":
    spark = criar_spark_session("silver-layer-job")
    
    BASE_PATH = "/home/claude/datalake"
    
    # Paths Bronze
    BRONZE_CLIMA = f"{BASE_PATH}/bronze/clima"
    BRONZE_CULTURAS = f"{BASE_PATH}/bronze/culturas"
    
    # Paths Silver
    SILVER_DIM_TEMPO = f"{BASE_PATH}/silver/dim_tempo"
    SILVER_CLIMA_DIARIO = f"{BASE_PATH}/silver/clima_diario"
    SILVER_CLIMA_MENSAL = f"{BASE_PATH}/silver/clima_mensal"
    SILVER_CLIMA_ESTACIONAL = f"{BASE_PATH}/silver/clima_estacional"
    SILVER_CULTURAS = f"{BASE_PATH}/silver/culturas"
    
    try:
        # Ler Bronze de clima para criar dimensão tempo
        df_clima_bronze = spark.read.parquet(BRONZE_CLIMA)
        
        # Criar dimensão tempo
        criar_dim_tempo(spark, df_clima_bronze, SILVER_DIM_TEMPO)
        
        # Agregar clima: diário
        agregar_clima_diario(spark, BRONZE_CLIMA, SILVER_CLIMA_DIARIO)
        
        # Agregar clima: mensal
        agregar_clima_mensal(spark, SILVER_CLIMA_DIARIO, SILVER_CLIMA_MENSAL)
        
        # Agregar clima: estacional
        agregar_clima_estacional(spark, SILVER_CLIMA_MENSAL, SILVER_CLIMA_ESTACIONAL)
        
        # Enriquecer culturas
        enriquecer_culturas(spark, BRONZE_CULTURAS, SILVER_CULTURAS)
        
        print("\n" + "="*60)
        print("✅ SILVER LAYER COMPLETA!")
        print("="*60)
        
    finally:
        spark.stop()
