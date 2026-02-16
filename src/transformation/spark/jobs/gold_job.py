"""
JOB GOLD - Transformação Silver → Gold
=======================================

O QUE FAZ A CAMADA GOLD?
------------------------
A Gold é a camada de "resposta". Ela:
1. Cruza diferentes fontes de dados (clima + culturas)
2. Calcula métricas de negócio (risco climático)
3. Gera tabelas prontas para dashboards
4. RESPONDE PERGUNTAS DE NEGÓCIO

PERGUNTA QUE VAMOS RESPONDER:
-----------------------------
"Qual o risco climático de cada cultura por estação do ano?"

ANALOGIA: É o prato pronto servido na mesa.
Bronze = ingredientes organizados
Silver = ingredientes preparados
Gold = refeição pronta para comer

CONCEITOS SPARK USADOS:
-----------------------
1. crossJoin() - Cruza todas as linhas de duas tabelas (produto cartesiano)
2. UDF - User Defined Function (função Python dentro do Spark)
3. Broadcast - Otimização para tabelas pequenas
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, udf, broadcast,
    round as spark_round, concat, desc, asc, row_number
)
from pyspark.sql.types import (
    IntegerType, StringType, FloatType, StructType, StructField
)
from pyspark.sql.window import Window
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.transformation.spark.spark_session import criar_spark_session


# =========================================================
# FUNÇÕES DE CÁLCULO DE RISCO
# =========================================================
# Estas funções implementam a lógica de negócio

def calcular_score_temperatura_udf():
    """
    Cria uma UDF (User Defined Function) para calcular o score de temperatura.
    
    O QUE É UDF?
    ------------
    UDF permite usar funções Python dentro do Spark.
    O Spark distribui essa função para todos os workers.
    
    CUIDADO: UDFs são mais lentas que funções nativas do Spark.
    Use apenas quando não há alternativa nativa.
    
    LÓGICA DO SCORE:
    ----------------
    - 100: Temperatura perfeita (dentro do range ideal)
    - 70: Fora do ideal, mas tolerável (até 3°C de diferença)
    - 45: Moderadamente fora (3-6°C de diferença)
    - 20: Severamente fora (>6°C de diferença)
    """
    def calcular(temp_real, temp_min_ideal, temp_max_ideal):
        if temp_real is None or temp_min_ideal is None or temp_max_ideal is None:
            return 50  # Indefinido
        
        # Dentro do range ideal
        if temp_min_ideal <= temp_real <= temp_max_ideal:
            return 95
        
        # Fora do range - calcular diferença
        if temp_real < temp_min_ideal:
            diferenca = temp_min_ideal - temp_real
        else:
            diferenca = temp_real - temp_max_ideal
        
        # Classificar por severidade
        if diferenca <= 3:
            return 70  # Leve
        elif diferenca <= 6:
            return 45  # Moderado
        else:
            return 20  # Severo
    
    return udf(calcular, IntegerType())


def calcular_score_precipitacao_udf():
    """
    Cria UDF para calcular score de precipitação.
    
    LÓGICA:
    -------
    Compara precipitação real com necessidade da cultura.
    - Alta necessidade: precisa 200-500mm por estação
    - Média necessidade: precisa 100-350mm
    - Baixa necessidade: precisa 30-200mm
    """
    def calcular(precip_mm, necessidade_agua, dias_chuva):
        if precip_mm is None or necessidade_agua is None:
            return 50
        
        # Definir ranges ideais por necessidade
        ranges = {
            "Alta": {"min": 200, "max": 500, "critico": 100},
            "Média": {"min": 100, "max": 350, "critico": 50},
            "Baixa": {"min": 30, "max": 200, "critico": 15}
        }
        
        ref = ranges.get(necessidade_agua, ranges["Média"])
        
        # Dentro do ideal
        if ref["min"] <= precip_mm <= ref["max"]:
            if dias_chuva and dias_chuva >= 15:
                return 95  # Bem distribuído
            return 80  # Ok
        
        # Déficit crítico
        if precip_mm < ref["critico"]:
            return 15
        
        # Déficit moderado
        if precip_mm < ref["min"]:
            return 50
        
        # Excesso
        if precip_mm > ref["max"]:
            excesso = precip_mm - ref["max"]
            if excesso > 300:
                return 30
            return 60
        
        return 50
    
    return udf(calcular, IntegerType())


def classificar_risco_udf():
    """
    Classifica o risco geral baseado nos scores.
    
    PESOS:
    ------
    - Temperatura: 60%
    - Precipitação: 40%
    
    CLASSIFICAÇÃO:
    --------------
    - >= 80: BAIXO (verde)
    - >= 60: MÉDIO (amarelo)
    - >= 40: ALTO (laranja)
    - < 40: CRÍTICO (vermelho)
    """
    def classificar(score_temp, score_precip, tolerante_seca, tolerante_frio, sensivel_frio, sensivel_calor):
        if score_temp is None or score_precip is None:
            return "INDEFINIDO"
        
        # Calcular score base (60% temp, 40% precip)
        score_base = score_temp * 0.6 + score_precip * 0.4
        
        # Ajustes por tolerância
        ajuste = 0
        if tolerante_seca and score_precip < 50:
            ajuste += 15
        if tolerante_frio and score_temp < 60:
            ajuste += 15
        if sensivel_frio and score_temp < 70:
            ajuste -= 20
        if sensivel_calor and score_temp < 70:
            ajuste -= 20
        
        score_final = min(100, max(0, score_base + ajuste))
        
        if score_final >= 80:
            return "BAIXO"
        elif score_final >= 60:
            return "MÉDIO"
        elif score_final >= 40:
            return "ALTO"
        else:
            return "CRÍTICO"
    
    return udf(classificar, StringType())


def gerar_recomendacao_udf():
    """
    Gera recomendação textual baseada no risco.
    """
    def recomendar(risco):
        recomendacoes = {
            "BAIXO": "✅ IDEAL PARA PLANTIO",
            "MÉDIO": "⚠️ POSSÍVEL COM CUIDADOS",
            "ALTO": "⚠️ ALTO RISCO - EVITAR",
            "CRÍTICO": "❌ NÃO RECOMENDADO",
            "INDEFINIDO": "❓ DADOS INSUFICIENTES"
        }
        return recomendacoes.get(risco, "❓ DADOS INSUFICIENTES")
    
    return udf(recomendar, StringType())


# =========================================================
# FUNÇÕES PRINCIPAIS
# =========================================================

def gerar_fato_risco_climatico(
    spark: SparkSession,
    clima_estacional_path: str,
    culturas_path: str,
    output_path: str
) -> DataFrame:
    """
    GERA A TABELA FATO DE RISCO CLIMÁTICO
    =====================================
    
    Esta é a tabela principal que responde:
    "Qual o risco climático de cada cultura por estação do ano?"
    
    COMO FUNCIONA:
    --------------
    1. Carrega clima por estação (4 linhas: Verão, Outono, Inverno, Primavera)
    2. Carrega culturas (76 culturas)
    3. Faz CROSS JOIN: cada cultura × cada estação = 304 combinações
    4. Calcula scores de risco para cada combinação
    5. Classifica e gera recomendações
    
    CROSS JOIN EXPLICADO:
    ---------------------
    É como fazer todas as combinações possíveis.
    
    Culturas:     Estações:        Resultado:
    Café          Verão            Café + Verão
    Milho    ×    Inverno     =    Café + Inverno
    Soja          ...              Milho + Verão
                                   Milho + Inverno
                                   ... (304 linhas)
    """
    print("\n" + "="*70)
    print("🥇 GOLD: Gerando FATO_RISCO_CLIMATICO")
    print("="*70)
    print("\n📌 Pergunta: 'Qual o risco climático de cada cultura por estação?'")
    
    # =========================================================
    # PASSO 1: CARREGAR DADOS
    # =========================================================
    print("\n📖 Carregando dados Silver...")
    
    df_clima = spark.read.parquet(clima_estacional_path)
    df_culturas = spark.read.parquet(culturas_path)
    
    print(f"   Clima estacional: {df_clima.count()} registros (estações)")
    print(f"   Culturas: {df_culturas.count()} registros")
    
    # Mostrar dados de entrada
    print("\n🌡️ Clima por Estação:")
    df_clima.show()
    
    print("\n🌱 Amostra de Culturas:")
    df_culturas.select(
        "cultura", "categoria", "temp_min_ideal", "temp_max_ideal",
        "necessidade_de_agua", "tolerante_seca"
    ).show(5)
    
    # =========================================================
    # PASSO 2: CROSS JOIN
    # =========================================================
    # Cruzar cada cultura com cada estação
    # broadcast() otimiza quando uma tabela é pequena (cabe na memória)
    
    print("\n🔀 Fazendo CROSS JOIN (culturas × estações)...")
    
    df_cruzado = df_culturas.crossJoin(broadcast(df_clima))
    
    print(f"   Combinações geradas: {df_cruzado.count()}")
    
    # =========================================================
    # PASSO 3: CALCULAR SCORES
    # =========================================================
    print("\n📊 Calculando scores de risco...")
    
    # Registrar UDFs
    calc_temp = calcular_score_temperatura_udf()
    calc_precip = calcular_score_precipitacao_udf()
    
    df_scores = df_cruzado \
        .withColumn(
            "score_temperatura",
            calc_temp(
                col("temp_media"),
                col("temp_min_ideal"),
                col("temp_max_ideal")
            )
        ) \
        .withColumn(
            "score_precipitacao",
            calc_precip(
                col("precipitacao_total_mm"),
                col("necessidade_de_agua"),
                col("dias_chuva")
            )
        )
    
    # =========================================================
    # PASSO 4: CLASSIFICAR RISCO
    # =========================================================
    print("\n🎯 Classificando risco geral...")
    
    classif_risco = classificar_risco_udf()
    gerar_recom = gerar_recomendacao_udf()
    
    df_risco = df_scores \
        .withColumn(
            "risco_geral",
            classif_risco(
                col("score_temperatura"),
                col("score_precipitacao"),
                col("tolerante_seca"),
                col("tolerante_frio"),
                col("sensivel_frio"),
                col("sensivel_calor")
            )
        ) \
        .withColumn(
            "recomendacao",
            gerar_recom(col("risco_geral"))
        )
    
    # =========================================================
    # PASSO 5: CALCULAR SCORE FINAL
    # =========================================================
    df_final = df_risco.withColumn(
        "score_final",
        spark_round(col("score_temperatura") * 0.6 + col("score_precipitacao") * 0.4, 0).cast(IntegerType())
    )
    
    # =========================================================
    # PASSO 6: SELECIONAR E ORDENAR COLUNAS
    # =========================================================
    print("\n📋 Organizando colunas finais...")
    
    df_gold = df_final.select(
        # Identificadores
        col("cultura"),
        col("categoria"),
        col("ano"),
        col("estacao"),
        
        # Dados climáticos reais
        col("temp_media").alias("temp_real"),
        col("temp_min").alias("temp_min_real"),
        col("temp_max").alias("temp_max_real"),
        col("precipitacao_total_mm").alias("precipitacao_mm"),
        col("dias_chuva"),
        col("umidade_media"),
        
        # Requisitos da cultura
        col("temp_min_ideal"),
        col("temp_max_ideal"),
        col("necessidade_de_agua"),
        
        # Tolerâncias
        col("tolerante_seca"),
        col("tolerante_frio"),
        col("sensivel_frio"),
        col("sensivel_calor"),
        
        # Scores calculados
        col("score_temperatura"),
        col("score_precipitacao"),
        col("score_final"),
        
        # Classificação final
        col("risco_geral"),
        col("recomendacao")
    ).withColumn("processado_em", current_timestamp())
    
    # =========================================================
    # PASSO 7: SALVAR
    # =========================================================
    print(f"\n💾 Salvando em: {output_path}")
    
    df_gold.write \
        .mode("overwrite") \
        .partitionBy("estacao") \
        .parquet(output_path)
    
    print("\n✅ FATO_RISCO_CLIMATICO gerada com sucesso!")
    
    return df_gold


def gerar_relatorio_resumido(spark: SparkSession, gold_path: str, output_path: str) -> DataFrame:
    """
    Gera um relatório resumido de risco por estação.
    
    OBJETIVO:
    ---------
    Criar uma visão consolidada para dashboard executivo.
    
    MÉTRICAS:
    ---------
    - Top 10 culturas recomendadas por estação
    - Distribuição de risco por categoria
    """
    print("\n" + "="*70)
    print("📊 GOLD: Gerando RELATÓRIO RESUMIDO")
    print("="*70)
    
    # Ler dados
    df_gold = spark.read.parquet(gold_path)
    
    # =========================================================
    # RELATÓRIO 1: TOP 10 CULTURAS POR ESTAÇÃO
    # =========================================================
    print("\n🏆 TOP 10 CULTURAS POR ESTAÇÃO:")
    
    # Usar Window para rankear dentro de cada estação
    window_spec = Window.partitionBy("estacao").orderBy(desc("score_final"))
    
    df_ranking = df_gold \
        .withColumn("ranking", row_number().over(window_spec)) \
        .filter(col("ranking") <= 10) \
        .select(
            "estacao", "ranking", "cultura", "categoria",
            "score_final", "risco_geral", "recomendacao"
        )
    
    # Mostrar por estação
    for estacao in ["Verão", "Outono", "Inverno", "Primavera"]:
        print(f"\n{'='*50}")
        print(f"🌿 {estacao.upper()}")
        print('='*50)
        df_ranking.filter(col("estacao") == estacao).show(10, truncate=False)
    
    # =========================================================
    # RELATÓRIO 2: DISTRIBUIÇÃO DE RISCO
    # =========================================================
    print("\n📈 DISTRIBUIÇÃO DE RISCO POR ESTAÇÃO:")
    
    df_distribuicao = df_gold.groupBy("estacao", "risco_geral") \
        .count() \
        .orderBy("estacao", "risco_geral")
    
    df_distribuicao.show()
    
    # Salvar relatórios
    df_ranking.write.mode("overwrite").parquet(f"{output_path}/ranking_culturas")
    df_distribuicao.write.mode("overwrite").parquet(f"{output_path}/distribuicao_risco")
    
    return df_ranking


def exibir_resposta_pergunta(spark: SparkSession, gold_path: str):
    """
    EXIBE A RESPOSTA FINAL À PERGUNTA DE NEGÓCIO
    =============================================
    
    Pergunta: "Qual o risco climático de cada cultura por estação do ano?"
    """
    print("\n")
    print("╔" + "═"*70 + "╗")
    print("║" + " "*20 + "🎯 RESPOSTA À PERGUNTA DE NEGÓCIO" + " "*17 + "║")
    print("╠" + "═"*70 + "╣")
    print("║ Pergunta: Qual o risco climático de cada cultura por estação?       ║")
    print("╚" + "═"*70 + "╝")
    
    df = spark.read.parquet(gold_path)
    
    # Resumo por estação
    print("\n📊 RESUMO EXECUTIVO:")
    print("-"*70)
    
    for estacao in ["Verão", "Outono", "Inverno", "Primavera"]:
        df_est = df.filter(col("estacao") == estacao)
        
        total = df_est.count()
        baixo = df_est.filter(col("risco_geral") == "BAIXO").count()
        medio = df_est.filter(col("risco_geral") == "MÉDIO").count()
        alto = df_est.filter(col("risco_geral") == "ALTO").count()
        critico = df_est.filter(col("risco_geral") == "CRÍTICO").count()
        
        pct_favoravel = (baixo / total) * 100 if total > 0 else 0
        
        emoji = "🌞" if estacao == "Verão" else "🍂" if estacao == "Outono" else "❄️" if estacao == "Inverno" else "🌸"
        
        print(f"\n{emoji} {estacao.upper()}")
        print(f"   ✅ Risco BAIXO:    {baixo:3d} culturas ({baixo/total*100:.0f}%)")
        print(f"   ⚠️  Risco MÉDIO:   {medio:3d} culturas ({medio/total*100:.0f}%)")
        print(f"   ⚠️  Risco ALTO:    {alto:3d} culturas ({alto/total*100:.0f}%)")
        print(f"   ❌ Risco CRÍTICO: {critico:3d} culturas ({critico/total*100:.0f}%)")
    
    # Top 5 culturas mais versáteis (baixo risco em mais estações)
    print("\n" + "="*70)
    print("🏆 TOP 5 CULTURAS MAIS VERSÁTEIS (menor risco geral):")
    print("="*70)
    
    df.filter(col("risco_geral") == "BAIXO") \
        .groupBy("cultura", "categoria") \
        .count() \
        .orderBy(desc("count")) \
        .show(5)
    
    # Culturas mais arriscadas
    print("\n⚠️ CULTURAS QUE EXIGEM MAIS ATENÇÃO (mais estações com risco alto/crítico):")
    print("="*70)
    
    df.filter(col("risco_geral").isin("ALTO", "CRÍTICO")) \
        .groupBy("cultura", "categoria") \
        .count() \
        .orderBy(desc("count")) \
        .show(5)


# =========================================================
# EXECUÇÃO PRINCIPAL
# =========================================================
if __name__ == "__main__":
    spark = criar_spark_session("gold-layer-job")
    
    BASE_PATH = "/home/claude/datalake"
    
    # Paths Silver
    SILVER_CLIMA_ESTACIONAL = f"{BASE_PATH}/silver/clima_estacional"
    SILVER_CULTURAS = f"{BASE_PATH}/silver/culturas"
    
    # Paths Gold
    GOLD_FATO_RISCO = f"{BASE_PATH}/gold/fato_risco_climatico"
    GOLD_RELATORIOS = f"{BASE_PATH}/gold/relatorios"
    
    try:
        # Gerar tabela fato principal
        df_risco = gerar_fato_risco_climatico(
            spark,
            SILVER_CLIMA_ESTACIONAL,
            SILVER_CULTURAS,
            GOLD_FATO_RISCO
        )
        
        # Mostrar amostra
        print("\n📋 AMOSTRA DA TABELA FATO_RISCO_CLIMATICO:")
        df_risco.select(
            "cultura", "estacao", "temp_real", "temp_min_ideal", "temp_max_ideal",
            "score_temperatura", "score_precipitacao", "score_final",
            "risco_geral", "recomendacao"
        ).orderBy("cultura", "estacao").show(20, truncate=False)
        
        # Gerar relatórios
        gerar_relatorio_resumido(spark, GOLD_FATO_RISCO, GOLD_RELATORIOS)
        
        # Exibir resposta final
        exibir_resposta_pergunta(spark, GOLD_FATO_RISCO)
        
        print("\n" + "="*70)
        print("✅ GOLD LAYER COMPLETA!")
        print("="*70)
        print(f"\n📁 Arquivos gerados em: {BASE_PATH}/gold/")
        
    finally:
        spark.stop()
