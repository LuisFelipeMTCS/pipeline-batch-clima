"""
JOB BRONZE - Transformação Raw → Bronze
========================================

O QUE FAZ A CAMADA BRONZE?
--------------------------
A Bronze é a primeira camada de "limpeza". Ela:
1. Lê os dados RAW (CSV original)
2. Padroniza nomes de colunas
3. Corrige tipos de dados
4. Remove registros totalmente vazios
5. Adiciona metadados (data de processamento, origem)

NÃO FAZ:
- Agregações
- Cálculos de negócio
- Junção de tabelas

ANALOGIA: É como organizar a bagunça do seu quarto - você não joga nada fora,
apenas coloca cada coisa no lugar certo.

CONCEITOS SPARK USADOS:
-----------------------
1. spark.read.csv() - Lê arquivo CSV
2. .withColumn() - Adiciona ou modifica coluna
3. .withColumnRenamed() - Renomeia coluna
4. .filter() - Filtra linhas
5. .write.parquet() - Salva em formato Parquet (otimizado)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, regexp_replace, to_date, when, lit, 
    current_timestamp, trim, lower, regexp_extract,
    coalesce
)
from pyspark.sql.types import FloatType, IntegerType
import os
import sys

# Adicionar o path do projeto
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from src.transformation.spark.spark_session import criar_spark_session


def processar_clima_bronze(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Processa dados de clima do INMET: Raw → Bronze
    
    PASSO A PASSO:
    --------------
    1. Ler CSV pulando as 8 primeiras linhas (metadados do INMET)
    2. Padronizar nomes de colunas
    3. Converter tipos (string → float, string → date)
    4. Adicionar metadados
    5. Salvar como Parquet
    
    Parameters:
        spark: Sessão Spark ativa
        input_path: Caminho do CSV raw
        output_path: Caminho para salvar o Parquet bronze
    
    Returns:
        DataFrame: Dados processados
    """
    print("\n" + "="*60)
    print("🥉 BRONZE: Processando dados de CLIMA")
    print("="*60)
    
    # =========================================================
    # PASSO 1: LER O CSV
    # =========================================================
    # O arquivo INMET tem 8 linhas de cabeçalho com metadados
    # Precisamos pular essas linhas
    
    print("\n📖 Lendo arquivo CSV...")
    
    # O arquivo INMET tem 8 linhas de metadados que precisamos pular
    # Vamos ler todo o arquivo e filtrar as linhas de dados
    
    # Primeiro, ler como texto para pular as 8 primeiras linhas
    rdd = spark.sparkContext.textFile(input_path)
    
    # Pegar o header (linha 9 = índice 8)
    header_line = rdd.zipWithIndex().filter(lambda x: x[1] == 8).map(lambda x: x[0]).first()
    header = header_line.split(";")
    
    # Pegar dados (linhas após a 9)
    data_rdd = rdd.zipWithIndex().filter(lambda x: x[1] > 8).map(lambda x: x[0])
    
    # Converter para DataFrame
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Criar schema baseado no header
    schema = StructType([StructField(col.strip(), StringType(), True) for col in header])
    
    # Converter RDD para DataFrame
    def parse_line(line):
        parts = line.split(";")
        # Garantir que temos o mesmo número de colunas que o header
        while len(parts) < len(header):
            parts.append(None)
        return parts[:len(header)]
    
    data_rdd = data_rdd.map(parse_line)
    df_raw = spark.createDataFrame(data_rdd, schema)
    
    # Mostrar schema original
    print("\n📋 Schema original:")
    df_raw.printSchema()
    
    # Mostrar primeiras linhas
    print("\n👀 Primeiras linhas (raw):")
    df_raw.show(5, truncate=False)
    
    # =========================================================
    # PASSO 2: LIMPAR E RENOMEAR COLUNAS
    # =========================================================
    # Nomes originais são longos e com caracteres especiais
    # Vamos padronizar para snake_case
    
    print("\n🔧 Padronizando colunas...")
    
    # Mapear nomes antigos para novos
    colunas_map = {
        "Data": "data",
        "Hora UTC": "hora_utc",
    }
    
    # Renomear colunas conhecidas
    df_renamed = df_raw
    for old_name, new_name in colunas_map.items():
        if old_name in df_raw.columns:
            df_renamed = df_renamed.withColumnRenamed(old_name, new_name)
    
    # Para outras colunas, vamos identificar pelo conteúdo
    # Encontrar coluna de temperatura (contém "TEMPERATURA" e "BULBO SECO")
    for col_name in df_renamed.columns:
        col_lower = col_name.lower()
        if "precipita" in col_lower and "total" in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "precipitacao_mm")
        elif "temperatura" in col_lower and "bulbo seco" in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "temperatura_c")
        elif "temperatura" in col_lower and "xima" in col_lower and "orvalho" not in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "temp_max_c")
        elif "temperatura" in col_lower and "nima" in col_lower and "orvalho" not in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "temp_min_c")
        elif "umidade relativa do ar" in col_lower and "horaria" in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "umidade_pct")
        elif "pressao atmosferica ao nivel" in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "pressao_mb")
        elif "radiacao global" in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "radiacao_kj_m2")
        elif "vento" in col_lower and "velocidade" in col_lower:
            df_renamed = df_renamed.withColumnRenamed(col_name, "vento_velocidade_ms")
    
    # =========================================================
    # PASSO 3: SELECIONAR COLUNAS RELEVANTES
    # =========================================================
    # Vamos manter apenas as colunas que precisamos
    
    colunas_finais = [
        "data", "hora_utc", "precipitacao_mm", "temperatura_c",
        "temp_max_c", "temp_min_c", "umidade_pct", "pressao_mb",
        "radiacao_kj_m2", "vento_velocidade_ms"
    ]
    
    # Selecionar apenas colunas que existem
    colunas_existentes = [c for c in colunas_finais if c in df_renamed.columns]
    df_selected = df_renamed.select(colunas_existentes)
    
    print(f"\n📊 Colunas selecionadas: {colunas_existentes}")
    
    # =========================================================
    # PASSO 4: CONVERTER TIPOS DE DADOS
    # =========================================================
    # CSV lê tudo como string, precisamos converter para os tipos corretos
    
    print("\n🔄 Convertendo tipos de dados...")
    
    # O INMET usa vírgula como decimal, precisamos trocar por ponto
    # Também precisamos tratar valores vazios
    
    df_typed = df_selected
    
    # Converter data (formato: 2025/01/01)
    if "data" in df_typed.columns:
        df_typed = df_typed.withColumn(
            "data",
            to_date(col("data"), "yyyy/MM/dd")
        )
    
    # Converter colunas numéricas
    colunas_numericas = [
        "precipitacao_mm", "temperatura_c", "temp_max_c", "temp_min_c",
        "umidade_pct", "pressao_mb", "radiacao_kj_m2", "vento_velocidade_ms"
    ]
    
    for col_name in colunas_numericas:
        if col_name in df_typed.columns:
            # Trocar vírgula por ponto, tratar vazios, e converter para float
            # Primeiro substituímos valores vazios ou só com espaços por null
            df_typed = df_typed.withColumn(
                col_name,
                when(
                    (col(col_name).isNull()) | 
                    (trim(col(col_name)) == "") |
                    (trim(col(col_name)) == ","),
                    lit(None)
                ).otherwise(
                    regexp_replace(col(col_name), ",", ".").cast(FloatType())
                )
            )
    
    # =========================================================
    # PASSO 5: REMOVER LINHAS INVÁLIDAS
    # =========================================================
    # Filtrar linhas onde a data é nula (provavelmente cabeçalho/lixo)
    
    print("\n🧹 Removendo linhas inválidas...")
    
    df_cleaned = df_typed.filter(col("data").isNotNull())
    
    # Contar registros antes e depois
    count_before = df_selected.count()
    count_after = df_cleaned.count()
    print(f"   Registros antes: {count_before}")
    print(f"   Registros depois: {count_after}")
    print(f"   Removidos: {count_before - count_after}")
    
    # =========================================================
    # PASSO 6: ADICIONAR METADADOS
    # =========================================================
    # Boas práticas: adicionar informações sobre o processamento
    
    print("\n📝 Adicionando metadados...")
    
    df_bronze = df_cleaned \
        .withColumn("estacao_codigo", lit("A771")) \
        .withColumn("estacao_nome", lit("SAO PAULO - INTERLAGOS")) \
        .withColumn("uf", lit("SP")) \
        .withColumn("processado_em", current_timestamp()) \
        .withColumn("camada", lit("bronze"))
    
    # =========================================================
    # PASSO 7: SALVAR COMO PARQUET
    # =========================================================
    # Parquet é um formato colunar otimizado para analytics
    # Muito mais rápido e compacto que CSV
    
    print(f"\n💾 Salvando em: {output_path}")
    
    df_bronze.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("\n✅ Bronze de CLIMA concluído!")
    print(f"   Total de registros: {df_bronze.count()}")
    
    # Mostrar amostra final
    print("\n📋 Amostra dos dados Bronze:")
    df_bronze.show(5)
    
    return df_bronze


def processar_culturas_bronze(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Processa dados de Culturas Agrícolas: Raw → Bronze
    
    TRANSFORMAÇÕES:
    ---------------
    1. Padronizar nomes de colunas
    2. Extrair temperatura min/max do campo "Clima Ideal"
    3. Classificar necessidade de água (score numérico)
    4. Extrair tolerâncias (seca, frio) das observações
    """
    print("\n" + "="*60)
    print("🥉 BRONZE: Processando dados de CULTURAS")
    print("="*60)
    
    # Ler CSV
    print("\n📖 Lendo arquivo CSV...")
    
    df_raw = spark.read.csv(
        input_path,
        sep=",",
        header=True,
        encoding="utf-8"
    )
    
    print(f"   Registros lidos: {df_raw.count()}")
    df_raw.show(5, truncate=False)
    
    # Padronizar nomes
    print("\n🔧 Padronizando colunas...")
    
    df_renamed = df_raw
    for old_col in df_raw.columns:
        # Converter para snake_case
        new_col = old_col.lower() \
            .replace(" ", "_") \
            .replace("á", "a").replace("é", "e").replace("í", "i") \
            .replace("ó", "o").replace("ú", "u") \
            .replace("ã", "a").replace("õ", "o") \
            .replace("ç", "c")
        df_renamed = df_renamed.withColumnRenamed(old_col, new_col)
    
    print(f"   Colunas: {df_renamed.columns}")
    
    # Extrair temperatura min e max do campo clima_ideal
    # Formato: "20-35°C; pleno sol"
    print("\n🌡️ Extraindo faixas de temperatura...")
    
    df_temp = df_renamed \
        .withColumn(
            "temp_min_ideal",
            regexp_extract(col("clima_ideal"), r"(\d+)-\d+", 1).cast(IntegerType())
        ) \
        .withColumn(
            "temp_max_ideal",
            regexp_extract(col("clima_ideal"), r"\d+-(\d+)", 1).cast(IntegerType())
        )
    
    # Classificar necessidade de água como score
    print("\n💧 Classificando necessidade de água...")
    
    df_agua = df_temp.withColumn(
        "necessidade_agua_score",
        when(col("necessidade_de_agua") == "Alta", 3)
        .when(col("necessidade_de_agua") == "Média", 2)
        .when(col("necessidade_de_agua") == "Baixa", 1)
        .otherwise(2)
    )
    
    # Extrair tolerâncias das observações
    print("\n🛡️ Extraindo tolerâncias...")
    
    df_tolerancias = df_agua \
        .withColumn(
            "tolerante_seca",
            when(
                lower(col("observacoes_praticas")).contains("tolerante") & 
                lower(col("observacoes_praticas")).contains("seca"),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "tolerante_frio",
            when(
                (lower(col("observacoes_praticas")).contains("tolerante") | 
                 lower(col("observacoes_praticas")).contains("resistente")) & 
                lower(col("observacoes_praticas")).contains("frio"),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "sensivel_frio",
            when(
                lower(col("observacoes_praticas")).contains("sensível") & 
                lower(col("observacoes_praticas")).contains("frio"),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "sensivel_calor",
            when(
                lower(col("observacoes_praticas")).contains("sensível") & 
                lower(col("observacoes_praticas")).contains("calor"),
                True
            ).otherwise(False)
        )
    
    # Adicionar metadados
    df_bronze = df_tolerancias \
        .withColumn("processado_em", current_timestamp()) \
        .withColumn("camada", lit("bronze"))
    
    # Salvar
    print(f"\n💾 Salvando em: {output_path}")
    
    df_bronze.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("\n✅ Bronze de CULTURAS concluído!")
    print(f"   Total de registros: {df_bronze.count()}")
    
    df_bronze.select(
        "cultura", "categoria", "temp_min_ideal", "temp_max_ideal",
        "necessidade_de_agua", "tolerante_seca", "tolerante_frio"
    ).show(10)
    
    return df_bronze


# =========================================================
# EXECUÇÃO PRINCIPAL
# =========================================================
if __name__ == "__main__":
    # Criar sessão Spark
    spark = criar_spark_session("bronze-layer-job")
    
    # Definir paths (ajuste conforme seu ambiente)
    BASE_PATH = "/home/claude/datalake"  # Ajuste este path
    
    RAW_CLIMA = "/mnt/user-data/uploads/INMET_SE_SP_A771_SAO_PAULO_-_INTERLAGOS_01-01-2025_A_31-12-2025.CSV"
    RAW_CULTURAS = "/mnt/user-data/uploads/Culturas_Agrícolas.csv"
    
    BRONZE_CLIMA = f"{BASE_PATH}/bronze/clima"
    BRONZE_CULTURAS = f"{BASE_PATH}/bronze/culturas"
    
    try:
        # Processar clima
        df_clima = processar_clima_bronze(spark, RAW_CLIMA, BRONZE_CLIMA)
        
        # Processar culturas
        df_culturas = processar_culturas_bronze(spark, RAW_CULTURAS, BRONZE_CULTURAS)
        
        print("\n" + "="*60)
        print("✅ BRONZE LAYER COMPLETA!")
        print("="*60)
        
    finally:
        spark.stop()
