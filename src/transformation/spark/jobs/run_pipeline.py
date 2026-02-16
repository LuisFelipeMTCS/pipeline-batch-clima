"""
PIPELINE COMPLETO - Arquitetura Medalhão
========================================

Este script executa todo o pipeline de transformação:
RAW → BRONZE → SILVER → GOLD

FLUXO DE DADOS:
---------------

    ┌─────────────────────────────────────────────────────────────────┐
    │                         📁 RAW (CSV)                            │
    │  ┌─────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
    │  │ clima.csv   │  │ culturas.csv    │  │ solos.csv            │ │
    │  │ (INMET)     │  │ (Agrícolas)     │  │ (IBGE)               │ │
    │  └──────┬──────┘  └────────┬────────┘  └──────────────────────┘ │
    └─────────┼──────────────────┼────────────────────────────────────┘
              │                  │
              ▼                  ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                       🥉 BRONZE (Parquet)                        │
    │  ┌─────────────┐  ┌─────────────────┐                           │
    │  │ clima/      │  │ culturas/       │                           │
    │  │ - padroniz. │  │ - temp extraída │                           │
    │  │ - tipos ok  │  │ - tolerâncias   │                           │
    │  └──────┬──────┘  └────────┬────────┘                           │
    └─────────┼──────────────────┼────────────────────────────────────┘
              │                  │
              ▼                  ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                       🥈 SILVER (Parquet)                        │
    │  ┌─────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
    │  │ clima_      │  │ clima_          │  │ culturas_            │ │
    │  │ diario/     │  │ estacional/     │  │ enriquecidas/        │ │
    │  │             │  │ (4 estações)    │  │                      │ │
    │  └─────────────┘  └────────┬────────┘  └──────────┬───────────┘ │
    └────────────────────────────┼───────────────────────┼────────────┘
                                 │                       │
                                 └───────────┬───────────┘
                                             │
                                             ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                        🥇 GOLD (Parquet)                         │
    │  ┌─────────────────────────────────────────────────────────┐    │
    │  │                 fato_risco_climatico/                    │    │
    │  │  ┌────────┬────────┬───────┬───────┬──────────────────┐ │    │
    │  │  │Cultura │Estação │Score  │Risco  │Recomendação      │ │    │
    │  │  ├────────┼────────┼───────┼───────┼──────────────────┤ │    │
    │  │  │Café    │Inverno │45     │ALTO   │⚠️ EVITAR         │ │    │
    │  │  │Milho   │Verão   │95     │BAIXO  │✅ IDEAL          │ │    │
    │  │  │...     │...     │...    │...    │...               │ │    │
    │  │  └────────┴────────┴───────┴───────┴──────────────────┘ │    │
    │  └─────────────────────────────────────────────────────────┘    │
    └─────────────────────────────────────────────────────────────────┘

COMO EXECUTAR:
--------------
    cd pipeline-batch-clima
    python -m src.transformation.spark.jobs.run_pipeline

OU com spark-submit:
    spark-submit src/transformation/spark/jobs/run_pipeline.py
"""

import os
import sys
from datetime import datetime

# Adicionar path do projeto
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.insert(0, PROJECT_ROOT)

from src.transformation.spark.spark_session import criar_spark_session
from src.transformation.spark.jobs.bronze_job import (
    processar_clima_bronze,
    processar_culturas_bronze
)
from src.transformation.spark.jobs.silver_job import (
    criar_dim_tempo,
    agregar_clima_diario,
    agregar_clima_mensal,
    agregar_clima_estacional,
    enriquecer_culturas
)
from src.transformation.spark.jobs.gold_job import (
    gerar_fato_risco_climatico,
    gerar_relatorio_resumido,
    exibir_resposta_pergunta
)


def print_header(titulo: str, emoji: str = "🚀"):
    """Imprime cabeçalho formatado."""
    print("\n")
    print("╔" + "═"*70 + "╗")
    print(f"║  {emoji} {titulo:<65} ║")
    print("╚" + "═"*70 + "╝")


def print_step(numero: int, titulo: str):
    """Imprime passo do pipeline."""
    print(f"\n{'─'*70}")
    print(f"  PASSO {numero}: {titulo}")
    print(f"{'─'*70}")


def run_pipeline(
    raw_clima_path: str,
    raw_culturas_path: str,
    datalake_path: str
):
    """
    Executa o pipeline completo de transformação.
    
    Parameters:
        raw_clima_path: Caminho do CSV de clima (INMET)
        raw_culturas_path: Caminho do CSV de culturas agrícolas
        datalake_path: Caminho base do datalake (onde serão salvos os dados)
    """
    inicio = datetime.now()
    
    print_header("PIPELINE MEDALHÃO - RISCO CLIMÁTICO AGRÍCOLA")
    print(f"\n📅 Iniciado em: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 Datalake: {datalake_path}")
    
    # Definir paths das camadas
    paths = {
        # Bronze
        "bronze_clima": f"{datalake_path}/bronze/clima",
        "bronze_culturas": f"{datalake_path}/bronze/culturas",
        
        # Silver
        "silver_dim_tempo": f"{datalake_path}/silver/dim_tempo",
        "silver_clima_diario": f"{datalake_path}/silver/clima_diario",
        "silver_clima_mensal": f"{datalake_path}/silver/clima_mensal",
        "silver_clima_estacional": f"{datalake_path}/silver/clima_estacional",
        "silver_culturas": f"{datalake_path}/silver/culturas",
        
        # Gold
        "gold_fato_risco": f"{datalake_path}/gold/fato_risco_climatico",
        "gold_relatorios": f"{datalake_path}/gold/relatorios",
    }
    
    # Criar diretórios
    for layer in ["bronze", "silver", "gold"]:
        os.makedirs(f"{datalake_path}/{layer}", exist_ok=True)
    
    # Criar sessão Spark
    spark = criar_spark_session("pipeline-medalhao-completo")
    
    try:
        # =========================================================
        # CAMADA BRONZE
        # =========================================================
        print_header("CAMADA BRONZE", "🥉")
        
        print_step(1, "Processando dados de CLIMA (INMET)")
        df_clima_bronze = processar_clima_bronze(
            spark, raw_clima_path, paths["bronze_clima"]
        )
        
        print_step(2, "Processando dados de CULTURAS AGRÍCOLAS")
        df_culturas_bronze = processar_culturas_bronze(
            spark, raw_culturas_path, paths["bronze_culturas"]
        )
        
        # =========================================================
        # CAMADA SILVER
        # =========================================================
        print_header("CAMADA SILVER", "🥈")
        
        print_step(3, "Criando DIMENSÃO TEMPO")
        df_clima_bronze = spark.read.parquet(paths["bronze_clima"])
        criar_dim_tempo(spark, df_clima_bronze, paths["silver_dim_tempo"])
        
        print_step(4, "Agregando clima DIÁRIO")
        agregar_clima_diario(spark, paths["bronze_clima"], paths["silver_clima_diario"])
        
        print_step(5, "Agregando clima MENSAL")
        agregar_clima_mensal(spark, paths["silver_clima_diario"], paths["silver_clima_mensal"])
        
        print_step(6, "Agregando clima ESTACIONAL")
        agregar_clima_estacional(spark, paths["silver_clima_mensal"], paths["silver_clima_estacional"])
        
        print_step(7, "Enriquecendo dados de CULTURAS")
        enriquecer_culturas(spark, paths["bronze_culturas"], paths["silver_culturas"])
        
        # =========================================================
        # CAMADA GOLD
        # =========================================================
        print_header("CAMADA GOLD", "🥇")
        
        print_step(8, "Gerando FATO_RISCO_CLIMATICO")
        df_risco = gerar_fato_risco_climatico(
            spark,
            paths["silver_clima_estacional"],
            paths["silver_culturas"],
            paths["gold_fato_risco"]
        )
        
        print_step(9, "Gerando RELATÓRIOS")
        gerar_relatorio_resumido(spark, paths["gold_fato_risco"], paths["gold_relatorios"])
        
        # =========================================================
        # RESULTADO FINAL
        # =========================================================
        print_header("RESULTADO FINAL", "🎯")
        exibir_resposta_pergunta(spark, paths["gold_fato_risco"])
        
        # =========================================================
        # RESUMO
        # =========================================================
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        
        print("\n")
        print("╔" + "═"*70 + "╗")
        print("║" + " "*25 + "✅ PIPELINE CONCLUÍDO!" + " "*23 + "║")
        print("╠" + "═"*70 + "╣")
        print(f"║  ⏱️  Duração: {duracao:.1f} segundos{' '*(52-len(f'{duracao:.1f}'))}║")
        print(f"║  📁 Datalake: {datalake_path:<54}║")
        print("╠" + "═"*70 + "╣")
        print("║  📊 Dados gerados:                                                  ║")
        print("║     • bronze/clima/          - Dados climáticos padronizados       ║")
        print("║     • bronze/culturas/       - Culturas com tolerâncias            ║")
        print("║     • silver/clima_diario/   - Agregação diária                    ║")
        print("║     • silver/clima_mensal/   - Agregação mensal                    ║")
        print("║     • silver/clima_estacional/ - Agregação por estação             ║")
        print("║     • silver/culturas/       - Culturas enriquecidas               ║")
        print("║     • gold/fato_risco_climatico/ - TABELA PRINCIPAL                ║")
        print("║     • gold/relatorios/       - Rankings e distribuições            ║")
        print("╚" + "═"*70 + "╝")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()


# =========================================================
# EXECUÇÃO
# =========================================================
if __name__ == "__main__":
    # Paths padrão (ajuste conforme seu ambiente)
    RAW_CLIMA = os.environ.get(
        "RAW_CLIMA_PATH",
        "/mnt/user-data/uploads/INMET_SE_SP_A771_SAO_PAULO_-_INTERLAGOS_01-01-2025_A_31-12-2025.CSV"
    )
    
    RAW_CULTURAS = os.environ.get(
        "RAW_CULTURAS_PATH",
        "/mnt/user-data/uploads/Culturas_Agrícolas.csv"
    )
    
    DATALAKE_PATH = os.environ.get(
        "DATALAKE_PATH",
        "/home/claude/datalake"
    )
    
    # Executar pipeline
    sucesso = run_pipeline(RAW_CLIMA, RAW_CULTURAS, DATALAKE_PATH)
    
    sys.exit(0 if sucesso else 1)
