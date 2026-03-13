"""
Loader: S3 Bronze → PostgreSQL
================================

Lê os arquivos Parquet da camada bronze no S3 e carrega
no PostgreSQL criando o schema `bronze` com as tabelas:
  - bronze.clima
  - bronze.culturas

Uso:
    python -m src.load.targets.postgres
    python -m src.load.targets.postgres --tabela clima
    python -m src.load.targets.postgres --tabela culturas
"""

import os
import argparse
import logging
import boto3
import pandas as pd
from io import BytesIO, StringIO
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# =========================
# CONFIGURAÇÕES
# =========================

BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-lab1")

PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = os.getenv("PG_PORT",     "5433")
PG_USER     = os.getenv("PG_USER",     "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")
PG_DB       = os.getenv("PG_DB",       "datalake")

S3_PREFIXES = {
    "clima":    "bronze/clima/",
    "culturas": "bronze/culturas/",
}


# =========================
# CONEXÃO POSTGRESQL
# =========================

def criar_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(url)
    logger.info(f"Conectado ao PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    return engine


def criar_schema(engine, schema: str = "bronze"):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    logger.info(f"Schema '{schema}' pronto.")


# =========================
# LEITURA DO S3
# =========================

def listar_arquivos_s3(prefixo: str) -> list:
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefixo)
    arquivos = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    logger.info(f"Encontrados {len(arquivos)} arquivo(s) em s3://{BUCKET_NAME}/{prefixo}")
    return arquivos


def ler_parquet_s3(chave: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=chave)
    buffer = BytesIO(obj["Body"].read())
    return pd.read_parquet(buffer)


def carregar_todos_parquets(prefixo: str) -> pd.DataFrame:
    arquivos = listar_arquivos_s3(prefixo)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo Parquet em: s3://{BUCKET_NAME}/{prefixo}")

    dfs = []
    for i, chave in enumerate(arquivos, 1):
        logger.info(f"  Lendo {i}/{len(arquivos)}: {chave}")
        dfs.append(ler_parquet_s3(chave))

    df_final = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total de registros lidos: {len(df_final):,}")
    return df_final


# =========================
# CARGA NO POSTGRESQL
# =========================

def carregar_no_postgres(df: pd.DataFrame, tabela: str, engine, schema: str = "bronze"):
    total = len(df)
    logger.info(f"Carregando {total:,} registros em {schema}.{tabela}...")
    # Create table structure first (empty, no data)
    with engine.begin() as conn:
        df.head(0).to_sql(name=tabela, con=conn, schema=schema, if_exists="replace", index=False)
    # Use PostgreSQL COPY in chunks to avoid OOM with large datasets
    chunk_size = 500_000
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            buffer = StringIO()
            chunk.to_csv(buffer, index=False, header=False, na_rep="")
            buffer.seek(0)
            cursor.copy_expert(f"COPY {schema}.{tabela} FROM STDIN WITH (FORMAT CSV, NULL '')", buffer)
            logger.info(f"  Inseridos {min(i + chunk_size, total):,}/{total:,} registros")
        cursor.close()
        raw_conn.commit()
    finally:
        raw_conn.close()
    logger.info(f"✅ {schema}.{tabela} carregada com sucesso!")


# =========================
# FUNÇÕES PRINCIPAIS
# =========================

def carregar_clima(engine):
    logger.info("CARREGANDO: S3 bronze/clima → PostgreSQL bronze.clima")
    df = carregar_todos_parquets(S3_PREFIXES["clima"])
    logger.info(f"Colunas: {list(df.columns)}")
    carregar_no_postgres(df, tabela="clima", engine=engine)


def carregar_culturas(engine):
    logger.info("CARREGANDO: S3 bronze/culturas → PostgreSQL bronze.culturas")
    df = carregar_todos_parquets(S3_PREFIXES["culturas"])
    logger.info(f"Colunas: {list(df.columns)}")
    carregar_no_postgres(df, tabela="culturas", engine=engine)


# =========================
# ENTRY POINT
# =========================

def main():
    parser = argparse.ArgumentParser(description="Loader S3 Bronze → PostgreSQL")
    parser.add_argument(
        "--tabela",
        choices=["clima", "culturas", "todas"],
        default="todas",
        help="Qual tabela carregar (padrão: todas)"
    )
    args = parser.parse_args()

    engine = criar_engine()
    criar_schema(engine, schema="bronze")

    if args.tabela in ("clima", "todas"):
        carregar_clima(engine)
    if args.tabela in ("culturas", "todas"):
        carregar_culturas(engine)

    logger.info("CARGA CONCLUÍDA! Próximo passo: cd dbt && dbt run")


if __name__ == "__main__":
    main()
