import os
import pandas as pd
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
import boto3
import tempfile
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID")
logger = logging.getLogger("local.test")

def authenticate_drive():
    creds_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../keys/credentials.json'))
    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def list_csv_files(service, folder_id):
    if not folder_id:
        raise ValueError("FOLDER_ID não definido. Configure no .env")
    query = f"'{folder_id}' in parents and name contains '.csv'"
    results = service.files().list(q=query, pageSize=1000, fields="files(id, name)").execute()
    return results.get('files', [])

def download_csv(service, file_id, filename):
    request = service.files().get_media(fileId=file_id)
    tmp_dir = tempfile.gettempdir()
    filepath = os.path.join(tmp_dir, filename)
    os.makedirs(tmp_dir, exist_ok=True)
    with open(filepath, "wb") as f:
        f.write(request.execute())
    return filepath

def ingest_data():
    service = authenticate_drive()
    files = list_csv_files(service, FOLDER_ID)

    if not files:
        logger.info("Nenhum arquivo encontrado no Drive.")
        return "Nenhum arquivo encontrado."

    s3 = boto3.client("s3")
    total_arquivos = 0
    total_linhas = 0
    tmp_dir = tempfile.gettempdir()

    for i, file in enumerate(files): 
        logger.info(f"Baixando: {file['name']}")
        path = download_csv(service, file['id'], file['name'])

        try:
            df = pd.read_csv(path, sep=None, engine="python", encoding="latin1", skiprows=8, on_bad_lines="skip")
            linhas = len(df)
            total_linhas += linhas

            parquet_name = f"{os.path.splitext(file['name'])[0]}.parquet"
            parquet_path = os.path.join(tmp_dir, parquet_name)
            df.to_parquet(parquet_path, engine="pyarrow", index=False)

            s3.upload_file(parquet_path, "datalake-lab1", f"bronze/inmet/year/2025/{parquet_name}")
            logger.info(f"Upload concluído: bronze/inmet/year/2025/{parquet_name} ({linhas} linhas)")
            total_arquivos += 1
        except Exception as e:
            logger.error(f"Erro ao processar {file['name']}: {e}")

    # Log final com resumo
    resumo = f"{total_arquivos} arquivos processados, {total_linhas} linhas enviadas para S3"
    logger.info(resumo)

    # Opcional: enviar métricas para CloudWatch
    cloudwatch = boto3.client("cloudwatch")
    cloudwatch.put_metric_data(
        Namespace="DataLake/Ingestao",
        MetricData=[
            {"MetricName": "ArquivosIngeridos", "Value": total_arquivos, "Unit": "Count"},
            {"MetricName": "LinhasIngeridas", "Value": total_linhas, "Unit": "Count"}
        ]
    )

    return resumo

if __name__ == "__main__":
    print(ingest_data())
