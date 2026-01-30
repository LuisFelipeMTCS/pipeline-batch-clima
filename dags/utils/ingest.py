import os
import pandas as pd
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID")

logger = logging.getLogger("airflow.task")

def authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(
        os.path.join(os.path.dirname(__file__), '../../keys/credentials.json')
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def list_csv_files(service, folder_id):
    query = f"'{folder_id}' in parents and name contains '.csv'"
    results = service.files().list(q=query, pageSize=1000, fields="files(id, name)").execute()
    return results.get('files', [])

def download_csv(service, file_id, filename):
    request = service.files().get_media(fileId=file_id)
    filepath = os.path.join("/tmp", filename)
    with open(filepath, "wb") as f:
        f.write(request.execute())
    return filepath

def ingest_data():
    service = authenticate_drive()
    files = list_csv_files(service, FOLDER_ID)

    if not files:
        logger.info("Nenhum arquivo encontrado no Drive.")
        return "Nenhum arquivo encontrado."

    total = 0
    for file in files:
        logger.info(f"Baixando: {file['name']}")
        path = download_csv(service, file['id'], file['name'])

        try:
            df = pd.read_csv(
                path,
                sep=None,
                engine="python",
                encoding="latin1",
                skiprows=8,
                on_bad_lines="skip"
            )

            original_name = os.path.splitext(file['name'])[0] + ".parquet"
            parquet_path = f"/tmp/{original_name}"

            df.to_parquet(parquet_path, engine="pyarrow", index=False)
            logger.info(f"Parquet gerado em: {parquet_path}")

            s3 = S3Hook(aws_conn_id="aws_default")
            s3.load_file(
                filename=parquet_path,
                key=f"bronze/inmet/2025/{original_name}",
                bucket_name="datalake-lab1",
                replace=True
            )
            logger.info(f"Upload para S3 concluído: bronze/inmet/2025/{original_name}")
            total += 1

        except Exception as e:
            logger.error(f"Erro ao processar {file['name']}: {e}")

    return f"{total} arquivos processados e enviados para S3"
