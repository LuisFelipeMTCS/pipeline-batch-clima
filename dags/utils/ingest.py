import os
import io
import pandas as pd
import logging
import tempfile
import boto3

from dotenv import load_dotenv
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =========================
# CONFIG
# =========================

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = os.getenv("FOLDER_ID")  # pasta INMET
BUCKET_NAME = "datalake-lab1"
AWS_REGION = "sa-east-1"

METRIC_NAMESPACE = "inmet_bronze_lab"

# =========================
# LOG LOCAL (console)
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("inmet_ingestion")

# =========================
# CLOUDWATCH METRICS
# =========================

def put_metric(cloudwatch, metric_name, value=1):
    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": metric_name,
                "Value": value,
                "Unit": "Count",
                "Timestamp": datetime.utcnow()
            }
        ]
    )

# =========================
# GOOGLE DRIVE
# =========================

def authenticate_drive():
    creds_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../keys/credentials.json")
    )

    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=SCOPES
    )

    return build("drive", "v3", credentials=creds)


def list_year_folders(service, parent_folder_id):
    query = (
        f"'{parent_folder_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder'"
    )

    folders = []
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        folders.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return folders


def list_csv_files(service, folder_id):
    query = (
        f"'{folder_id}' in parents "
        "and name contains '.csv'"
    )

    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return files


def download_csv(service, file_id, filename):
    tmp_dir = tempfile.gettempdir()
    filepath = os.path.join(tmp_dir, filename)

    request = service.files().get_media(fileId=file_id)

    with io.FileIO(filepath, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    return filepath

# =========================
# INGESTÃO
# =========================

def ingest_data():
    logger.info("START_INGESTION")

    service = authenticate_drive()
    year_folders = list_year_folders(service, FOLDER_ID)

    if not year_folders:
        logger.warning("NO_YEAR_FOLDER_FOUND")
        return "Nenhuma pasta de ano encontrada."

    s3 = boto3.client("s3")
    cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)

    total_sucesso = 0
    total_erros = 0

    for year_folder in year_folders:
        year_name = year_folder["name"]
        year_id = year_folder["id"]

        logger.info(f"PROCESSING_YEAR year/{year_name}")

        csv_files = list_csv_files(service, year_id)

        if not csv_files:
            logger.warning(f"NO_FILES_FOUND year/{year_name}")
            continue

        for file in csv_files:
            filename = file["name"]

            try:
                path = download_csv(service, file["id"], filename)

                df = pd.read_csv(
                    path,
                    sep=";",
                    encoding="latin1",
                    skiprows=8,
                    on_bad_lines="skip"
                )

                parquet_name = f"{os.path.splitext(filename)[0]}.parquet"
                parquet_path = os.path.join(tempfile.gettempdir(), parquet_name)

                df.to_parquet(parquet_path, engine="pyarrow", index=False)

                s3_key = f"bronze/inmet/year/{year_name}/{parquet_name}"
                s3.upload_file(parquet_path, BUCKET_NAME, s3_key)

                logger.info(f"INGEST_SUCCESS year/{year_name} file={filename}")
                total_sucesso += 1
                os.remove(parquet_path)

                # 📊 métrica em tempo real
                put_metric(cloudwatch, "ArquivoProcessado")

            except Exception:
                logger.exception(
                    f"INGEST_ERROR year/{year_name} file={filename}"
                )
                total_erros += 1

                # 🚨 métrica de erro em tempo real
                put_metric(cloudwatch, "ArquivoComErro")

    # =========================
    # MÉTRICAS FINAIS (RESUMO)
    # =========================

    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": "ArquivosIngeridosTotal",
                "Value": total_sucesso,
                "Unit": "Count"
            },
            {
                "MetricName": "ArquivosComErroTotal",
                "Value": total_erros,
                "Unit": "Count"
            }
        ]
    )

    resumo = f"{total_sucesso} sucesso | {total_erros} erro(s)"
    logger.info(f"FINAL {resumo}")

    return resumo


if __name__ == "__main__":
    print(ingest_data())
