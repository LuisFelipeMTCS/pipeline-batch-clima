from utils.ingest import ingest_data

if __name__ == "__main__":
    ingest_data()
    print("Ingestão concluída! Arquivo enviado para S3 Bronze.")
