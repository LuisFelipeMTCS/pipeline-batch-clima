FROM apache/airflow:2.7.0

USER root

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instala dependências Python
RUN pip install --no-cache-dir \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-auth-httplib2 \
    pandas \
    pyarrow \
    boto3 \
    python-dotenv \
    certifi