-- Cria o banco de dados do pipeline (dados do datalake)
-- O banco "airflow" já é criado automaticamente pelo POSTGRES_DB=airflow
CREATE DATABASE datalake;
GRANT ALL PRIVILEGES ON DATABASE datalake TO airflow;
