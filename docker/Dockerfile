FROM apache/airflow:2.7.3-python3.10

USER root

# Instalar build tools E Java (necessário para PySpark driver)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Configurar JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Baixar JARs do Hadoop-AWS para suporte S3 no PySpark driver
RUN mkdir -p /home/airflow/.ivy2/jars && \
    curl -o /home/airflow/.ivy2/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /home/airflow/.ivy2/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
