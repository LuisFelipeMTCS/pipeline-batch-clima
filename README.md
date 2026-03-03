# pipeline-batch-clima

Pipeline de dados batch para ingestão e transformação de dados meteorológicos, agrícolas e de solos brasileiros.

## 📋 Sobre o Projeto

Este projeto implementa um pipeline de dados batch usando Apache Airflow para processar dados de:
- **Clima**: Dados meteorológicos do INMET (Instituto Nacional de Meteorologia)
- **Agricultura**: Dados agrícolas do IBGE
- **Solos**: Dados de características de solos

## 🏗️ Arquitetura

O pipeline segue a arquitetura Medallion (Bronze → Silver → Gold):

```
┌────────────────┐      ┌──────────────┐      ┌──────────────────┐      ┌──────────────┐
│    Fontes      │ ──>  │   Ingestão   │ ──>  │  Transformação   │ ──>  │     Load     │
│  (APIs/GDrive) │      │    (RAW)     │      │ (Bronze/Silver/  │      │ (S3/Postgres)│
│                │      │              │      │      Gold)       │      │              │
└────────────────┘      └──────────────┘      └──────────────────┘      └──────────────┘
```

### Camadas:
- **RAW**: Dados brutos armazenados em S3
- **Bronze**: Dados limpos e estruturados (Spark)
- **Silver**: Dados agregados e enriquecidos (Spark)
- **Gold**: Dados prontos para análise (dbt)

## 🚀 Começando

### Pré-requisitos

- Python 3.9+
- Docker e Docker Compose
- Apache Airflow 2.x
- Apache Spark 3.5.0

### Instalação

1. Clone o repositório:
```bash
git clone https://github.com/LuisFelipeMTCS/pipeline-batch-clima.git
cd pipeline-batch-clima
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Configure as variáveis de ambiente (crie um arquivo `.env`):
```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your_bucket
```

4. Suba o ambiente com Docker:
```bash
cd docker
docker-compose up -d
```

## 📊 DAGs Disponíveis

### DAGs de Ingestão:
- `pipeline_clima`: Ingestão de dados meteorológicos do INMET
- `pipeline_solos`: Ingestão de dados de solos
- `pipeline_agricultura`: Ingestão de dados agrícolas do IBGE

### DAGs de Transformação:
- `transform_bronze_dag`: Processamento para camada Bronze
- `transform_silver_dag`: Processamento para camada Silver
- `transform_gold_dag`: Processamento para camada Gold

## 🧪 Testes

Execute os testes com pytest:

```bash
pytest tests/ -v
```

## 📁 Estrutura do Projeto

```
pipeline-batch-clima/
├── dags/                    # DAGs do Airflow
├── src/
│   ├── ingestion/          # Módulos de ingestão
│   ├── transformation/     # Módulos de transformação (Spark)
│   ├── load/               # Módulos de carga (S3/Postgres)
│   ├── validation/         # Regras de validação de dados
│   ├── config/             # Configurações YAML
│   └── common/             # Utilitários compartilhados
├── dbt/                    # Modelos dbt
├── tests/                  # Testes unitários
├── docker/                 # Configuração Docker
└── jenkins/                # Pipeline CI/CD

```

## 🔍 Observabilidade

O projeto inclui observabilidade integrada com:
- **CloudWatch**: Logs e métricas
- **SNS**: Alertas de sucesso/erro
- **Métricas customizadas**: Taxa de processamento, volumes de dados, etc.

## 📖 Documentação Adicional

- [Jenkins CI/CD](jenkins/README.md): Guia de configuração do pipeline CI/CD

## 🤝 Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT.

## 👤 Autor

**Luis Felipe Toledo**
- GitHub: [@LuisFelipeMTCS](https://github.com/LuisFelipeMTCS)
- Email: luisfelipetoledo.profissional@gmail.com
