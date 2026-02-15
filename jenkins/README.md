# Jenkins CI/CD - 100% Docker

## 📋 O que é isso?

Sistema de CI/CD que roda **tudo dentro do Docker**, sem precisar instalar Python ou qualquer outra coisa na sua máquina.

## 🏗️ Estrutura

```
jenkins/
├── Jenkinsfile           # Pipeline (receita do CI/CD)
├── docker-compose.yml    # Sobe o Jenkins
└── scripts/
    ├── run_tests.sh      # Roda pytest no Docker
    ├── build_docker.sh   # Build da imagem
    └── deploy.sh         # Deploy no Airflow

tests/
├── test_validation.py    # Testes de validação
├── test_ingestion.py     # Testes de ingestão
└── test_dags.py          # Testes das DAGs

pytest.ini                # Config do pytest
```

## 🚀 Como Instalar

### Passo 1: Subir o Jenkins

```bash
cd jenkins
docker-compose up -d
```

### Passo 2: Pegar a senha inicial

```bash
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

### Passo 3: Acessar o Jenkins

1. Abra http://localhost:8081
2. Cole a senha
3. Clique "Install suggested plugins"
4. Crie usuário admin

### Passo 4: Criar o Pipeline

1. **New Item** → Nome: `pipeline-batch-clima` → **Pipeline** → OK

2. Em **Pipeline**:
   - Definition: `Pipeline script from SCM`
   - SCM: `Git`
   - Repository URL: `https://github.com/SEU_USUARIO/pipeline-batch-clima.git`
   - Branch: `*/main`
   - Script Path: `jenkins/Jenkinsfile`

3. **Save**

### Passo 5: Rodar!

Clique em **Build Now**

## 🧪 Rodar Testes Localmente

Você pode rodar os testes sem o Jenkins:

```bash
# Na raiz do projeto
./jenkins/scripts/run_tests.sh
```

## 📊 O que o Pipeline faz

```
┌─────────────────────────────────────────┐
│            CI (sempre roda)             │
├─────────────────────────────────────────┤
│ 1. Checkout    → Baixa código           │
│ 2. Lint        → Verifica sintaxe       │
│ 3. Tests       → Roda pytest            │
└─────────────────────────────────────────┘
                    │
                    │ (se branch = main)
                    ▼
┌─────────────────────────────────────────┐
│            CD (só na main)              │
├─────────────────────────────────────────┤
│ 4. Build       → Constrói imagem Docker │
│ 5. Deploy      → docker-compose up      │
│ 6. Health      → Verifica se funciona   │
└─────────────────────────────────────────┘
```

## ❓ Problemas Comuns

### Jenkins não acessa Docker

```bash
docker exec -u root jenkins chmod 666 /var/run/docker.sock
```

### Rede não encontrada

Se der erro de rede, primeiro suba o Airflow:
```bash
cd docker
docker-compose up -d
```

Depois suba o Jenkins:
```bash
cd jenkins
docker-compose up -d
```

## 🔗 URLs

| Serviço | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| Jenkins | http://localhost:8081 |
