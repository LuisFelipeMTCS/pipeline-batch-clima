#!/bin/bash
# ===========================================
# run_tests.sh - Executa pytest no Docker
# ===========================================
# 
# Este script roda os testes DENTRO de um container Docker,
# então não precisa de Python instalado na máquina.
#

set -e

echo "🧪 Rodando testes no Docker..."
echo ""

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# ===========================================
# Roda pytest dentro de um container
# ===========================================

docker run --rm \
    -v "${PWD}:/app" \
    -w /app \
    python:3.10-slim \
    bash -c "
        echo '=== Instalando dependências ==='
        pip install --quiet pandas pyarrow pytest boto3 google-auth google-api-python-client apache-airflow
        
        echo ''
        echo '=== Rodando testes ==='
        export PYTHONPATH=/app
        
        pytest tests/ \
            -v \
            --tb=short \
            --junitxml=test-results.xml \
            || exit \$?
        
        echo ''
        echo '✅ Testes concluídos!'
    "

# Verifica resultado
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════${NC}"
    echo -e "${GREEN}  ✅ Todos os testes passaram!         ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════${NC}"
else
    echo ""
    echo -e "${RED}═══════════════════════════════════════${NC}"
    echo -e "${RED}  ❌ Alguns testes falharam!            ${NC}"
    echo -e "${RED}═══════════════════════════════════════${NC}"
    exit 1
fi
