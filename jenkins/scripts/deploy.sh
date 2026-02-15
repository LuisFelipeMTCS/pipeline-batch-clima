#!/bin/bash
# ===========================================
# deploy.sh - Deploy com docker-compose
# ===========================================

set -e

echo "🚀 Iniciando deploy..."
echo ""

# Cores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Config
DOCKER_DIR="docker"

# ===========================================
# 1. Verifica pré-requisitos
# ===========================================
echo "=== Verificando pré-requisitos ==="

if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker não está rodando${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker disponível"

if ! command -v docker-compose &> /dev/null; then
    # Tenta docker compose (v2)
    if ! docker compose version &> /dev/null; then
        echo -e "${RED}❌ Docker Compose não encontrado${NC}"
        exit 1
    fi
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi
echo -e "${GREEN}✓${NC} Docker Compose disponível"

echo ""

# ===========================================
# 2. Para containers existentes
# ===========================================
echo "=== Parando containers existentes ==="

cd "$DOCKER_DIR"

$COMPOSE_CMD down --remove-orphans 2>/dev/null || true
echo -e "${GREEN}✓${NC} Containers parados"

echo ""

# ===========================================
# 3. Sobe containers
# ===========================================
echo "=== Subindo containers ==="

$COMPOSE_CMD up -d --build

echo -e "${GREEN}✓${NC} Containers iniciados"
echo ""

# ===========================================
# 4. Status
# ===========================================
echo "=== Status dos containers ==="
$COMPOSE_CMD ps

echo ""
echo -e "${GREEN}═══════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Deploy concluído!                  ${NC}"
echo -e "${GREEN}═══════════════════════════════════════${NC}"
echo ""
echo "Airflow: http://localhost:8080"
echo "Jenkins: http://localhost:8081"
