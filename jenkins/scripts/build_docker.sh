#!/bin/bash
# ===========================================
# build_docker.sh - Build da imagem Docker
# ===========================================

set -e

echo "🐳 Construindo imagem Docker..."
echo ""

# Cores
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# Config
DOCKER_DIR="docker"
IMAGE_NAME="pipeline-clima-airflow"
IMAGE_TAG="latest"

# ===========================================
# 1. Verifica se Docker está disponível
# ===========================================
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker não está rodando${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker disponível"

# ===========================================
# 2. Build da imagem
# ===========================================
echo ""
echo "=== Construindo imagem ==="

cd "$DOCKER_DIR"

docker build \
    --tag ${IMAGE_NAME}:${IMAGE_TAG} \
    --file Dockerfile \
    ..

echo ""
echo -e "${GREEN}✓${NC} Imagem construída: ${IMAGE_NAME}:${IMAGE_TAG}"

# ===========================================
# 3. Lista imagem
# ===========================================
echo ""
echo "=== Imagem criada ==="
docker images | head -1
docker images | grep ${IMAGE_NAME} || true

echo ""
echo -e "${GREEN}═══════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Build concluído!                   ${NC}"
echo -e "${GREEN}═══════════════════════════════════════${NC}"
