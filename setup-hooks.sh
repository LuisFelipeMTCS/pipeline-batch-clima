#!/bin/bash
# Setup Git Hooks
# ===============
# Instala os hooks do Git para o desenvolvedor local

echo "🔧 Configurando Git Hooks..."
echo ""

# Copia o pre-push hook
cp .githooks/pre-push .git/hooks/pre-push
chmod +x .git/hooks/pre-push

echo "✅ Pre-push hook instalado!"
echo ""
echo "O que ele faz:"
echo "  - Roda os testes antes de cada push"
echo "  - Bloqueia o push se os testes falharem"
echo ""
echo "Para testar: git push origin main"
echo ""
