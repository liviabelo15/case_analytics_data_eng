#!/bin/bash
# =============================================================================
# setup_project.sh — Configuração e execução do pipeline Casa dos Ventos
#
# O que este script faz:
#   1. Clona o repositório (se necessário)
#   2. Cria o ambiente virtual e instala as dependências
#   3. Executa o pipeline completo: extract → load_transform → model
#   4. Exibe as instruções para iniciar a API
#
# Uso:
#   bash setup_project.sh
# =============================================================================

set -e  # Para na primeira falha

REPO_URL="https://github.com/liviabelo15/case_analytics_data_eng.git"
REPO_NAME=$(basename "$REPO_URL" .git)

# --- Cores para output ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # sem cor

log_step()  { echo -e "\n${BLUE}▶ $1${NC}"; }
log_ok()    { echo -e "${GREEN}✔ $1${NC}"; }
log_warn()  { echo -e "${YELLOW}⚠ $1${NC}"; }
log_error() { echo -e "${RED}✘ $1${NC}"; }

# =============================================================================
# 1. Verificações de dependências
# =============================================================================
log_step "Verificando dependências do sistema..."

if ! command -v git &> /dev/null; then
    log_error "Git não encontrado. Instale em: https://git-scm.com"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    log_error "Python 3 não encontrado. Instale em: https://python.org"
    exit 1
fi

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
log_ok "Git $(git --version | cut -d' ' -f3) e Python $PYTHON_VERSION encontrados."

# =============================================================================
# 2. Clone do repositório
# =============================================================================
log_step "Configurando o repositório..."

# Verifica se o script está sendo rodado de DENTRO do próprio repositório
if [ -f "src/extract.py" ] && [ -f "requirements.txt" ]; then
    log_ok "Repositório já presente. Usando o diretório atual."
    PROJECT_DIR="."
elif [ -d "$REPO_NAME" ]; then
    log_warn "Pasta '$REPO_NAME' já existe. Usando a existente."
    PROJECT_DIR="$REPO_NAME"
else
    log_step "Clonando $REPO_URL..."
    git clone "$REPO_URL"
    PROJECT_DIR="$REPO_NAME"
fi

cd "$PROJECT_DIR"

# =============================================================================
# 3. Ambiente virtual
# =============================================================================
log_step "Criando ambiente virtual..."

if [ ! -d "venv" ]; then
    python3 -m venv venv
    log_ok "Ambiente virtual criado em ./venv"
else
    log_ok "Ambiente virtual já existe. Reutilizando."
fi

PYTHON="venv/bin/python3"
PIP="venv/bin/pip"

log_step "Instalando dependências..."
$PIP install --upgrade pip --quiet
$PIP install -r requirements.txt --quiet
log_ok "Dependências instaladas com sucesso."

# =============================================================================
# 4. Pipeline ELT
# =============================================================================
echo ""
echo "============================================================"
echo "  EXECUTANDO PIPELINE"
echo "============================================================"

# Etapa 1: Extração
log_step "[1/3] Extração — baixando dados do ONS (S3)..."
$PYTHON src/extract.py
log_ok "Extração concluída. Arquivos salvos em data/raw/"

# Etapa 2: Transformação e Qualidade
log_step "[2/3] Transformação e Qualidade — validando e unindo datasets..."
$PYTHON src/load_transform.py
log_ok "Transformação concluída. Silver layer em data/modeled/"
log_ok "Relatório de qualidade gerado: relatorio_qualidade.json"

# Etapa 3: Modelagem Dimensional
log_step "[3/3] Modelagem Dimensional — construindo Star Schema Gold..."
$PYTHON src/model.py
log_ok "Modelagem concluída. Banco em data/warehouse/cv_case.db"

# =============================================================================
# 5. Instruções finais
# =============================================================================
echo ""
echo "============================================================"
echo -e "${GREEN}  PIPELINE CONCLUÍDO COM SUCESSO${NC}"
echo "============================================================"
echo ""
echo "Para iniciar a API REST, execute:"
echo ""
echo -e "  ${YELLOW}source venv/bin/activate${NC}"
echo -e "  ${YELLOW}uvicorn src.api:app --reload${NC}"
echo ""
echo "  Acesse a documentação interativa em: http://127.0.0.1:8000/docs"
echo ""
