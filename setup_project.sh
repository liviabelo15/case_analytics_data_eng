#!/bin/bash

set -e
# =====================================================================
# CONFIGURAÇÃO: Altere a URL abaixo para o repositório que deseja usar
# =====================================================================
REPO_URL="https://github.com/liviabelo15/case_analytics_data_eng.git"

# Extrai o nome da pasta a partir da URL do Git
REPO_NAME=$(basename "$REPO_URL" .git)

echo "Iniciando automação de configuração do projeto Python..."

# 1. Verifica se o Git está instalado
if ! command -v git &> /dev/null; then
    echo "❌ Erro: O Git não está instalado ou não foi encontrado no PATH."
    exit 1
fi

# 2. Verifica se o Python 3 está instalado
if ! command -v python3 &> /dev/null; then
    echo "❌ Erro: O Python 3 não está instalado ou não foi encontrado no PATH."
    exit 1
fi

# 3. Clona o repositório do GitHub (se a pasta já não existir)
if [ ! -d "$REPO_NAME" ]; then
    echo "Clonando o repositório: $REPO_URL..."
    git clone "$REPO_URL"
    cd "$REPO_NAME"
else
    echo "A pasta '$REPO_NAME' já existe. Acessando o diretório existente..."
    cd "$REPO_NAME"
fi

# 4. Cria o Ambiente Virtual (venv)
echo "Criando o ambiente virtual local (pasta 'venv')..."
python3 -m venv venv

# 5. Ativa o Ambiente Virtual
echo "Ativando o ambiente virtual..."
source venv/bin/activate

# 6. Atualiza o gerenciador de pacotes (pip)
echo "Atualizando o pip..."
pip install --upgrade pip

# 7. Instala as dependências listadas no requirements.txt
if [ -f "requirements.txt" ]; then
    echo "📥 Instalando bibliotecas do requirements.txt..."
    pip install -r requirements.txt
    echo "✨ Todas as bibliotecas foram instaladas perfeitamente!"
else
    echo "⚠️ Aviso: O arquivo 'requirements.txt' não foi encontrado na raiz do projeto."
fi

echo "--------------------------------------------------------"
echo "🎉 Tudo pronto! O ambiente de desenvolvimento está configurado."
echo "👉 Para começar, a rodar os códigos Python, usaremos os comandos:"
echo "   cd $REPO_NAME"
echo "   source venv/bin/activate"
echo "--------------------------------------------------------"

cd $REPO_NAME
source venv/bin/activate

echo "--------------------------------------------------------"
echo "Iniciando primeira etapa da Pipeline ELT: Extract"
echo "Arquivo: extract.py"
echo "--------------------------------------------------------"

#python extract.py

echo "--------------------------------------------------------"
echo "Iniciando segunda e terceira etapa da Pipeline ELT: Load and Transform"
echo "Arquivo: load_transform.py"
echo "--------------------------------------------------------"

#python load_transform.py

echo "--------------------------------------------------------"
echo "Iniciando construção de API"
echo "Arquivo: api.py"
echo "--------------------------------------------------------"

#python api.py
