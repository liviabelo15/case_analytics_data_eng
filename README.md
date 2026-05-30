# 📑 Case Data Engineering & Analytics — Casa dos Ventos & ONS

Este repositório contém a solução completa para o pipeline de dados de alta performance focado na consolidação, auditoria de qualidade e modelagem analítica dos dados de **Restrição de Geração (*Constrained-off*) de Usinas Eólicas**. [cite_start]O ecossistema processa dados brutos disponibilizados pelo ONS e unifica-os com o mapeamento cadastral interno da Casa dos Ventos[cite: 1].

---

## 🏗️ 1. Arquitetura da Solução

O ecossistema foi desenhado seguindo a **Arquitetura de Medalhão** moderna baseada em um **Data Lakehouse local e sem servidor (Serverless)**, garantindo processamento vetorizado de alta velocidade e desacoplamento de infraestrutura rígida.

```text
  [ ONS API / Parquet ]       [ CSV Mestre CDV ]
            |                          |
            v                          v
   +---------------------------------------+
   |  Camada BRONZE (Ingestão Raw)        | -> Tabelas: raw_usinas, raw_usinas_detail
   +---------------------------------------+
            |
            v (SQL Otimizado via DuckDB)
   +---------------------------------------+
   |  Camada SILVER (Limpeza & Filtros)    | -> Tabelas: int_usinas_cv, int_cv_conj
   +---------------------------------------+
            |
            v (Validação Algorítmica & Contratos de Dados)
   +---------------------------------------+
   |  Camada GOLD (Modelagem Star Schema)  | -> Parquets: fato_geracao_restricao, dim_ativo_eolico, dim_tempo
   +---------------------------------------+
            |
            v
       [ FastAPI ] -> Camada de Serviço / Endpoint REST

```

### 🛠️ Stack Tecnológica Central

* **DuckDB:** Motor analítico colunar em memória para transformações SQL ultra rápidas de bases volumétricas.
* **Pandas & Apache Arrow:** Manipulação estruturada de dataframes e compartilhamento de memória com *Zero-Copy*.
* **Pandera:** Framework para aplicação de **Contratos de Dados (Data Contracts)** rígidos e validação estatística.
* **FastAPI & Uvicorn:** Camada de serviço de baixa latência para exposição dos dados modelados via API REST.
* **Parquet (Snappy):** Formato de armazenamento colunar otimizado para compressão de disco e leitura analítica rápida.

---

## 🧠 2. Premissas e Decisões de Design

### 🔹 Migração Estratégica: De Snowflake para Star Schema Supremo

Os dados originais do ONS seguem intrinsecamente um modelo normalizado (*Snowflake Schema*), no qual atributos de usinas, conjuntos operacionais e localizações geográficas encontram-se fragmentados e dependentes de múltiplos relacionamentos em cadeia.

Para otimizar o consumo por ferramentas de Business Intelligence (como Power BI e Looker) e queries de Analytics, o pipeline realiza uma **desnormalização proposital** na camada Gold, consolidando um **Star Schema** puro composto por apenas três tabelas de alto desempenho:

1. **`fato_geracao_restricao`**: Tabela central contendo as métricas de geração (MWmed), restrições, velocidade do vento e sinalizações de alertas.
2. **`dim_ativo_eolico`**: Dimensão totalmente achatada que serve como cadastro estável unificado de todos os níveis organizacionais (Projeto, Conjunto, SPE/Usina e Localização).
3. **`dim_tempo`**: Calendário analítico detalhado para quebras dinâmicas de séries temporais de 30 em 30 minutos.

### 🔹 Framework Híbrido de Qualidade: Hard Drop vs. Soft Drop vs. Flagging

Para garantir a confiabilidade cega dos relatórios executivos entregues à diretoria, foi implementada uma triagem tripla contra anomalias de dados:

* **Hard Drop (Descarte Crítico):** Registros duplicados na mesma chave composta ou linhas que violam o contrato estrutural mínimo exigido pelo Pandera são sumariamente expurgados da base útil para não distorcer agregações, gerando um registro descritivo no arquivo `relatorio_qualidade.json`.
* **Soft Drop (Anulação de Célula):** Medições físicas absurdas (ex: velocidade do vento negativa ou superior a 40 m/s, ou falha explícita de telemetria) são convertidas para `NULL`. Isso preserva o restante das informações financeiras/contábeis da linha sem invalidar o registro inteiro.
* **Flagging (Sinalização de Negócio):** Inconsistências de regras de negócio (como quando o volume de energia limitado/cortado pelo ONS sob uma restrição supera a própria geração de referência calculada para o ativo) são mantidas na base, mas recebem uma flag binária (`flg_alerta_limitada = 1`) para auditoria imediata pelo time de operações.

### 🔹 Governança Cadastral e Preservação de Ativos (Balanço de Massa)

Para evitar que usinas recém-comissionadas ou ativos sem geração reportada no mês sumissem dos filtros de mercado, a dimensão `dim_ativo_eolico` utiliza como âncora soberana o arquivo de sementes internas `spes_casa_dos_ventos.csv`. Através de um `LEFT JOIN` resiliente com a base de telemetria, assegura-se que a dimensão contenha perfeitamente a integridade de todos os **47 ativos eólicos cadastrados**, blindando a modelagem contra perdas ocultas de histórico.

---

## 💻 3. Instruções de Instalação e Configuração

### Pré-requisitos

* Python 3.10 ou superior instalado.

### 📦 1. Clonar o Repositório e Instalar Dependências

```bash
# Clone o repositório
git clone [https://github.com/seu-usuario/case_analytics_data_eng.git](https://github.com/seu-usuario/case_analytics_data_eng.git)
cd case_analytics_data_eng

# Crie e ative o ambiente virtual (Recomendado)
python -m venv venv
source venv/bin/activate  # No Windows use: venv\Scripts\activate

# Instale os pacotes necessários
pip install pandas pandera duckdb pyarrow fastapi uvicorn

```

---

## ⚡ 4. Sequência de Execução do Pipeline

O ecossistema foi modularizado para garantir total separação de escopos. Execute os comandos sempre a partir da **raiz do projeto**:

### Passo 1: Extração dos Dados Brutos (`Bronze`)

Copia e consolida os arquivos brutos fornecidos para as tabelas iniciais do banco de dados analítico.

```bash
python src/extract.py

```

### Passo 2: Transformação, Limpeza e Auditoria de Qualidade (`Silver`)

Aplica os cruzamentos lógicos baseados na regex de código CEG, valida os contratos de dados via Pandera, aplica as regras de Soft Drop/Flagging e gera o relatório consolidado de saúde da base (`relatorio_qualidade.json`) com testes de *Freshness*, *Completude* e *Gaps*.

```bash
python src/load_transform.py

```

### Passo 3: Modelagem Dimensional Gold (`Star Schema`)

Lê a base estruturada e quebra as informações no formato de estrela analítica, gerando as tabelas e arquivos parquets indexados de alta performance. Exibe no terminal o dicionário de dados e uma amostragem física de validação da `dim_ativo_eolico`.

```bash
python src/model.py

```

---

## 🔍 5. Scripts de Diagnóstico e Auditoria Extra

O repositório conta com scripts utilitários focados em checagens rápidas de consistência e depuração de dados diretamente no banco DuckDB:

* **`src/check_data.py`**: Realiza queries gerais de validação volumétrica e integridade nas tabelas do Data Warehouse.
* **`src/check_data_spe.py`**: Focado na auditoria individualizada por SPE para garantir consistência temporal.
* **`src/debug_raf11.py`**: Script de rastreabilidade cirúrgica desenvolvido para diagnosticar o fluxo do ativo RAF11 por todas as camadas do pipeline (Raw -> Intermediate -> Gold), identificando possíveis gargalos de mapeamento de strings ou descartes.

Para rodar qualquer um deles, execute da raiz:

```bash
python src/debug_raf11.py

```

---

## 🌐 6. Inicialização e Consumo da API REST

Para expor os dados limpos e modelados do Star Schema para o consumo de outras aplicações ou portais corporativos, execute o servidor da API a partir da **raiz do projeto**:

```bash
uvicorn api:app --reload --app-dir src

```

> 💡 **Nota de Execução:** O parâmetro `--app-dir src` garante que a raiz do projeto permaneça como o diretório ativo de trabalho, salvaguardando a integridade dos caminhos relativos das pastas `data/`.

Após a inicialização do servidor, a documentação interativa Swagger poderá ser acessada diretamente pelo navegador através do endereço: **`http://127.0.0.1:8000/docs`**.

---

### 📂 Estrutura do Repositório

```text
case_analytics_data_eng/
├── data/
│   ├── raw/                 # Dados brutos obtidos da extração
│   ├── warehouse/           # Data Warehouse embarcado (.db do DuckDB)
│   └── modeled/             # Camada GOLD (Arquivos Parquet do Star Schema)
├── src/
│   ├── extract.py           # Pipeline de Ingestão Raw
│   ├── load_transform.py    # Tratamento Pandera, Qualidade e Logs
│   ├── model.py             # Fatiamento Dimensional e Governança de Chaves
│   ├── api.py               # Servidor FastAPI de exposição dos dados
│   ├── check_data.py        # Script utilitário de validação geral do DW
│   ├── check_data_spe.py    # Script utilitário de validação por SPE
│   └── debug_raf11.py       # Script de depuração e rastreabilidade do ativo RAF11
[cite_start]├── spes_casa_dos_ventos.csv # Mapeamento mestre de sementes (47 ativos) [cite: 1]
├── relatorio_qualidade.json # Dicionário consolidado de anomalias e erros
└── README.md                # Este documento de documentação geral

```

```

Só copiar e colar, Lívia! Vai ficar perfeito na sua entrega. Se precisar de mais alguma última checagem antes de enviar, manda bala! 🏁🍃

```