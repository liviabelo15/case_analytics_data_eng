# Case Data Engineering & Analytics — Casa dos Ventos & ONS

Pipeline de dados para consolidação, qualidade e análise dos dados de **restrição de geração (constrained-off) de parques eólicos**, integrando dados abertos do ONS com o cadastro interno da Casa dos Ventos.

---

## O que este projeto faz

O ONS publica mensalmente dois datasets sobre restrições de geração eólica:
- **Dataset 1 (Conjuntos):** métricas elétricas por complexo — geração total, limitações, disponibilidade e motivo da restrição.
- **Dataset 2 (SPEs):** métricas físicas por aerogerador individual — velocidade do vento, geração estimada e verificada.

Este pipeline baixa esses arquivos automaticamente, une os dois datasets, valida a qualidade dos dados, constrói um modelo dimensional para análise e expõe os resultados via API REST.

---

## Arquitetura: Camadas Bronze → Silver → Gold

```
ONS (S3)
   │
   ▼
[extract.py]  ──────────────────────────────────── Camada Bronze
   │  Baixa os arquivos Parquet do ONS e salva em data/raw/
   │
   ▼
[load_transform.py]  ────────────────────────────── Camada Silver
   │  Une os dois datasets, aplica regras de qualidade,
   │  valida com Pandera e gera relatorio_qualidade.json
   │  Saída: data/modeled/fato_geracao_cv_tratado.parquet
   │
   ▼
[model.py]  ─────────────────────────────────────── Camada Gold
   │  Constrói o modelo dimensional (Star Schema)
   │  Saída: 6 tabelas no banco data/warehouse/cv_case.db
   │
   ▼
[api.py]  ───────────────────────────────────────── Consumo
      API REST para consulta dos dados modelados
```

---

## Stack Tecnológica

| Ferramenta | Para que serve |
|---|---|
| **DuckDB** | Banco de dados analítico embutido — executa SQL direto em arquivos Parquet sem servidor |
| **Pandas** | Manipulação de dados em Python |
| **Pandera** | Validação de contratos de dados (tipos, ranges, nulos obrigatórios) |
| **FastAPI** | Framework para construção da API REST |
| **Parquet** | Formato de armazenamento colunar — compacto e rápido para leitura analítica |
| **Tenacity** | Retry automático com backoff exponencial nas requisições ao S3 do ONS |

---

## Decisões de Design

### Modelo dimensional com dois fatos separados

Os dados do ONS têm duas granularidades distintas: uma por SPE (aerogerador individual) e outra por conjunto/complexo. Misturá-las em uma única tabela fato causa um problema chamado **fan trap** — ao somar métricas de complexo sobre linhas de SPE, o resultado é multiplicado pelo número de SPEs, gerando valores incorretos silenciosamente.

A solução foi separar em dois fatos com seus grãos naturais:

- **`fato_geracao_spe`** — 1 linha = 1 SPE × 1 intervalo de 30 min  
  Métricas: vento, geração estimada, geração verificada
- **`fato_geracao_conjunto`** — 1 linha = 1 complexo × 1 intervalo de 30 min  
  Métricas: geração total, limitação, disponibilidade, motivo da restrição

### Três níveis de tratamento de dados

Em vez de descartar qualquer dado com problema, o pipeline aplica o tratamento mais adequado para cada tipo de anomalia:

| Tipo | O que é | O que acontece |
|---|---|---|
| **Quarentena** | Linha que falha na validação Pandera ou é duplicata com valores conflitantes | Gravada em `data/quarentena/rejeitados.parquet` com o motivo — nunca descartada silenciosamente |
| **Soft Drop** | Valor fisicamente impossível numa coluna (ex: vento negativo ou > 40 m/s) | Só aquela célula vira `NULL`; o resto da linha é preservado |
| **Flagging** | Regra de negócio violada (ex: `val_geracaolimitada > val_geracaoreferencia`) | Linha permanece; recebe `flg_alerta_limitada = 1` para auditoria |

### Garantia de 47 ativos na dimensão

A `dim_spe` é construída a partir do arquivo `spes_casa_dos_ventos.csv` como base, com um `LEFT JOIN` nos dados de telemetria. Isso garante que todos os 47 ativos cadastrados apareçam na dimensão — mesmo que uma SPE não tenha enviado dados no período.

### `dim_restricao` como vocabulário fixo do ONS

Os códigos de restrição (REL, CNF, ENE, PAR) e origens (LOC, SIS) são definidos pelo ONS e não mudam. A `dim_restricao` é uma tabela seed hardcoded com as 8 combinações, garantindo que todos os tipos apareçam no modelo independente de quais ocorreram no período coletado.

---

## Modelo Dimensional (Camada Gold)

```
                    dim_tempo
                    PK: din_instante
                         │
          ┌──────────────┴──────────────┐
          │                             │
  fato_geracao_spe              fato_geracao_conjunto
  PK: (spe, din_instante)       PK: (id_ons_conjunto, din_instante)
  FK → dim_spe                  FK → dim_conjunto
  FK → dim_tempo                FK → dim_tempo
                                FK → dim_restricao
          │
          ▼
       dim_spe                  dim_conjunto
       PK: spe          ──────► PK: id_ons_conjunto
       FK → dim_conjunto         nom_conjunto, subsistema, estado

                                dim_restricao
                                PK: (cod_razaorestricao, cod_origemrestricao)
                                8 linhas fixas do dicionário ONS
```

---

## Instalação

**Pré-requisito:** Python 3.10 ou superior.

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/case_analytics_data_eng.git
cd case_analytics_data_eng

# Crie e ative o ambiente virtual
python -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate         # Windows

# Instale as dependências
pip install -r requirements.txt
```

---

## Como executar

Execute os comandos sempre a partir da **raiz do projeto**, na ordem abaixo:

### 1. Extração — baixa os dados do ONS

```bash
python src/extract.py
```

Baixa os arquivos Parquet do S3 do ONS para os últimos 6 meses completos.  
Inclui fallback para `.csv` e `.xlsx` se o Parquet não estiver disponível.  
Saída: `data/raw/raw_usinas/` e `data/raw/raw_usinas_detail/`

### 2. Transformação e qualidade — limpa e valida os dados

```bash
python src/load_transform.py
```

Une os dois datasets, aplica validações Pandera, executa as regras de qualidade e gera o relatório.  
Saída: `data/modeled/fato_geracao_cv_tratado.parquet` e `relatorio_qualidade.json`

> Verifique o `relatorio_qualidade.json` após essa etapa. Ele mostra completude por projeto, distribuição de nulos, gaps de timestamp e registros em quarentena.

### 3. Modelagem — constrói o Star Schema

```bash
python src/model.py
```

Lê o Silver e constrói as 6 tabelas do modelo dimensional no banco DuckDB.  
Saída: `data/warehouse/cv_case.db` + arquivos Parquet em `data/modeled/`

| Tabela | Linhas | Descrição |
|---|---|---|
| `dim_spe` | 47 | Cadastro de SPEs (aerogeradores) |
| `dim_conjunto` | 7 | Cadastro de complexos eólicos |
| `dim_restricao` | 8 | Tipos de restrição do ONS (seed estática) |
| `dim_tempo` | 8.736 | Calendário de intervalos de 30 min |
| `fato_geracao_spe` | 395.088 | Métricas físicas por SPE |
| `fato_geracao_conjunto` | 61.152 | Métricas elétricas por complexo |

### 4. API — sobe o servidor de consulta

```bash
uvicorn src.api:app --reload
```

Acesse a documentação interativa em: **`http://127.0.0.1:8000/docs`**

---

## API REST — Endpoints

Todos os endpoints leem diretamente do banco Gold (`data/warehouse/cv_case.db`).

### `GET /projects`
Lista os 7 projetos com nome do conjunto, estado, subsistema e quantidade de SPEs.

### `GET /generation/{project_id}`
Geração verificada e estimada de um projeto agregada por dia ou mês.

| Parâmetro | Valores | Descrição |
|---|---|---|
| `project_id` | FLS, RVD, BBS, RVE, UMR, BBC, TGR | Código do projeto |
| `agrupamento` | `diario` (padrão) ou `mensal` | Granularidade temporal |
| `data_inicio` | AAAA-MM-DD | Filtro de data inicial (opcional) |
| `data_fim` | AAAA-MM-DD | Filtro de data final (opcional) |

### `GET /restrictions/summary`
Resumo de restrições por tipo: horas restritas e MWh cortado por projeto e motivo.

> `total_mwh_cortado` positivo = corte real de energia. Negativo = restrição registrada pelo ONS mas sem impacto efetivo na geração (o teto autorizado estava acima da capacidade de referência).

### `GET /health`
Verifica se o banco Gold está disponível e retorna a contagem de linhas em cada tabela.

---

## Scripts de diagnóstico

| Script | O que faz |
|---|---|
| `src/check_data.py` | Validações gerais de volume e integridade no banco |
| `src/check_data_spe.py` | Auditoria por SPE individual |
| `src/debug_raf11.py` | Rastreia o ativo RAF11 por todas as camadas do pipeline para identificar por que ele não tem dados |

```bash
python src/debug_raf11.py
```

---

## Estrutura do repositório

```
case_analytics_data_eng/
├── data/
│   ├── raw/
│   │   ├── raw_usinas/          # Dataset 1: métricas de conjuntos (Bronze)
│   │   └── raw_usinas_detail/   # Dataset 2: métricas de SPEs (Bronze)
│   ├── modeled/                 # Parquets Silver + Gold
│   ├── warehouse/
│   │   └── cv_case.db           # Banco DuckDB com o modelo dimensional
│   └── quarentena/
│       └── rejeitados.parquet   # Registros rejeitados com motivo
├── src/
│   ├── extract.py               # Extração dos dados do ONS (S3)
│   ├── load_transform.py        # Transformação, qualidade e validação
│   ├── model.py                 # Modelagem dimensional (Star Schema)
│   ├── api.py                   # API REST (FastAPI)
│   ├── check_data.py            # Diagnóstico geral do banco
│   ├── check_data_spe.py        # Diagnóstico por SPE
│   └── debug_raf11.py           # Rastreabilidade do ativo RAF11
├── spes_casa_dos_ventos.csv     # Cadastro mestre dos 47 ativos
├── relatorio_qualidade.json     # Relatório gerado pelo load_transform.py
├── requirements.txt
└── README.md
```

---

## Limitações conhecidas da fonte de dados

**Join SPE → Conjunto feito por nome:** O dataset de conjuntos do ONS não tem CEG — o campo aparece como `"-"` para todos os complexos. O único caminho de join disponível é pelo nome da usina (`nom_usina`), em comparação case-insensitive. Essa é uma limitação dos dados abertos do ONS, não do pipeline.

**Unidade de vento no dicionário ONS:** O campo `val_ventoverificado` está descrito como `m³/s` no dicionário oficial — o correto é `m/s`. A validação do pipeline usa o intervalo físico correto de 0 a 40 m/s.