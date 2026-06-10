# Documentação Técnica — Pipeline de Dados Casa dos Ventos

---

## Sumário

1. [Contexto e objetivo do projeto](#1-contexto-e-objetivo-do-projeto)
2. [Por que DuckDB?](#2-por-que-duckdb)
3. [Arquitetura de Medalhão — Bronze, Silver e Gold](#3-arquitetura-de-medalhão--bronze-silver-e-gold)
4. [Parte 1 — Extração (`extract.py`)](#4-parte-1--extração-extractpy)
5. [Parte 2 — Transformação e Qualidade (`load_transform.py`)](#5-parte-2--transformação-e-qualidade-load_transformpy)
6. [Parte 3 — Modelagem Dimensional (`model.py`)](#6-parte-3--modelagem-dimensional-modelpy)
7. [Parte 4 — API REST (`api.py`)](#7-parte-4--api-rest-apipy)
8. [Orquestração com Apache Airflow](#8-orquestração-com-apache-airflow)
9. [Evolução para produção em nuvem](#9-evolução-para-produção-em-nuvem)

---

## 1. Contexto e objetivo do projeto

O ONS (Operador Nacional do Sistema Elétrico) publica mensalmente dados sobre restrições de geração eólica — situações em que usinas precisam reduzir ou interromper a geração por motivos externos (rede elétrica, confiabilidade do sistema, etc.). Esse evento é chamado de **constrained-off** ou **curtailment**.

A Casa dos Ventos opera 47 SPEs (Sociedades de Propósito Específico). Quando uma usina é restringida, ela deixa de gerar energia que poderia vender — impacto direto em receita.

**O objetivo deste pipeline é:**
1. Coletar esses dados automaticamente do ONS
2. Garantir que a qualidade dos dados é confiável antes de qualquer análise
3. Organizar os dados num modelo que facilite a análise dos dados
4. Expor esses dados via API para consumo por outros sistemas

---

## 2. Por que DuckDB?

### A justificativa central

O problema deste pipeline é **analítico**: calcular somas, médias e agrupamentos sobre 395 mil registros de uma vez. Assim, foi escolhido o DuckDB, pois ele processa os dados coluna por coluna, o que significa que para calcular `SUM(val_geracaoverificada)`, ele lê apenas essa coluna — sem tocar nas outras 26 do dataset. Além disso, funciona como uma biblioteca Python (`import duckdb`), sem servidor nem configuração, e lê arquivos Parquet diretamente sem precisar importá-los primeiro.

### Comparação com as principais alternativas

| Critério | SQLite | PostgreSQL | DuckDB |
|---|---|---|---|
| Para que foi feito | Apps simples | Sistemas web | Análise de dados |
| Precisa de servidor? | Não | Sim | Não |
| Lê Parquet sem importar? | Não | Não | Sim |
| Integração com Pandas | Manual | Manual | Nativa (`.df()`) |
| Velocidade em GROUP BY (395k linhas) | Lenta | Média | Rápida |

---

## 3. Arquitetura de Medalhão — Bronze, Silver e Gold

A arquitetura de medalhão organiza os dados em camadas de qualidade crescente. O principal benefício prático é a rastreabilidade: se um erro for identificado em qualquer etapa, é possível reprocessar apenas a partir daquela camada, sem refazer tudo desde a extração.

| Camada | Onde está | O que contém |
|---|---|---|
| **Bronze** | `data/raw/` | Dados brutos exatamente como saíram do ONS — preservados sem modificação |
| **Silver** | `data/modeled/fato_geracao_cv_tratado.parquet` | Dados limpos, validados e unidos — prontos para modelar |
| **Gold** | `data/warehouse/cv_case.db` | Modelo dimensional — organizado para análise e consumo pela API |

---

## 4. Parte 1 — Extração (`extract.py`)

### O que faz

Baixa automaticamente os arquivos de restrição de geração do servidor S3 do ONS para os últimos 6 meses completos.

### Decisões técnicas

**Geração dinâmica de datas:** Usa-se a função `generate_dynamic_month_list()` que calcula automaticamente os últimos 6 meses a partir da data atual, com defasagem de 1 mês (para não tentar baixar o mês em andamento, que ainda não foi publicado pelo ONS).

```python
# Hoje é junho/2026 → baixa dezembro/2025 até maio/2026
COMPET = generate_dynamic_month_list(qtd_meses=6, defasagem_meses=1)
```

**Fallback de formatos (.parquet → .csv → .xlsx):** O ONS nem sempre publica o mesmo formato. O pipeline tenta Parquet primeiro (mais eficiente), mas cai para CSV e depois XLSX se o Parquet não estiver disponível. Isso evita que o pipeline quebre por uma mudança de formato na fonte.

**Retry com backoff exponencial:** Nos casos de falhas no Servidor por instabilidade momentânea de rede. Em vez de falhar imediatamente, o pipeline tenta novamente em intervalos crescentes (2s, 4s, 8s... até 30s), até 5 tentativas. 

**Download em streaming:** Os arquivos são gravados em disco em pedaços de 1 MB conforme chegam, em vez de serem carregados inteiros na memória antes de salvar. Isso mantém o consumo de memória constante independente do tamanho do arquivo.

---

## 5. Parte 2 — Transformação e Qualidade (`load_transform.py`)

### O que faz

Une os dois datasets do ONS, aplica regras de qualidade e gera um relatório detalhado sobre a saúde dos dados.

### Arquitetura ELT

Este pipeline usa **ELT** (Extract → Load → Transform): os dados brutos são carregados primeiro no DuckDB, e as transformações são feitas em SQL dentro do banco.

**Por que ELT aqui?**
- O DuckDB é otimizado para transformações analíticas — fazer o JOIN entre os dois datasets em SQL é mais rápido e legível que em Python puro
- As transformações ficam documentadas como queries SQL auditáveis
- Se precisar reprocessar só uma etapa, é mais fácil re-executar uma query SQL do que um bloco de código Python

### O join entre SPE e Conjunto 

O maior desafio técnico desta etapa é unir os dados de SPEs (Dataset 2) com os dados de conjuntos (Dataset 1). O campo natural para esse join seria o código CEG, mas o dataset de conjuntos do ONS tem `ceg = "-"` para todos os registros — o ONS simplesmente não disponibiliza o CEG dos complexos.

A solução é fazer o join pelo nome da usina (`nom_usina`), em comparação case-insensitive:

```sql
ON UPPER(spe.nom_usina) = UPPER(conj.nom_usina)
```

Há ainda um caso especial: SPEs do tipo "Tipo II-C" usam o campo `nom_conjuntousina` como referência ao conjunto, não `nom_usina`. Isso é tratado com um `CASE WHEN` no join.

Essa limitação é documentada no código via `logger.warning` quando `nom_usina` é NULL — porque se o nome for nulo, o join falha silenciosamente e a SPE perde todas as suas métricas de conjunto.

### Formas de tratar dados problemáticos que foram utilizadas

Para não descartar uma linha inteira porque uma coluna tem valor suspeito e perder as outras 26 colunas válidas da mesma linha que contém informações importantes, o pipeline aplica o tratamento mais proporcional ao tipo de problema:

| Tratamento | Quando aplicar | O que acontece |
|---|---|---|
| **Quarentena** | Linha viola o contrato de dados (Pandera) ou é duplicata com valores conflitantes | Gravada em `data/quarentena/rejeitados.parquet` com o motivo — nunca descartada silenciosamente |
| **Soft Drop** | Valor fisicamente impossível numa coluna (ex: vento negativo ou > 40 m/s) | Só aquela célula vira `NULL`; o resto da linha é preservado |
| **Flagging** | Regra de negócio violada (ex: limitação acima da referência) | Linha permanece com `flg_alerta_limitada = 1` para auditoria |

A quarentena é importante, pois responde à pergunta "o que foi removido e por quê?". Com o arquivo de quarentena, a resposta está documentada e acessível.

O limite de 40 m/s para o vento é baseado no domínio físico. Um valor acima de 40 m/s indica erro de telemetria, não vento real.

### A coluna `flg_estimada_por_historico`

O campo `flg_dadoventoinvalido` do ONS tem três estados possíveis:
- `NULL` — sem dado de vento
- `0.0` — vento válido, estimativa calculada pela curva vento × potência
- `1.0` — telemetria falhou por mais de 6 minutos; o ONS calcula `val_geracaoestimada` usando histórico de geração em vez da curva física

Quando a flag é 1, a estimativa é menos precisa — mas ficava invisível no dado. A nova coluna derivada `flg_estimada_por_historico` torna explícita essa diferença de qualidade.

### Contratos de dados com Pandera

Pandera é uma biblioteca que permite definir um "contrato" de como os dados devem estar antes de prosseguir. Exemplos do contrato neste pipeline:

```python
"val_geracao_conjunto": Column(float, checks=Check.ge(0), nullable=False)
# → deve ser float, >= 0 e não pode ser NULL

"val_ventoverificado": Column(float, checks=Check.in_range(0, 40), nullable=True)
# → deve ser float entre 0 e 40, mas pode ser NULL
```

### Completude com denominador fixo

Para verificar a métrica de completude, é dividido o valor de dados recebido pelo valor esperado. Assim, o denominador correto é calculado com base no período completo:

```
Esperado = n_spes_do_projeto × n_intervalos_de_30min_no_período
```

Onde o período vai do primeiro dia do mês mais antigo até o último dia do mês mais recente nos dados.

---

## 6. Parte 3 — Modelagem Dimensional (`model.py`)

### O que é modelagem dimensional?

Modelagem dimensional é uma técnica de organizar dados para facilitar análises. O conceito central é separar os dados em dois tipos de tabela:

- **Dimensões:** descrevem as entidades do negócio — "quem, o quê, onde, quando". Ex: `dim_spe` descreve cada aerogerador.
- **Fatos:** registram as métricas que aconteceram. Ex: `fato_geracao_spe` registra quanto cada SPE gerou em cada instante.

O modelo resultante, onde as fatos ficam no centro e as dimensões ao redor, é chamado de **Star Schema** (esquema estrela).

### O problema que foi resolvido: fan trap

Os dados do ONS têm duas granularidades distintas:
- **Dataset 2 (SPEs):** uma linha por SPE por intervalo de 30 min
- **Dataset 1 (Conjuntos):** uma linha por complexo por intervalo de 30 min

Para que não ocorra uma fan trap e, ao somar a geração do complexo, o valor de algumas métricas ser multiplicado pelo número de SPEs no complexo, foram criadas duas tabelas-fatos respeitando suas respectivas métricas.

**Tabelas-Fato:**

`fato_geracao_spe` → 1 linha = 1 SPE × 1 intervalo (395.088 linhas)  
`fato_geracao_conjunto` → 1 linha = 1 complexo × 1 intervalo (61.152 linhas = 7 × 8.736)

### Por que Star Schema e não uma tabela única?

Com uma tabela única, qualquer consulta analítica exige filtros e lógica espalhados pela query. O Star Schema organiza os dados em dimensões (quem, o quê, quando) e fatos (o que aconteceu), permitindo navegação natural pela hierarquia do negócio: projeto → conjunto → SPE.

`dim_spe` e `dim_conjunto` ficam como dimensões separadas — e não achatadas numa só — porque são entidades distintas no domínio do ONS: têm IDs próprios (`id_ons_spe` vs `id_ons_conjunto`) e métricas completamente diferentes. Tratá-las como a mesma entidade esconderia essa hierarquia.

### SCDs — Slowly Changing Dimensions

**SCD Tipo 1 (sobrescreve):** Simplesmente atualiza o valor. Histórico anterior fica com o novo valor também. Simples, mas perde a rastreabilidade.

**SCD Tipo 2 (versiona):** Cria uma nova linha para cada mudança, com datas de início e fim de validade. Preserva o histórico completo, mas aumenta a complexidade das queries.

**Escolha neste projeto:** SCD Tipo 1, porque:
- O período de análise é de 6 meses (curto), sem evidência de reclassificações ONS nesse intervalo
- A Casa dos Ventos não forneceu histórico de mudanças de configuração das SPEs
- Para o case, a simplicidade do Tipo 1 é adequada

**Em produção:** seria recomendado Tipo 2 em `dim_spe` para preservar o histórico se o projeto de uma SPE mudar.

### `dim_restricao` como seed estática

Os 4 códigos de razão de restrição (REL, CNF, ENE, PAR) e 2 origens (LOC, SIS) são definidos pelo ONS no dicionário de dados e não mudam operacionalmente.

Se essa dimensão fosse derivada dos dados (`SELECT DISTINCT cod_razaorestricao FROM fato`), ela ficaria incompleta em meses onde um tipo de restrição não ocorreu — causando joins quebrados em dashboards que exibem todos os tipos.

A solução é uma **tabela seed**: 8 linhas hardcoded diretamente no código, baseadas no dicionário ONS. É um vocabulário controlado, não um dado operacional.

---

## 7. Parte 4 — API REST (`api.py`)

### O que faz

Expõe os dados do modelo dimensional via HTTP para consumo por outros sistemas, dashboards ou times que não têm acesso direto ao banco.

A API lê diretamente do banco DuckDB Gold (`data/warehouse/cv_case.db`), ou seja, com os dados já tratados e modelados.

### Decisões técnicas nos endpoints

**`/generation/{project_id}` — conversão de MW para MWh**

Os dados do ONS registram potência média em MW durante um intervalo de 30 min. Para obter energia em MWh:

```
MWh = MW × horas = MW × 0.5
```

Cada intervalo semihorário representa 0,5 hora de geração.

**`/restrictions/summary` — COALESCE na fórmula de MWh cortado**

```sql
COALESCE(val_geracaoreferenciafinal, val_geracaoreferencia) - val_geracaolimitada
```

O campo `val_geracaoreferenciafinal` é o valor definitivo calculado pelo ONS após a operação. O `val_geracaoreferencia` é o valor inicial, calculado antes. Usar só o inicial subestima o corte real quando o ONS publicou o valor final. O `COALESCE` garante que o valor mais preciso é usado sempre que disponível.

**Tratamento de `NaN` na serialização JSON**

Quando `AVG()` é calculado sobre um conjunto de valores NULL (ex: uma SPE sem dados de vento), o DuckDB retorna NULL, que vira `float('nan')` no pandas. O encoder JSON padrão do Python rejeita `NaN` com erro. A solução é usar `df.to_json()` em vez de `df.to_dict()`, pois esse método converte `NaN` para `null` (JSON válido) automaticamente.

### Por que FastAPI e não Flask?

| | Flask | FastAPI |
|---|---|---|
| Documentação automática | Não (precisa de extensão) | Sim (Swagger em `/docs`) |
| Validação de parâmetros | Manual | Automática via tipagem Python |
| Performance | Boa | Melhor (async nativo) |
| Curva de aprendizado | Menor | Semelhante |

O Swagger automático permite testar todos os endpoints sem precisar de Postman ou curl.

---

## 8. Orquestração com Apache Airflow

### O problema que o Airflow resolve

O pipeline atual é executado manualmente: você roda `extract.py`, depois `load_transform.py`, depois `model.py`. Em produção, isso não é viável — as etapas precisam rodar automaticamente todo mês, na ordem certa, com alertas quando algo falha e histórico de execuções.

**Apache Airflow**  permite:
- Definir dependências entre tarefas ("execute B só depois que A terminar com sucesso")
- Agendar execuções (ex: todo dia 2 do mês às 06:00)
- Reexecutar apenas a etapa que falhou, sem refazer tudo
- Visualizar o histórico de execuções com logs de cada tarefa
- Enviar alertas por e-mail quando uma tarefa falha

### Conceito de DAG

No Airflow, um pipeline é definido como um **DAG** (Directed Acyclic Graph — Grafo Direcionado Acíclico). É um diagrama onde cada nó é uma tarefa e as setas mostram as dependências.

"Acíclico" significa que não pode haver ciclos — a tarefa A não pode depender de B se B já depende de A. Isso garante que o pipeline sempre tem uma direção clara de execução.

### Como o pipeline ficaria como DAG

```
[extracao_ons] ──► [sensor_arquivos] ──► [transformacao_qualidade] ──► [modelagem_dimensional]
   Baixa os          Verifica se os         Limpa, valida e              Constrói o Star
  arquivos ONS      arquivos chegaram       une os datasets              Schema Gold
```

**Agenda:** todo dia 2 do mês às 06:00 — garante que o ONS já publicou os dados do mês anterior.

**Retries:** 3 tentativas com 10 minutos de intervalo em caso de falha — uma instabilidade passageira não derruba o pipeline.

**Alertas:** e-mail automático para o time de dados em caso de erro em qualquer tarefa.

O `sensor_arquivos` é um tipo especial de tarefa que fica verificando se os arquivos de extração existem antes de liberar a transformação — evita que a etapa seguinte comece com dados incompletos.

Se `transformacao_qualidade` falhar, o Airflow reexecuta só ela, sem precisar refazer a extração. Isso reduz tempo de recuperação e custo operacional.

---

## 9. Evolução para produção em nuvem

### O estado atual: serverless local

O pipeline atual roda localmente: os dados ficam em disco, o banco é um arquivo `.db` e a API é um processo Python. Isso é suficiente para demonstrar a solução, mas não é adequado para produção por alguns motivos:
- O computador local não está sempre ligado e acessível
- Não há escalabilidade — se os dados crescerem 100×, o mesmo hardware não aguenta
- Não há separação entre ambientes (desenvolvimento, homologação, produção)
- Não há monitoramento ou alertas automatizados

### Como cada componente evoluiria (GCP)

A estrutura existente mapeia diretamente para serviços cloud. O código muda pouco — a infraestrutura muda:

| Componente atual | Equivalente em nuvem | Motivo |
|---|---|---|
| `data/raw/` e `data/modeled/` (disco local) | Google Cloud Storage (GCS) | Armazenamento durável, acessível por qualquer serviço |
| Scripts rodando localmente | Cloud Run (containers sob demanda) | Execução serverless — paga só pelo tempo de processamento |
| DuckDB local (`cv_case.db`) | BigQuery | Data warehouse gerenciado, escala automaticamente, sem servidor |
| Execução manual | Cloud Composer (Airflow gerenciado) | O DAG da seção anterior rodaria aqui, com interface visual e alertas |
| Sem monitoramento | Cloud Monitoring + Alerting | Alertas automáticos para falhas, completude baixa ou quarentena alta |

### Evolução do DuckDB para BigQuery

A transição seria gradual:

| Fase | O que muda | O que permanece |
|---|---|---|
| **Fase 1 (atual)** | Tudo local | — |
| **Fase 2** | Armazenamento vai para GCS; processamento ainda local | Código Python sem mudança |
| **Fase 3** | Cloud Run para execução; DuckDB lê do GCS nativo | Código Python sem mudança |
| **Fase 4** | BigQuery substitui DuckDB Gold; API lê do BQ | Queries SQL quase idênticas |

DuckDB e BigQuery usam SQL com sintaxe muito similar. As queries do `model.py` e `api.py` precisariam de ajustes mínimos (principalmente `ANY_VALUE` → `ANY_VALUE` já existe no BQ; `read_parquet()` → tabelas externas do GCS).

---