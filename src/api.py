from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from datetime import date
from typing import Optional, List
import duckdb
from pathlib import Path
import pandas as pd

app = FastAPI(
    title="API de Geração e Restrições - Casa dos Ventos",
    description="API REST para expor dados tratados de geração e restrições de parques eólicos.",
    version="1.0.0"
)

# Caminho do nosso arquivo Parquet tratado (Camada Gold)
PARQUET_PATH = Path("data/modeled/fato_geracao_cv_tratado.parquet")

# Helper function para rodar queries no DuckDB apontando pro Parquet
def executar_query(query: str, params: tuple = ()) -> pd.DataFrame:
    if not PARQUET_PATH.exists():
        raise HTTPException(
            status_code=500, 
            detail=f"Arquivo de dados não encontrado no caminho: {PARQUET_PATH}. Execute o pipeline primeiro."
        )
    
    # O DuckDB consegue ler o arquivo Parquet diretamente usando a função read_parquet
    with duckdb.connect() as con:
        try:
            return con.execute(query.replace("fato_gold", f"read_parquet('{PARQUET_PATH}')"), params).df()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro interno ao processar banco de dados: {str(e)}")

# =====================================================================
# ROTA 0: Redirecionar a raiz (/) para o Swagger automaticamente
# =====================================================================
@app.get("/", include_in_schema=False)
def raiz():
    return RedirectResponse(url="/docs")

# =====================================================================
# ROTA 1: GET /projects
# =====================================================================
@app.get("/projects", tags=["Projetos"])
def listar_projetos():
    """
    Lista todos os projetos disponíveis na base com seus metadados básicos.
    """
    query = """
        SELECT DISTINCT 
            nome_spe_cv as id_projeto,
            nom_usina as nome_parque,
            id_estado as estado,
            nom_subsistema as subsistema
        FROM fato_gold
        ORDER BY nome_spe_cv
    """
    df = executar_query(query)
    return df.to_dict(orient="records")

# =====================================================================
# ROTA 2: GET /generation/{project_id}
# =====================================================================
@app.get("/generation/{project_id}", tags=["Métricas de Geração"])
def obter_geracao(
    project_id: str,
    agrupamento: str = Query("diario", description="Tipo de agregação temporal: 'diario' ou 'mensal'"),
    data_inicio: Optional[date] = Query(None, description="Data inicial do filtro (AAAA-MM-DD)"),
    data_fim: Optional[date] = Query(None, description="Data final do filtro (AAAA-MM-DD)")
):
    """
    Retorna os dados de geração agregados (diário ou mensal) de um projeto específico,
    com filtros opcionais por período.
    """
    # Validar se o agrupamento enviado é válido
    if agrupamento not in ["diario", "mensal"]:
        raise HTTPException(status_code=400, detail="O parâmetro 'agrupamento' deve ser 'diario' ou 'mensal'.")

    # Definir a lógica de agrupamento de tempo no SQL
    formato_tempo = "CAST(din_instante AS DATE)" if agrupamento == "diario" else "strftime(din_instante, '%Y-%m')"
    
    # Construção dinâmica de filtros de data
    clausulas_where = ["nome_spe_cv = ?"]
    params = [project_id]

    if data_inicio:
        clausulas_where.append("din_instante >= ?")
        params.append(str(data_inicio))
    if data_fim:
        clausulas_where.append("din_instante <= ?")
        params.append(str(data_fim))

    where_str = " AND ".join(clausulas_where)

    query = f"""
        SELECT 
            {formato_tempo} as periodo,
            ROUND(SUM(val_geracao_conjunto), 2) as total_gerado_mwh,
            ROUND(AVG(val_geracao_conjunto), 2) as media_geracao_mwh,
            ROUND(AVG(val_ventoverificado), 2) as velocidade_media_vento_ms
        FROM fato_gold
        WHERE {where_str}
        GROUP BY periodo
        ORDER BY periodo
    """
    
    df = executar_query(query, tuple(params))
    
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Nenhum dado encontrado para o projeto '{project_id}' no período selecionado.")
        
    return df.to_dict(orient="records")

# =====================================================================
# ROTA 3: GET /restrictions/summary
# =====================================================================
@app.get("/restrictions/summary", tags=["Restrições Operacionais"])
def resumo_restricoes(
    project_id: Optional[str] = Query(None, description="Filtrar por ID de um projeto específico"),
    data_inicio: Optional[date] = Query(None, description="Data inicial do filtro (AAAA-MM-DD)"),
    data_fim: Optional[date] = Query(None, description="Data final do filtro (AAAA-MM-DD)")
):
    """
    Retorna o resumo de restrições de corte de carga: total de horas e MWh restrito,
    agrupado pela razão da restrição (cod_razaorestricao).
    """
    clausulas_where = ["cod_razaorestricao IS NOT NULL"]
    params = []

    if project_id:
        clausulas_where.append("nome_spe_cv = ?")
        params.append(project_id)
    if data_inicio:
        clausulas_where.append("din_instante >= ?")
        params.append(str(data_inicio))
    if data_fim:
        clausulas_where.append("din_instante <= ?")
        params.append(str(data_fim))

    where_str = " AND ".join(clausulas_where)

    query = f"""
        SELECT 
            cod_razaorestricao as razao_restricao,
            ANY_VALUE(dsc_restricao) as descricao_motivo,
            -- Como o dado é semi-horário (30 min), cada linha com restrição equivale a 0.5 horas
            ROUND(COUNT(*) * 0.5, 1) as total_horas_com_restricao,
            -- Diferença entre o que poderia gerar (referência) e o que gerou devido à limitação
            ROUND(SUM(val_geracaoreferencia_conjunto - val_geracaolimitada_conjunto), 2) as total_mwh_cortado
        FROM fato_gold
        WHERE {where_str}
        GROUP BY cod_razaorestricao
        ORDER BY total_mwh_cortado DESC
    """
    
    df = executar_query(query, tuple(params))
    return df.to_dict(orient="records")

# =====================================================================
# ROTA 4: GET /health
# =====================================================================
@app.get("/health", tags=["Infraestrutura"])
def health_check():
    """
    Health check simples para monitoramento da API e status do arquivo Parquet.
    """
    status_arquivo = "Saudável" if PARQUET_PATH.exists() else "Indisponível (Arquivo Gold ausente)"
    return {
        "status": "UP",
        "database_file": status_arquivo,
        "api_version": "1.0.0"
    }