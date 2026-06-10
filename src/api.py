from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from datetime import date
from typing import Optional
import json
import duckdb
import pandas as pd
from pathlib import Path

app = FastAPI(
    title="API de Geração e Restrições - Casa dos Ventos",
    description="API REST para dados tratados de geração e restrições de parques eólicos.",
    version="2.0.0"
)

# A API lê do banco Gold criado por model.py — não do Silver.
DB_PATH = Path("data/warehouse/cv_case.db")


def df_to_response(df: pd.DataFrame) -> list:
    # to_json converte NaN → null e datetime → ISO, evitando ValueError do encoder padrão.
    return json.loads(df.to_json(orient="records", date_format="iso"))


def get_con():
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Banco Gold não encontrado: {DB_PATH}. Execute model.py primeiro."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


@app.get("/", include_in_schema=False)
def raiz():
    return RedirectResponse(url="/docs")


# =====================================================================
# ROTA 1: GET /projects
# =====================================================================
@app.get("/projects", tags=["Projetos"])
def listar_projetos():
    """
    Lista os 7 projetos com metadados: conjunto associado, estado, subsistema e
    quantidade de SPEs. Fonte: dim_spe + dim_conjunto (Gold layer).
    """
    query = """
        SELECT
            d.projeto_cv      AS id_projeto,
            c.nom_conjunto    AS nome_conjunto,
            c.nom_subsistema  AS subsistema,
            c.id_estado       AS estado,
            COUNT(d.spe)      AS qtd_spes
        FROM dim_spe d
        JOIN dim_conjunto c ON d.id_ons_conjunto = c.id_ons_conjunto
        GROUP BY d.projeto_cv, c.nom_conjunto, c.nom_subsistema, c.id_estado
        ORDER BY d.projeto_cv
    """
    with get_con() as con:
        df = con.execute(query).df()
    return df_to_response(df)


# =====================================================================
# ROTA 2: GET /generation/{project_id}
# =====================================================================
@app.get("/generation/{project_id}", tags=["Métricas de Geração"])
def obter_geracao(
    project_id: str,
    agrupamento: str = Query(
        "diario",
        description="Agregação temporal: 'diario' ou 'mensal'"
    ),
    data_inicio: Optional[date] = Query(None, description="Data inicial (AAAA-MM-DD)"),
    data_fim:    Optional[date] = Query(None, description="Data final (AAAA-MM-DD)")
):
    """
    Geração de um projeto agregada por dia ou mês.

    Métricas em MW são convertidas para MWh multiplicando por 0.5
    (cada intervalo ONS = 30 min = 0.5 h).

    Fonte: fato_geracao_spe + dim_spe (Gold layer).
    """
    if agrupamento not in ("diario", "mensal"):
        raise HTTPException(
            status_code=400,
            detail="O parâmetro 'agrupamento' deve ser 'diario' ou 'mensal'."
        )

    periodo_expr = (
        "CAST(f.din_instante AS DATE)"
        if agrupamento == "diario"
        else "DATE_TRUNC('month', f.din_instante)"
    )

    filtros = ["d.projeto_cv = ?"]
    params: list = [project_id]

    if data_inicio:
        filtros.append("f.din_instante >= ?")
        params.append(str(data_inicio))
    if data_fim:
        filtros.append("f.din_instante <= ?")
        params.append(str(data_fim))

    where = " AND ".join(filtros)

    query = f"""
        SELECT
            {periodo_expr}                                AS periodo,
            ROUND(SUM(f.val_geracaoverificada * 0.5), 2) AS total_mwh_verificado,
            ROUND(SUM(f.val_geracaoestimada   * 0.5), 2) AS total_mwh_estimado,
            ROUND(AVG(f.val_ventoverificado),         2) AS vento_medio_ms,
            COUNT(DISTINCT f.spe)                         AS qtd_spes_ativas
        FROM fato_geracao_spe f
        JOIN dim_spe d ON f.spe = d.spe
        WHERE {where}
        GROUP BY {periodo_expr}
        ORDER BY {periodo_expr}
    """

    with get_con() as con:
        df = con.execute(query, params).df()

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Nenhum dado para o projeto '{project_id}' no período selecionado."
        )
    return df_to_response(df)


# =====================================================================
# ROTA 3: GET /restrictions/summary
# =====================================================================
@app.get("/restrictions/summary", tags=["Restrições Operacionais"])
def resumo_restricoes(
    project_id: Optional[str] = Query(None, description="Filtrar por projeto (ex: FLS, TGR)"),
    data_inicio: Optional[date] = Query(None, description="Data inicial (AAAA-MM-DD)"),
    data_fim:    Optional[date] = Query(None, description="Data final (AAAA-MM-DD)")
):
    """
    Resumo de restrições agrupado por razão e origem: horas restritas e MWh cortado.

    MWh cortado = COALESCE(val_geracaoreferenciafinal, val_geracaoreferencia) − val_geracaolimitada,
    somado nos intervalos com restrição ativa. O COALESCE usa o valor definitivo (pós-operação)
    quando disponível e cai no valor de referência inicial caso contrário.

    Sinal do total_mwh_cortado:
    - Positivo: limitação abaixo da referência → corte real de energia.
    - Negativo: limitação acima da referência → restrição formal sem corte efetivo
      (flg_alerta_limitada=1 no pipeline). Pode indicar ajuste operacional ou
      inconsistência entre val_geracaoreferencia e val_geracaoreferenciafinal.

    Restrições no ONS são definidas no grão conjunto/complexo, não por SPE individual.
    Fonte: fato_geracao_conjunto + dim_restricao (Gold layer) — grão conjunto×timestamp
    para evitar dupla contagem por número de SPEs no complexo.
    """
    filtros = ["fc.cod_razaorestricao IS NOT NULL"]
    params: list = []

    if project_id:
        # Cada conjunto pertence a exatamente 1 projeto; subquery evita produto cartesiano.
        filtros.append("""
            fc.id_ons_conjunto IN (
                SELECT DISTINCT id_ons_conjunto
                FROM dim_spe
                WHERE projeto_cv = ?
            )
        """)
        params.append(project_id)
    if data_inicio:
        filtros.append("fc.din_instante >= ?")
        params.append(str(data_inicio))
    if data_fim:
        filtros.append("fc.din_instante <= ?")
        params.append(str(data_fim))

    where = " AND ".join(filtros)

    # Restrições no ONS existem no grão conjunto — não há cod_razaorestricao por SPE.
    # Agrupamos por projeto + conjunto + tipo de restrição para que o resultado
    # seja identificável mesmo quando project_id não é informado.
    query = f"""
        SELECT
            ds.projeto_cv                                               AS projeto,
            dc.nom_conjunto                                             AS nome_conjunto,
            fc.cod_razaorestricao                                       AS razao_restricao,
            fc.cod_origemrestricao                                      AS origem,
            COALESCE(
                ANY_VALUE(dr.dsc_restricao_completa),
                ANY_VALUE(fc.dsc_restricao)
            )                                                           AS descricao,
            ROUND(COUNT(*) * 0.5, 1)                                   AS total_horas_com_restricao,
            ROUND(
                SUM(
                    COALESCE(fc.val_geracaoreferenciafinal, fc.val_geracaoreferencia)
                    - fc.val_geracaolimitada
                ), 2
            )                                                           AS total_mwh_cortado
        FROM fato_geracao_conjunto fc
        JOIN dim_conjunto dc ON fc.id_ons_conjunto = dc.id_ons_conjunto
        JOIN (
            SELECT DISTINCT id_ons_conjunto, projeto_cv FROM dim_spe
        ) ds ON ds.id_ons_conjunto = fc.id_ons_conjunto
        LEFT JOIN dim_restricao dr
            ON  fc.cod_razaorestricao  = dr.cod_razaorestricao
            AND fc.cod_origemrestricao = dr.cod_origemrestricao
        WHERE {where}
        GROUP BY ds.projeto_cv, dc.nom_conjunto, fc.cod_razaorestricao, fc.cod_origemrestricao
        ORDER BY total_mwh_cortado DESC NULLS LAST
    """

    with get_con() as con:
        df = con.execute(query, params).df()
    return df_to_response(df)


# =====================================================================
# ROTA 4: GET /health
# =====================================================================
@app.get("/health", tags=["Infraestrutura"])
def health_check():
    """
    Verifica se o banco Gold existe e se as tabelas principais estão populadas.
    """
    if not DB_PATH.exists():
        return {"status": "DOWN", "detail": "Banco Gold ausente. Execute model.py."}

    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            spes      = con.sql("SELECT COUNT(*) FROM dim_spe").fetchone()[0]
            conjuntos = con.sql("SELECT COUNT(*) FROM dim_conjunto").fetchone()[0]
            fatos_spe = con.sql("SELECT COUNT(*) FROM fato_geracao_spe").fetchone()[0]
            fatos_conj = con.sql("SELECT COUNT(*) FROM fato_geracao_conjunto").fetchone()[0]
        return {
            "status": "UP",
            "dim_spe": spes,
            "dim_conjunto": conjuntos,
            "fato_geracao_spe": fatos_spe,
            "fato_geracao_conjunto": fatos_conj,
            "api_version": "2.0.0"
        }
    except Exception as e:
        return {"status": "DEGRADED", "detail": str(e)}
