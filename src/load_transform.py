# ==============================================================================
# NOME DO SCRIPT: load_transform.py
# AUTOR:          Lívia Belo
# DATA DA VERSÃO: Maio de 2026
# VERSÃO:         2.0.0
# DESCRIPTION:    Pipeline de Transformação e Qualidade (LT) que consolida os 
#                 dados eólicos da Casa dos Ventos/ONS. Realiza modelagem via 
#                 DuckDB (Soft Drop/Flagging), valida o contrato de dados via 
#                 Pandera (Hard Drop), audita a saúde da base (Completude, Gaps 
#                 e Freshness) e exporta diagnósticos estruturados em JSON e Parquet.
# ==============================================================================

import logging 
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema
import duckdb
import numpy as np
from pathlib import Path 
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_USINAS_DIR = Path("data/raw/raw_usinas")
RAW_USINAS_DETAIL_DIR = Path("data/raw/raw_usinas_detail")

DB_PATH = Path("data/warehouse/cv_case.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

GOLD_OUTPUT_DIR = Path("data/modeled")
GOLD_PARQUET_PATH = GOLD_OUTPUT_DIR / "fato_geracao_cv_tratado.parquet"
QUARENTENA_PATH = Path("data/quarentena/rejeitados.parquet")

def load_duckdb(db_path: Path, raw_usinas_path: Path, raw_detail_path: Path):
    logger.info("Iniciando a fase LOAD no DuckDB lendo arquivos Parquet...")
    con = duckdb.connect(str(db_path))
    try: 
        logger.info("Carregando dados brutos de restrição de usinas...")
        con.sql(f"""
            CREATE OR REPLACE TABLE raw_usinas as
            SELECT * FROM read_parquet ('{raw_usinas_path}/*.parquet', union_by_name=true, filename=true)
        """)

        logger.info("Carregando dados brutos de detalhamento das usinas...")
        con.sql(f"""
            CREATE OR REPLACE TABLE raw_usinas_detail as
            SELECT * FROM read_parquet ('{raw_detail_path}/*.parquet', union_by_name=true, filename=true)
        """)

        logger.info("Carregando dados SPES CDV...")
        con.sql("""
            CREATE OR REPLACE TABLE seed_spes_cv AS 
            SELECT * FROM read_csv_auto('spes_casa_dos_ventos.csv', 
            delim=',',
            encoding='utf-8',
            header=True,
            auto_detect=True
            )
        """)
        logger.info("Fase LOAD concluída com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o Load: {e}")
        raise
    finally:
        con.close()

def spes_cvd_data_extract(db_path: Path):
    logger.info("Iniciando a filtragem das SPEs da Casa dos Ventos (Fase Transform)...")
    con = duckdb.connect(str(db_path))
    try:
        query = r"""
            CREATE OR REPLACE TABLE int_usinas_cv AS
            SELECT 
                rud.*,
                s.projeto AS projeto_cv,
                s.spe AS nome_spe_cv
            FROM raw_usinas_detail AS rud
            INNER JOIN seed_spes_cv AS s
                ON regexp_extract(rud.ceg, '\d{6}-\d') = s.ceg
        """
        con.sql(query)
        linhas_cv = con.sql("SELECT COUNT(*) FROM int_usinas_cv").fetchone()
        logger.info(f"Cruzamento concluído! A nova tabela contém {linhas_cv} registros filtrados.")
    except Exception as e:
        logger.error(f"Erro ao cruzar as tabelas: {e}")
        raise
    finally:
        con.close()

def filtrar_raw_usinas_conj(db_path: Path):
    logger.info("Iniciando o filtro de usinas da CDV (Lista VIP)...")
    con = duckdb.connect(str(db_path))
    try:
        query = r"""
            CREATE OR REPLACE TABLE int_cv_conj AS
            SELECT * FROM raw_usinas
            WHERE UPPER(nom_usina) IN (
                SELECT DISTINCT UPPER(
                    CASE 
                        WHEN nom_modalidadeoperacao = 'Tipo II-C' THEN nom_conjuntousina
                        ELSE nom_usina
                    END
                )
                FROM int_usinas_cv
            )
        """
        con.sql(query)
        linhas_finais = con.sql("SELECT COUNT(*) FROM int_cv_conj").fetchone()
        logger.info(f"Filtro aplicado com sucesso! Total de registros mantidos: {linhas_finais}")

        # nom_usina é nullable no Dataset 1 (dicionário ONS) — se NULL, o join por nome falha
        nulos_nom = con.sql("SELECT COUNT(*) FROM int_cv_conj WHERE nom_usina IS NULL").fetchone()[0]
        if nulos_nom > 0:
            logger.warning(
                f"{nulos_nom} registro(s) em int_cv_conj com nom_usina NULL. "
                "Esses conjuntos não serão linkados a nenhuma SPE no join por nome."
            )
    except Exception as e:
        logger.error(f"Erro ao filtrar a tabela: {e}")
        raise
    finally:
        con.close()

def relacionar_spe_conjunto(db_path: Path):
    logger.info("Iniciando a junção final (Fato) com Regras de Soft Drop e Flagging...")
    con = duckdb.connect(str(db_path))
    try:
        query = r"""
            CREATE OR REPLACE TABLE fato_geracao_cv AS
            SELECT 
                spe.* EXCLUDE (id_ons, val_ventoverificado),
                spe.id_ons AS id_ons_spe,
                
                -- ====================================================
                -- REGRA: SOFT DROP (Anula Célula de Vento)
                -- ====================================================
                CASE 
                    WHEN TRY_CAST(spe.val_ventoverificado AS FLOAT) < 0 OR TRY_CAST(spe.val_ventoverificado AS FLOAT) > 40 THEN NULL
                    WHEN TRY_CAST(spe.flg_dadoventoinvalido AS INTEGER) = 1 THEN NULL
                    ELSE TRY_CAST(spe.val_ventoverificado AS FLOAT)
                END AS val_ventoverificado,
                
                conj.id_ons AS id_ons_conjunto,
                conj.nom_subsistema,
                
                -- ====================================================
                -- REGRA: SOFT DROP (Anula Geração Negativa)
                -- ====================================================
                CASE 
                    WHEN TRY_CAST(conj.val_geracao AS FLOAT) < 0 THEN NULL
                    ELSE TRY_CAST(conj.val_geracao AS FLOAT) 
                END AS val_geracao_conjunto,
                
                conj.cod_razaorestricao,
                conj.cod_origemrestricao,
                conj.dsc_restricao,
                conj.val_geracaolimitada AS val_geracaolimitada_conjunto,
                conj.val_disponibilidade AS val_disponibilidade_conjunto,
                conj.val_geracaoreferencia AS val_geracaoreferencia_conjunto,
                conj.val_geracaoreferenciafinal AS val_geracaoreferenciafinal_conjunto,

                -- ====================================================
                -- REGRA: FLAGGING (Sinaliza Limitada > Referência)
                -- ====================================================
                CASE
                    WHEN TRY_CAST(conj.val_geracaolimitada AS FLOAT) > COALESCE(TRY_CAST(conj.val_geracaoreferenciafinal AS FLOAT), TRY_CAST(conj.val_geracaoreferencia AS FLOAT)) THEN 1
                    ELSE 0
                END AS flg_alerta_limitada,

                -- ====================================================
                -- FLAG: val_geracaoestimada calculada por histórico
                -- (quando flg=1, o ONS usa histórico em vez da curva vento×potência)
                -- ====================================================
                CASE
                    WHEN TRY_CAST(spe.flg_dadoventoinvalido AS INTEGER) = 1 THEN 1
                    ELSE 0
                END AS flg_estimada_por_historico

            FROM int_usinas_cv AS spe
            LEFT JOIN int_cv_conj AS conj
                ON UPPER(
                    CASE 
                        WHEN spe.nom_modalidadeoperacao = 'Tipo II-C' THEN spe.nom_conjuntousina
                        ELSE spe.nom_usina
                    END
                ) = UPPER(conj.nom_usina)
                AND spe.din_instante = conj.din_instante
        """
        con.sql(query)
        linhas_finais = con.sql("SELECT COUNT(*) FROM fato_geracao_cv").fetchone()
        logger.info(f"Tabela Fato criada com sucesso! Total de registros unificados: {linhas_finais}")
    except Exception as e:
        logger.error(f"Erro na junção final: {e}")
        raise
    finally:
        con.close()

# =====================================================================
# DEFINIÇÃO DO CONTRATO DE DADOS (PANDERA)
# =====================================================================
schema = DataFrameSchema(
    columns={
        "id_subsistema": Column(nullable=False),
        "nom_subsistema": Column(nullable=False),
        "id_estado": Column(nullable=False),
        "nom_estado": Column(nullable=False),
        "id_ons_spe": Column(nullable=False),
        "id_ons_conjunto": Column(nullable=False),
        "ceg": Column(nullable=False),
        "din_instante": Column(pa.DateTime, nullable=False),
        "val_geracao_conjunto": Column(float, checks=Check.ge(0), nullable=False),
        "val_ventoverificado": Column(float, checks=Check.in_range(0, 40), nullable=True),
        "flg_alerta_limitada": Column(int, checks=Check.isin([0, 1]), nullable=False),
        "val_geracaolimitada_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_disponibilidade_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoreferencia_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoreferenciafinal_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoestimada": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoverificada": Column(float, checks=Check.ge(0), nullable=True),
        "flg_dadoventoinvalido": Column(float, checks=Check.isin([0, 1]), nullable=True),
    }
)

def extrair_e_tratar_pandera(db_path: Path) -> pd.DataFrame:
    logger.info("Iniciando extração e tratamento via Pandera...")

    THRESHOLD_COMPLETUDE = 95.0

    with duckdb.connect(str(db_path)) as con:
        df = con.sql("SELECT * FROM fato_geracao_cv").df()
        seed_df = con.sql("SELECT projeto, spe FROM seed_spes_cv").df()

    print(f"\n{'='*50}\n INICIANDO TRATAMENTO E RELATÓRIO DE QUALIDADE \n{'='*50}")

    linhas_iniciais = len(df)
    arquivo_log_json = Path("relatorio_qualidade.json")

    # Limpeza básica de tipos
    df = df.replace(r'^\s*$', np.nan, regex=True).replace(
        ['null', 'NULL', 'NaN', 'nan', 'N/A', 'n/a', '-'], np.nan
    )
    df['din_instante'] = pd.to_datetime(df['din_instante'], errors='coerce')
    cols_num = df.filter(regex='^val_|^flg_').columns
    df[cols_num] = df[cols_num].apply(pd.to_numeric, errors='coerce')

    # Nulos por coluna ANTES do tratamento (para comparar com depois)
    colunas_metricas = [
        "val_ventoverificado", "val_geracaoestimada", "val_geracaoverificada",
        "val_geracao_conjunto", "val_geracaolimitada_conjunto",
        "val_geracaoreferencia_conjunto", "val_geracaoreferenciafinal_conjunto",
        "val_disponibilidade_conjunto"
    ]
    nulos_antes = {
        col: round(100 * df[col].isna().sum() / max(len(df), 1), 2)
        for col in colunas_metricas if col in df.columns
    }

    # =========================================================
    # DUPLICATAS: separa idênticas (descartar) de conflitantes (quarentena)
    # =========================================================
    chave_dup = ['nome_spe_cv', 'din_instante']
    lista_quarentena = []
    detalhe_anomalias = []

    mask_qualquer_dup = df.duplicated(subset=chave_dup, keep=False)
    df_duplicados = df[mask_qualquer_dup]
    mask_linha_inteira_dup = df_duplicados.duplicated(keep=False)
    df_dup_conflitantes = df_duplicados[~mask_linha_inteira_dup].copy()

    if not df_dup_conflitantes.empty:
        df_dup_conflitantes["motivo_rejeicao"] = (
            "Duplicata conflitante: mesma chave (spe, din_instante) com valores divergentes"
        )
        df_dup_conflitantes["etapa_rejeicao"] = "deduplicacao"
        lista_quarentena.append(df_dup_conflitantes)
        # Remove TODAS as ocorrências da chave conflitante do df principal
        chaves_conf = df_dup_conflitantes[chave_dup].drop_duplicates()
        df = df.merge(chaves_conf, on=chave_dup, how='left', indicator=True)
        df = df[df['_merge'] == 'left_only'].drop(columns='_merge')
        logger.warning(
            f"{len(df_dup_conflitantes)} linha(s) com chave duplicada e valores divergentes "
            "removidas e enviadas para quarentena."
        )

    n_identicas_removidas = int(df.duplicated(subset=chave_dup).sum())
    df = df.drop_duplicates(subset=chave_dup).reset_index(drop=True)

    # =========================================================
    # VALIDAÇÃO PANDERA: quarentena em vez de hard drop silencioso
    # =========================================================
    try:
        df_clean = schema.validate(df, lazy=True)
        indices_falhos = []
    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        indices_falhos = list(failure_df['index'].dropna().unique().astype(int))

        df_rejeitado = df.loc[df.index.isin(indices_falhos)].copy()
        motivos_por_idx = (
            failure_df.groupby("index")["check"]
            .apply(lambda x: "; ".join(x.astype(str)))
            .to_dict()
        )
        df_rejeitado["motivo_rejeicao"] = df_rejeitado.index.map(
            lambda i: f"Pandera: {motivos_por_idx.get(i, 'violação de schema')}"
        )
        df_rejeitado["etapa_rejeicao"] = "pandera_schema"
        lista_quarentena.append(df_rejeitado)

        for idx in indices_falhos:
            row_data = df.loc[idx]
            motivos = failure_df[failure_df['index'] == idx]
            txt_motivos = [
                f"Coluna [{m['column']}] violou [{m['check']}]"
                for _, m in motivos.iterrows()
            ]
            ts = row_data.get('din_instante', 'N/A')
            ts_str = ts.strftime('%d/%m/%Y %H:%M') if isinstance(ts, pd.Timestamp) else str(ts)
            detalhe_anomalias.append({
                "nome": str(row_data.get('nome_spe_cv', 'N/A')),
                "estado": str(row_data.get('id_estado', 'N/A')),
                "subsistema": str(row_data.get('nom_subsistema', 'N/A')),
                "din_instante": ts_str,
                "justificativa": "QUARENTENA: " + " | ".join(txt_motivos)
            })

        df_clean = df.loc[~df.index.isin(indices_falhos)].copy()
        logger.warning(f"{len(indices_falhos)} linha(s) enviadas para quarentena (schema Pandera).")

    # Salva arquivo de quarentena (acumula todos os tipos de rejeição)
    n_quarentena = 0
    if lista_quarentena:
        df_quarentena = pd.concat(lista_quarentena, ignore_index=True)
        n_quarentena = len(df_quarentena)
        QUARENTENA_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_quarentena.to_parquet(QUARENTENA_PATH, index=False, compression="snappy")
        logger.info(f"Quarentena: {n_quarentena} registro(s) salvos em {QUARENTENA_PATH}")

    # =========================================================
    # SOFT DROPS E FLAGS (registra no JSON para auditoria)
    # =========================================================
    df_anomalias_soft = df_clean[
        (df_clean['val_ventoverificado'].isna()) |
        (df_clean['val_geracao_conjunto'].isna()) |
        (df_clean['flg_alerta_limitada'] == 1)
    ]
    for _, row in df_anomalias_soft.iterrows():
        motivos_soft = []
        if pd.isna(row['val_ventoverificado']):
            motivos_soft.append("Vento anulado (inválido, fora de 0–40 m/s ou flag de telemetria ativa)")
        if pd.isna(row['val_geracao_conjunto']):
            motivos_soft.append("Geração do conjunto anulada (valor negativo)")
        if row['flg_alerta_limitada'] == 1:
            motivos_soft.append("Geração limitada excede a geração de referência")
        ts_str = row['din_instante'].strftime('%d/%m/%Y %H:%M') if isinstance(row['din_instante'], pd.Timestamp) else str(row['din_instante'])
        detalhe_anomalias.append({
            "nome": str(row.get('nome_spe_cv', 'N/A')),
            "estado": str(row.get('id_estado', 'N/A')),
            "subsistema": str(row.get('nom_subsistema', 'N/A')),
            "din_instante": ts_str,
            "justificativa": "SOFT DROP/FLAG: " + " ; ".join(motivos_soft)
        })

    # =========================================================
    # MÉTRICAS GLOBAIS
    # =========================================================
    qtd_vento_nulo    = int(df_clean['val_ventoverificado'].isna().sum())
    qtd_geracao_nula  = int(df_clean['val_geracao_conjunto'].isna().sum())
    qtd_alertas       = int((df_clean['flg_alerta_limitada'] == 1).sum())
    qtd_flag_invalido = int((df_clean['flg_dadoventoinvalido'] == 1).sum())
    qtd_flag_null     = int(df_clean['flg_dadoventoinvalido'].isna().sum())
    qtd_flag_valido   = len(df_clean) - qtd_flag_invalido - qtd_flag_null
    spes_com_dados    = int(df_clean['nome_spe_cv'].nunique()) if 'nome_spe_cv' in df_clean.columns else 0
    spes_sem_conj     = int(df_clean['id_ons_conjunto'].isna().sum()) if 'id_ons_conjunto' in df_clean.columns else 0

    # Regras de negócio contadas no df pré-pandera (o que existia antes do tratamento)
    geracao_neg   = int((df['val_geracao_conjunto'].dropna() < 0).sum()) if 'val_geracao_conjunto' in df.columns else 0
    vento_fora    = int(((df['val_ventoverificado'] < 0) | (df['val_ventoverificado'] > 40)).sum()) if 'val_ventoverificado' in df.columns else 0
    vento_flag_b  = int((df['flg_dadoventoinvalido'] == 1).sum()) if 'flg_dadoventoinvalido' in df.columns else 0

    # Nulos por coluna DEPOIS do tratamento
    nulos_depois = {
        col: round(100 * df_clean[col].isna().sum() / max(len(df_clean), 1), 2)
        for col in colunas_metricas if col in df_clean.columns
    }
    nulos_por_coluna = {
        col: {"antes_pct": nulos_antes.get(col, 0), "depois_pct": nulos_depois.get(col, 0)}
        for col in colunas_metricas if col in df_clean.columns
    }

    # Distribuições numéricas (min/p25/p50/p75/max/media)
    distribuicoes = {}
    for col in colunas_metricas:
        if col in df_clean.columns and df_clean[col].notna().any():
            s = df_clean[col].dropna()
            distribuicoes[col] = {
                "min":   round(float(s.min()), 4),
                "p25":   round(float(s.quantile(0.25)), 4),
                "p50":   round(float(s.median()), 4),
                "p75":   round(float(s.quantile(0.75)), 4),
                "max":   round(float(s.max()), 4),
                "media": round(float(s.mean()), 4)
            }

    # =========================================================
    # COMPLETUDE POR PROJETO
    # Denominador FIXO: período esperado da coleta (não derivado dos dados).
    # Usar o range observado como denominador sempre dá ~100% — é como medir
    # sua própria altura contra você mesmo. O denominador certo é o período
    # contratado (out/2025–mar/2026), independente do que chegou.
    # =========================================================
    completude_proj = {}
    if 'din_instante' in df_clean.columns and 'projeto_cv' in df_clean.columns and not df_clean.empty:
        # Período fixo da coleta: usa o primeiro e último dia dos dados reais
        # mas impõe limites ao nível de mês, não de timestamp observado
        min_ts_obs = df_clean['din_instante'].min()
        max_ts_obs = df_clean['din_instante'].max()

        # Denominador: início do primeiro mês até fim do último mês (dias completos)
        inicio_periodo = min_ts_obs.replace(day=1, hour=0, minute=0, second=0)
        # Último dia do mês de max_ts
        import calendar
        ultimo_dia = calendar.monthrange(max_ts_obs.year, max_ts_obs.month)[1]
        fim_periodo = max_ts_obs.replace(day=ultimo_dia, hour=23, minute=30, second=0)
        n_periodos_fixo = len(pd.date_range(start=inicio_periodo, end=fim_periodo, freq="30min"))

        spes_por_projeto = seed_df.groupby('projeto')['spe'].count().to_dict()
        spes_lista = seed_df.groupby('projeto')['spe'].apply(list).to_dict()

        for projeto, n_spes in spes_por_projeto.items():
            esperado = n_spes * n_periodos_fixo

            # Conta timestamps DISTINTOS por SPE (não soma de linhas do projeto)
            df_proj = df_clean[df_clean['projeto_cv'] == projeto]
            recebido_por_spe = (
                df_proj.groupby('nome_spe_cv')['din_instante']
                .nunique()
            )
            recebido = int(recebido_por_spe.sum())

            # SPEs que estão no seed mas sem nenhum dado recebido
            spes_sem_dado = [
                s for s in spes_lista.get(projeto, [])
                if s not in recebido_por_spe.index
            ]

            pct_envio = round(100 * recebido / esperado, 1) if esperado > 0 else 0.0

            # Completude de dados válidos: timestamps com val_geracao_conjunto não-nulo
            recebido_valido = int(
                df_proj.dropna(subset=['val_geracao_conjunto'])
                .groupby('nome_spe_cv')['din_instante']
                .nunique()
                .sum()
            )
            pct_valido = round(100 * recebido_valido / esperado, 1) if esperado > 0 else 0.0

            # Gaps e completude POR SPE individual
            gaps_total = 0
            detalhe_por_spe = {}
            for spe_nome, grupo_spe in df_proj.groupby('nome_spe_cv'):
                grupo_ord = grupo_spe.sort_values('din_instante')
                gaps_spe = int((grupo_ord['din_instante'].diff() > pd.Timedelta(minutes=45)).sum())
                gaps_total += gaps_spe
                rec_spe = int(grupo_ord['din_instante'].nunique())
                pct_spe = round(100 * rec_spe / n_periodos_fixo, 1)
                # Inclui no detalhe apenas SPEs com completude < 100% ou gaps
                if pct_spe < 100.0 or gaps_spe > 0:
                    detalhe_por_spe[spe_nome] = {
                        "recebido": rec_spe,
                        "esperado": n_periodos_fixo,
                        "pct": pct_spe,
                        "gaps": gaps_spe
                    }

            completude_proj[projeto] = {
                "spes_esperadas": int(n_spes),
                "spes_sem_dado": spes_sem_dado,
                "n_periodos_fixo_por_spe": n_periodos_fixo,
                "esperado_total": esperado,
                "recebido_total": recebido,
                "pct_envio": pct_envio,
                "recebido_valido": recebido_valido,
                "pct_dados_validos": pct_valido,
                "gaps_detectados": gaps_total,
                "alerta": pct_envio < THRESHOLD_COMPLETUDE or pct_valido < THRESHOLD_COMPLETUDE,
                "spes_com_problema": detalhe_por_spe  # vazio se todas 100%
            }

    # =========================================================
    # FRESHNESS CHECK
    # =========================================================
    freshness_info = {"status": "FALHA", "ok": False, "ultimo_mes_esperado": "N/A", "mensagem": "Sem dados"}
    if 'din_instante' in df_clean.columns and not df_clean.empty:
        max_ts_f = df_clean['din_instante'].max()
        agora = pd.Timestamp.now()
        mes_esp = 12 if agora.month == 1 else agora.month - 1
        ano_esp = agora.year - 1 if agora.month == 1 else agora.year
        freshness_info["ultimo_mes_esperado"] = f"{mes_esp:02d}/{ano_esp}"
        if max_ts_f.year == ano_esp and max_ts_f.month == mes_esp:
            freshness_info.update({"status": "OK", "ok": True,
                "mensagem": f"Último registro: {max_ts_f.strftime('%d/%m/%Y %H:%M')}"})
        elif (max_ts_f.year > ano_esp) or (max_ts_f.year == ano_esp and max_ts_f.month > mes_esp):
            freshness_info.update({"status": "ALERTA",
                "mensagem": f"Base contém dados do mês atual ({max_ts_f.strftime('%m/%Y')})"})
        else:
            freshness_info.update({"status": "FALHA",
                "mensagem": f"Dados desatualizados. Último: {max_ts_f.strftime('%d/%m/%Y %H:%M')}"})

    # =========================================================
    # JSON EXPANDIDO
    # =========================================================
    periodo = {
        "inicio": df_clean['din_instante'].min().strftime('%Y-%m-%d') if not df_clean.empty else "N/A",
        "fim":    df_clean['din_instante'].max().strftime('%Y-%m-%d') if not df_clean.empty else "N/A"
    }
    relatorio_final_json = {
        "data_processamento": pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S'),
        "periodo_dados": periodo,
        "volumetria": {
            "total_registros_brutos":                  linhas_iniciais,
            "duplicatas_identicas_removidas":          n_identicas_removidas,
            "duplicatas_conflitantes_para_quarentena": len(df_dup_conflitantes) if not df_dup_conflitantes.empty else 0,
            "hard_drops_pandera":                      len(indices_falhos),
            "total_em_quarentena":                     n_quarentena,
            "registros_limpos":                        len(df_clean),
            "vento_anulado_soft_drop":                 qtd_vento_nulo,
            "geracao_conj_anulada_soft_drop":          qtd_geracao_nula,
            "alertas_limitada_flag":                   qtd_alertas
        },
        "integridade_referencial": {
            "spes_esperadas": 47,
            "spes_com_dados": spes_com_dados,
            "registros_sem_conjunto_apos_join": spes_sem_conj
        },
        "estados_vento": {
            "valido_flag_0":                             qtd_flag_valido,
            "invalido_flag_1_estimativa_por_historico":  qtd_flag_invalido,
            "sem_dado_flag_null":                        qtd_flag_null
        },
        "regras_negocio": {
            "geracao_negativa_bruta":     {"falhas": geracao_neg,    "acao": "campo_nullificado"},
            "vento_fora_faixa_0_40ms":    {"falhas": vento_fora,     "acao": "campo_nullificado"},
            "vento_flag_invalido":         {"falhas": vento_flag_b,   "acao": "campo_nullificado_flg_historico=1"},
            "limitada_maior_referencia":   {"falhas": qtd_alertas,    "acao": "flg_alerta_limitada=1"},
            "geracao_conj_nula_pos_join":  {"falhas": qtd_geracao_nula, "acao": "alerta_join_incompleto"}
        },
        "nulos_por_coluna":       nulos_por_coluna,
        "distribuicao_numerica":  distribuicoes,
        "completude_por_projeto": completude_proj,
        "freshness":              freshness_info,
        "quarentena": {
            "total":   n_quarentena,
            "arquivo": str(QUARENTENA_PATH) if n_quarentena > 0 else None
        },
        "anomalias_detectadas": detalhe_anomalias
    }

    with open(arquivo_log_json, "w", encoding="utf-8") as f:
        json.dump(relatorio_final_json, f, indent=4, ensure_ascii=False)
    logger.info(f"Relatório de qualidade salvo em '{arquivo_log_json}'.")

    # =========================================================
    # SALVA GOLD PARQUET
    # =========================================================
    try:
        GOLD_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_parquet(GOLD_PARQUET_PATH, index=False, compression="snappy")
        logger.info(f"Parquet Gold salvo em: {GOLD_PARQUET_PATH}")
    except Exception as e:
        logger.error(f"Erro ao salvar Parquet: {e}")
        raise

    # =========================================================
    # RELATÓRIO NO TERMINAL
    # =========================================================
    print("\n--- VOLUMETRIA ---")
    print(f"  Brutos:                    {linhas_iniciais}")
    print(f"  Duplicatas idênticas:      -{n_identicas_removidas}")
    print(f"  Quarentena (conflitantes): -{len(df_dup_conflitantes) if not df_dup_conflitantes.empty else 0}")
    print(f"  Quarentena (Pandera):      -{len(indices_falhos)}")
    print(f"  Limpos:                    {len(df_clean)}")
    if n_quarentena > 0:
        print(f"  Arquivo de quarentena:     {QUARENTENA_PATH}")

    print("\n--- SOFT DROPS E FLAGS ---")
    print(f"  Vento anulado:             {qtd_vento_nulo}")
    print(f"  Geração conj. anulada:     {qtd_geracao_nula}")
    print(f"  Alertas limitada>ref:      {qtd_alertas}")

    print("\n--- ESTADOS DO VENTO (flg_dadoventoinvalido) ---")
    print(f"  Válido   (flag=0):         {qtd_flag_valido}")
    print(f"  Inválido (flag=1, hist.):  {qtd_flag_invalido}")
    print(f"  Sem dado (flag=NULL):      {qtd_flag_null}")

    print("\n--- INTEGRIDADE REFERENCIAL ---")
    print(f"  SPEs esperadas: 47 | com dados: {spes_com_dados}")
    if spes_sem_conj > 0:
        print(f"  ALERTA: {spes_sem_conj} registros sem conjunto mapeado após o join")

    print(f"\n--- FRESHNESS ---")
    print(f"  {freshness_info['status']}: {freshness_info['mensagem']}")

    print(f"\n--- COMPLETUDE POR PROJETO (threshold: {THRESHOLD_COMPLETUDE}%) ---")
    for proj, info in completude_proj.items():
        alerta_str = " <- ALERTA" if info["alerta"] else ""
        print(
            f"  {proj}: envio {info['pct_envio']}% | dados válidos {info['pct_dados_validos']}%"
            f" ({info['recebido_total']}/{info['esperado_total']}) | gaps: {info['gaps_detectados']}{alerta_str}"
        )
        if info["spes_sem_dado"]:
            print(f"    SPEs sem nenhum dado: {info['spes_sem_dado']}")

    print("\n==========================================")
    logger.info("Tratamento via Pandera concluído!")
    return df_clean

if __name__ == "__main__":
    load_duckdb(DB_PATH, RAW_USINAS_DIR, RAW_USINAS_DETAIL_DIR)
    spes_cvd_data_extract(DB_PATH)
    filtrar_raw_usinas_conj(DB_PATH)
    relacionar_spe_conjunto(DB_PATH)
    extrair_e_tratar_pandera(DB_PATH)