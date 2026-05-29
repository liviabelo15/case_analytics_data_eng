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

# === CAMINHO PARA SALVAR O ARQUIVO PARQUET TRATADO ===
GOLD_OUTPUT_DIR = Path("data/modeled")
GOLD_PARQUET_PATH = GOLD_OUTPUT_DIR / "fato_geracao_cv_tratado.parquet"
# ==========================================================

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
                END AS flg_alerta_limitada
                
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
        "val_geracao_conjunto": Column(float, checks=Check.ge(0), nullable=True),
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
    
    with duckdb.connect(str(db_path)) as con:
        df = con.sql("SELECT * FROM fato_geracao_cv").df()

    print(f"\n{'='*50}\n INICIANDO TRATAMENTO E RELATÓRIO DE QUALIDADE \n{'='*50}")
    
    linhas_iniciais = len(df)
    arquivo_log_json = Path("relatorio_qualidade.json")
    
    # 1. Limpeza básica para o Pandera não quebrar
    df = df.replace(r'^\s*$', np.nan, regex=True).replace(['null', 'NULL', 'NaN', 'nan', 'N/A', 'n/a', '-'], np.nan)
    df['din_instante'] = pd.to_datetime(df['din_instante'], errors='coerce')
    
    cols_num = df.filter(regex='^val_|^flg_').columns
    df[cols_num] = df[cols_num].apply(pd.to_numeric, errors='coerce')

    # Duplicatas (Hard Drop)
    duplicatas_iniciais = df.duplicated(subset=['nome_spe_cv', 'din_instante']).sum()
    df = df.drop_duplicates(subset=['nome_spe_cv', 'din_instante'])
    df = df.reset_index(drop=True)

    # Lista mestra onde guardaremos todas as justificativas estruturadas
    detalhe_anomalias = []

    # 2. APLICAÇÃO DO CONTRATO (PANDERA) - DETECÇÃO DE HARD DROPS
    try:
        df_clean = schema.validate(df, lazy=True)
        indices_falhos = []
    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        indices_falhos = failure_df['index'].dropna().unique().astype(int)
        
        # Registra os descartes críticos (Hard Drops) no JSON
        for idx in indices_falhos:
            row_data = df.loc[idx]
            motivos = failure_df[failure_df['index'] == idx]
            txt_motivos = [f"Coluna [{m['column']}] violou a regra [{m['check']}]" for _, m in motivos.iterrows()]
            
            timestamp = row_data.get('din_instante', 'N/A')
            timestamp_str = timestamp.strftime('%d/%m/%Y %H:%M') if isinstance(timestamp, pd.Timestamp) else str(timestamp)
            
            detalhe_anomalias.append({
                "nome": str(row_data.get('nome_spe_cv', 'N/A')),
                "estado": str(row_data.get('id_estado', 'N/A')),
                "subsistema": str(row_data.get('nom_subsistema', 'N/A')),
                "din_instante": timestamp_str,
                "justificativa": "DESCARTE CRÍTICO (Linha removida): " + " | ".join(txt_motivos)
            })
            
        df_clean = df.drop(index=indices_falhos)

    # =====================================================================
    # DETECÇÃO DE ANOMALIAS OPERACIONAIS (SOFT DROPS E FLAGS) PARA O JSON
    # =====================================================================
    # Filtramos de forma performática apenas as linhas que possuem algum evento atípico
    df_anomalias_soft = df_clean[
        (df_clean['val_ventoverificado'].isna()) | 
        (df_clean['val_geracao_conjunto'].isna()) | 
        (df_clean['flg_alerta_limitada'] == 1)
    ]

    for _, row in df_anomalias_soft.iterrows():
        justificativas_linha = []
        if pd.isna(row['val_ventoverificado']):
            justificativas_linha.append("Velocidade do vento inválida, negativa, superior a 40 m/s ou flag de falha de telemetria ativa (Dado Anulado)")
        if pd.isna(row['val_geracao_conjunto']):
            justificativas_linha.append("Geração de energia com valor negativo reportada pelo ONS (Dado Anulado)")
        if row['flg_alerta_limitada'] == 1:
            justificativas_linha.append("Inconsistência Contábil: Geração limitada excede a geração de referência oficial (Sinalizado via Flag)")
            
        timestamp_str = row['din_instante'].strftime('%d/%m/%Y %H:%M') if isinstance(row['din_instante'], pd.Timestamp) else str(row['din_instante'])
        
        detalhe_anomalias.append({
            "nome": str(row.get('nome_spe_cv', 'N/A')),
            "estado": str(row.get('id_estado', 'N/A')),
            "subsistema": str(row.get('nom_subsistema', 'N/A')),
            "din_instante": timestamp_str,
            "justificativa": "ANOMALIA: " + " ; ".join(justificativas_linha)
        })

    # Coleta de Métricas Globais para o Cabeçalho do JSON e do Terminal
    qtd_vento_nulo = df_clean['val_ventoverificado'].isna().sum()
    qtd_geracao_nula = df_clean['val_geracao_conjunto'].isna().sum()
    qtd_alertas_limitada = (df_clean['flg_alerta_limitada'] == 1).sum()
    registros_descartados = len(indices_falhos)

    # =====================================================================
    # ESCRITA DO ARQUIVO JSON (Sempre sobrescrevendo com modo 'w')
    # =====================================================================
    relatorio_final_json = {
        "data_processamento": pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S'),
        "resumo_executivo": {
            "total_registros_brutos": linhas_iniciais,
            "duplicatas_removidas": int(duplicatas_iniciais),
            "descartes_totais_hard_drop": int(registros_descartados),
            "vento_anulado_soft_drop": int(qtd_vento_nulo),
            "geracao_anulada_soft_drop": int(qtd_geracao_nula),
            "alertas_limitada_flag": int(qtd_alertas_limitada)
        },
        "anomalias_detectadas": detalhe_anomalias
    }

    with open(arquivo_log_json, "w", encoding="utf-8") as f:
        json.dump(relatorio_final_json, f, indent=4, ensure_ascii=False)
    print(f"ℹ️ Sucesso! Arquivo '{arquivo_log_json}' atualizado com as justificativas estruturadas.")

    # =====================================================================
    # NOVA ANÁLISE 1: COMPLETUDE (Envio ONS vs Dados Úteis)
    # =====================================================================
    completude_relatorio = []
    THRESHOLD_COMPLETUDE = 95.0 
    
    if 'din_instante' in df_clean.columns and 'nome_spe_cv' in df_clean.columns:
        for spe, grupo in df_clean.groupby('nome_spe_cv'):
            min_data = grupo['din_instante'].min()
            max_data = grupo['din_instante'].max()
            
            if pd.notna(min_data) and pd.notna(max_data):
                range_esperado = pd.date_range(start=min_data, end=max_data, freq='30min')
                total_esperado = len(range_esperado)
                
                total_recebido = grupo['din_instante'].nunique()
                
                # Consideramos dados úteis apenas se vento E geração não forem nulos
                grupo_valido = grupo.dropna(subset=['val_geracao_conjunto', 'val_ventoverificado'])
                total_uteis = grupo_valido['din_instante'].nunique()
                
                perc_envio = (total_recebido / total_esperado * 100) if total_esperado > 0 else 0
                perc_uteis = (total_uteis / total_esperado * 100) if total_esperado > 0 else 0
                
                status = "🟢 OK" if perc_uteis >= THRESHOLD_COMPLETUDE else "🔴 ALERTA DE QUALIDADE"
                completude_relatorio.append(
                    f" - {spe}: Envio ONS: {perc_envio:.1f}% | Dados Úteis: {perc_uteis:.1f}% ({total_uteis}/{total_esperado}) {status}"
                )

    # =====================================================================
    # NOVA ANÁLISE 2: FRESHNESS CHECK
    # =====================================================================
    freshness_status = "🔴 FALHA: Sem dados do último mês completo na base."
    if 'din_instante' in df_clean.columns and not df_clean.empty:
        max_timestamp = df_clean['din_instante'].max()
        data_atual = pd.Timestamp.now()
        mes_esperado = 12 if data_atual.month == 1 else data_atual.month - 1
        ano_esperado = data_atual.year - 1 if data_atual.month == 1 else data_atual.year
            
        if max_timestamp.year == ano_esperado and max_timestamp.month == mes_esperado:
            freshness_status = f"🟢 SUCESSO: Dados validados com o último mês fechado! Último registro em: {max_timestamp.strftime('%d/%m/%Y %H:%M')}"
        elif (max_timestamp.year > ano_esperado) or (max_timestamp.year == ano_esperado and max_timestamp.month > mes_esperado):
            freshness_status = f"⚠️ ALERTA: A base contém dados do mês atual ({max_timestamp.strftime('%m/%Y')}), mas o critério de fechamento esperava apenas o mês anterior completo ({mes_esperado:02d}/{ano_esperado})."
        else:
            freshness_status = f"🔴 FALHA: Dados desatualizados ou incompletos. Último registro em: {max_timestamp.strftime('%d/%m/%Y %H:%M')} (Esperado safra fechada: {mes_esperado:02d}/{ano_esperado})"

    # =====================================================================
    # 3. ANÁLISE DE CONTINUIDADE (GAPS OPERACIONAIS REAIS)
    # =====================================================================
    gaps_relatorio = []
    if 'din_instante' in df_clean.columns and 'nome_spe_cv' in df_clean.columns:
        # Gaps operacionais calculados apenas sobre as séries temporais limpas de dados nulos
        df_validos = df_clean.dropna(subset=['val_geracao_conjunto', 'val_ventoverificado']).sort_values(by=['nome_spe_cv', 'din_instante'])
        df_validos['tempo_diff'] = df_validos.groupby('nome_spe_cv')['din_instante'].diff()
        gaps = df_validos[df_validos['tempo_diff'] > pd.Timedelta(minutes=45)]
        
        if not gaps.empty:
            for spe, count in gaps.groupby('nome_spe_cv').size().items():
                gaps_relatorio.append(f" - {spe}: {count} descontinuidades operacionais (>45m) reais encontradas.")

    # =====================================================================
    # SALVANDO A BASE TRATADA EM PARQUET (CAMINHO GOLD)
    # =====================================================================
    try:
        GOLD_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Gravando arquivo Parquet tratado em: {GOLD_PARQUET_PATH}...")
        df_clean.to_parquet(GOLD_PARQUET_PATH, index=False, compression="snappy")
        logger.info("Arquivo Parquet salvo com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo Parquet: {e}")
        raise

    # =====================================================================
    # RELATÓRIO FINAL EXPANDIDO NO TERMINAL
    # =====================================================================
    linhas_finais = len(df_clean)
    perc = (registros_descartados / linhas_iniciais) * 100 if linhas_iniciais > 0 else 0
    
    print("\n--- RELATÓRIO DE QUALIDADE E INTEGRIDADE DE DADOS ---")
    print(f"Total de registros originais: {linhas_iniciais}")
    print(f"Total de registros após limpeza: {linhas_finais}")
    print(f"Total de registros descartados (Hard Drop): {registros_descartados} ({perc:.2f}%)")
    print(f"Total de linhas duplicadas removidas: {duplicatas_iniciais}")
    
    print("\n--- ANOMALIAS TRATADAS (SOFT DROPS E ALERTAS) ---")
    print(f"⚠️ Medições de Vento anuladas (Inválido ou <0/>40): {qtd_vento_nulo} registros")
    print(f"⚠️ Medições de Geração anuladas (<0): {qtd_geracao_nula} registros")
    print(f"🚩 Alertas de Regra de Negócio (Limitada > Referência): {qtd_alertas_limitada} registros marcados")
    
    print("\n--- FRESHNESS CHECK (ATUALIDADE DA BASE) ---")
    print(freshness_status)
    
    print(f"\n--- COMPLETUDE DA SÉRIE TEMPORAL (LIMITE DE ALERTA: {THRESHOLD_COMPLETUDE}%) ---")
    if completude_relatorio:
        for relato in completude_relatorio: print(relato)
    else:
        print(" - Não foi possível calcular a completude (colunas ausentes).")
    
    print("\n--- CONTINUIDADE TEMPORAL (GAPS OPERACIONAIS) ---")
    if gaps_relatorio:
        for relato in gaps_relatorio: print(relato)
    else:
        print(" - Perfeito! Todos os timestamps com dados úteis estão contínuos (sem gaps reais de +45min).")
            
    print("\n==========================================")
    logger.info("Tratamento via Pandera concluído!")
    return df_clean

if __name__ == "__main__":
    load_duckdb(DB_PATH, RAW_USINAS_DIR, RAW_USINAS_DETAIL_DIR)
    spes_cvd_data_extract(DB_PATH)
    filtrar_raw_usinas_conj(DB_PATH)
    relacionar_spe_conjunto(DB_PATH)
    extrair_e_tratar_pandera(DB_PATH)