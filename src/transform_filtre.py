import pandas as pd
import duckdb
import json
import pandera.pandas as pa
from pandera.pandas import Column, Check
from pathlib import Path
import logging
import re

# =============================================================================
# 0. CONFIGURAÇÕES INICIAIS
# =============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RAW_DIR = Path("data/raw")
MODELED_DIR = Path("data/modeled")
MODELED_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# PASSO 1: EXTRAÇÃO (LOAD DA LANDING ZONE)
# =============================================================================

def extrair_dados_brutos():
    logging.info("Iniciando leitura dos arquivos brutos no Pandas...")
    
    arquivos_spe = list(RAW_DIR.glob("*DETAIL*.parquet"))
    arquivos_complexo = list(RAW_DIR.glob("*RESTRICAO_COFF_EOLICA_2*.parquet"))
    
    df_spe = pd.concat([pd.read_parquet(f) for f in arquivos_spe], ignore_index=True)
    df_complexo = pd.concat([pd.read_parquet(f) for f in arquivos_complexo], ignore_index=True)
    df_mestre = pd.read_csv("spes_casa_dos_ventos.csv", sep=",")

    return df_spe, df_complexo, df_mestre

# =============================================================================
# PASSO 2: FILTRAGEM (EARLY FILTERING) 
# =============================================================================

def filtro_spe_cdv(df_spe, df_complexo, df_mestre):
    logging.info("Aplicando Early Filtering - SPEs CDV")

    lista_cegs_mestre = df_mestre['ceg'].dropna().astype(str).str.strip().tolist()

    df_spe['ceg_limpo'] = df_spe['ceg'].astype(str).str.extract(r'(\d+-\d)', expand=False)


    df_spe = df_spe[df_spe['ceg_limpo'].isin(lista_cegs_mestre)].copy()
    df_spe['ceg'] = df_spe['ceg_limpo']
    df_spe = df_spe.drop(columns=['ceg_limpo'])

    df_spe['complexo_derivado_chave'] = df_spe['nom_usina'].apply(extrair_radical)
    df_complexo['complexo_derivado_chave'] = df_complexo['nom_usina'].apply(extrair_radical)

    complexos_cdv = df_spe['complexo_derivado_chave'].unique()
    df_complexo = df_complexo[df_complexo['complexo_derivado_chave'].isin(complexos_cdv)].copy()

    return df_spe, df_complexo

# =============================================================================
# PASSO 3: LIMPEZA, TIPAGEM E REGRAS DE NEGÓCIO (REGULATÓRIO ONS)
# =============================================================================

def limpeza_e_tipagem_dados(df_spe, df_complexo):
    """Aplica tipagem correta, remove gaps temporais e descarta dados corrompidos."""
    logging.info("[PASSO 3] Aplicando tipagem e descartando anemometria inválida...")
    
    # Renomeia ID para evitar colisão
    df_spe = df_spe.rename(columns={'id_ons': 'spe_id_ons'})

    # Tipagem de Datas
    df_spe['din_instante'] = pd.to_datetime(df_spe['din_instante'])
    df_complexo['din_instante'] = pd.to_datetime(df_complexo['din_instante'])

    # Validação de Gaps Temporais (30 min)
    df_sorted = df_spe.sort_values(by=['nom_usina', 'din_instante']).copy()
    df_sorted['delta_tempo'] = df_sorted.groupby('nom_usina')['din_instante'].diff()
    gaps = df_sorted[df_sorted['delta_tempo'].notna() & (df_sorted['delta_tempo'] != pd.Timedelta(minutes=30))]
    
    if not gaps.empty:
        logging.warning(f"ATENÇÃO: Detectados {len(gaps)} quebras de continuidade temporal!")
    df_spe = df_sorted.drop(columns=['delta_tempo'])

    # Filtro Regulatório (Flag de Vento Inválido)
    if 'flg_dadoventoinvalido' in df_spe.columns:
        df_spe['flg_dadoventoinvalido'] = pd.to_numeric(df_spe['flg_dadoventoinvalido'], errors='coerce')
        df_spe = df_spe[(df_spe['flg_dadoventoinvalido'] != 1.0) | (df_spe['flg_dadoventoinvalido'].isna())].copy()

    # Coerção Numérica (Mantendo nulos permitidos pelo dicionário)
    for col in ["val_geracaoverificada", "val_ventoverificado", "flg_dadoventoinvalido", "val_geracaoestimada"]:
        if col in df_spe.columns: df_spe[col] = pd.to_numeric(df_spe[col], errors='coerce').astype('float64')
            
    for col in ["val_geracaolimitada", "val_geracaoreferenciafinal"]:
        if col in df_complexo.columns: df_complexo[col] = pd.to_numeric(df_complexo[col], errors='coerce').astype('float64')

    return df_spe, df_complexo

# =============================================================================
# PASSO 4: VALIDAÇÃO DE QUALIDADE (PANDERA E RELATÓRIO)
# =============================================================================
def gerar_relatorio_qualidade(df: pd.DataFrame, nome_base: str):
    """Imprime estatísticas de completude (nulos e zeros)."""
    total = len(df)
    if total == 0:
        return logging.warning(f"A base {nome_base} está vazia!")
        
    logging.info(f"--- Relatório: {nome_base} ({total} registros) ---")
    
    # Avalia nulos
    nulos = df.isnull().sum()
    for col, qtd in nulos[nulos > 0].items():
        logging.info(f"  * Nulos em {col}: {qtd} ({(qtd / total) * 100:.2f}%)")
        
    # Avalia zeros nas colunas numéricas
    num_cols = df.select_dtypes(include=['float64', 'int64']).columns
    zeros = (df[num_cols] == 0).sum()
    for col, qtd in zeros[zeros > 0].items():
        logging.info(f"  * Zeros em {col}: {qtd} ({(qtd / total) * 100:.2f}%)")

def validar_matematica_dos_dados(df_spe, df_complexo):
    """Usa Pandera para garantir que leis da física e termodinâmica não foram violadas."""
    logging.info("[PASSO 4] Acionando validação rigorosa (Pandera)...")
    
    schema_spe = pa.DataFrameSchema({
        "val_geracaoverificada": Column(float, Check.greater_than_or_equal_to(0.0), nullable=True),
        "val_ventoverificado": Column(float, Check.in_range(0.0, 40.0), nullable=True),
        "flg_dadoventoinvalido": Column(float, Check.isin([0.0, 1.0]), nullable=True),
        "val_geracaoestimada": Column(float, Check.greater_than_or_equal_to(0.0), nullable=True)
    })

    schema_complexo = pa.DataFrameSchema(
        columns={
            "val_geracaolimitada": Column(float, Check.greater_than_or_equal_to(0.0), nullable=True),
            "val_geracaoreferenciafinal": Column(float, Check.greater_than_or_equal_to(0.0), nullable=True)
        },
        checks=pa.Check(
            lambda df: df["val_geracaolimitada"] <= df["val_geracaoreferenciafinal"],
            name="check_proporcao_limitante", ignore_na=True
        )
    )

    try:
        df_spe = schema_spe.validate(df_spe)
        df_complexo = schema_complexo.validate(df_complexo)
        
        gerar_relatorio_qualidade(df_spe, "Detalhamento (SPE)")
        gerar_relatorio_qualidade(df_complexo, "Geral (Complexos)")
        return df_spe, df_complexo
        
    except pa.errors.SchemaError as e:
        logging.error(f"Erro Crítico de Qualidade: {e}")
        raise

# =============================================================================
# PASSO 5: MODELAGEM DIMENSIONAL ELT (DUCKDB)
# =============================================================================
def executar_modelagem_duckdb(df_spe, df_complexo, df_mestre):
    """Realiza o Join e traduz os nomes das colunas usando SQL em memória."""
    logging.info("[PASSO 5] Modelando Star Schema no DuckDB e salvando Parquet...")
    
    con = duckdb.connect(database=':memory:')
    con.register('tb_spe', df_spe)
    con.register('tb_complexo', df_complexo)
    con.register('tb_mestre', df_mestre)
    
    query_fato = """
        CREATE TABLE fato_restricao AS
        SELECT 
            s.din_instante,
            m.projeto,
            m.ceg,
            s.spe_id_ons,
            c.cod_razaorestricao,
            c.cod_origemrestricao,
            c.dsc_restricao,
            
            -- TRADUÇÃO DAS MÉTRICAS PARA A ÁREA DE NEGÓCIOS:
            s.val_geracaoverificada AS val_geracao,
            c.val_geracaolimitada,
            c.val_geracaoreferenciafinal AS val_geracaoreferencia,
            s.val_ventoverificado AS velocidade_do_vento,
            
            EXTRACT(YEAR FROM s.din_instante) AS ano,
            EXTRACT(MONTH FROM s.din_instante) AS mes
        FROM tb_spe s
        INNER JOIN tb_mestre m ON s.ceg = m.ceg 
        LEFT JOIN tb_complexo c 
            ON s.complexo_derivado_chave = c.complexo_derivado_chave 
            AND s.din_instante = c.din_instante
    """
    con.execute(query_fato)
    
    query_dim_projeto = """
        CREATE TABLE dim_projeto_spe AS
        SELECT DISTINCT m.ceg, m.projeto, m.spe, s.nom_usina, s.id_estado, s.nom_estado, s.id_subsistema
        FROM tb_mestre m
        JOIN tb_spe s ON s.ceg = m.ceg
    """
    con.execute(query_dim_projeto)
    
    # Persistência Otimizada via Parquet Particionado
    con.execute(f"COPY fato_restricao TO '{MODELED_DIR}/fato_restricao' (FORMAT PARQUET, PARTITION_BY (ano, mes, projeto), OVERWRITE_OR_IGNORE 1);")
    con.execute(f"COPY dim_projeto_spe TO '{MODELED_DIR}/dim_projeto_spe.parquet' (FORMAT PARQUET);")
    logging.info("Pipeline concluído com sucesso!")


# =============================================================================
# ORQUESTRADOR PRINCIPAL (EXECUÇÃO PASSO A PASSO)
# =============================================================================
def main():
    logging.info("=== INICIANDO PIPELINE DE DADOS ===")
    
    # 1. Extração
    df_spe, df_complexo, df_mestre = extrair_dados_brutos()
    
    # 2. Filtragem e Padronização
    df_spe, df_complexo = filtro_spe_cdv(df_spe, df_complexo, df_mestre)
    
    # 3. Limpeza
    df_spe, df_complexo = limpeza_e_tipagem_dados(df_spe, df_complexo)
    
    # 4. Validação
    df_spe, df_complexo = validar_matematica_dos_dados(df_spe, df_complexo)
    
    # 5. Carga e Modelagem
    executar_modelagem_duckdb(df_spe, df_complexo, df_mestre)

if __name__ == "__main__":
    main()