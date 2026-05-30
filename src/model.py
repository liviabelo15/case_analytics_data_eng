# ==============================================================================
# NOME DO SCRIPT: model.py
# AUTOR:          Lívia Belo
# DATA DA VERSÃO: Maio de 2026
# VERSÃO:         1.0.3
# DESCRIPTION:    Script exclusivo de Modelagem Dimensional (Camada Gold).
#                 Consome a base mestre de sementes para garantir a integridade
#                 de todos os 47 ativos na dimensão, vincula as métricas da fato
#                 e exibe um relatório estruturado dos dados no terminal.
# ==============================================================================

import os
import logging
from pathlib import Path
import duckdb

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de Caminhos
DB_PATH = Path("data/warehouse/cv_case.db")
INPUT_PARQUET_PATH = Path("data/modeled/fato_geracao_cv_tratado.parquet")
CSV_PATH = Path("spes_casa_dos_ventos.csv")
OUTPUT_DIR = Path("data/modeled")

# Caminhos de saída da Camada Gold
GOLD_FATO_PATH = OUTPUT_DIR / "fato_geracao_restricao.parquet"
GOLD_DIM_ATIVO_PATH = OUTPUT_DIR / "dim_ativo_eolico.parquet"
GOLD_DIM_TEMPO_PATH = OUTPUT_DIR / "dim_tempo.parquet"

def main():
    print(f"\n{'='*60}\n 📐 INICIANDO MODELAGEM DIMENSIONAL (STAR SCHEMA SUPREMO) \n{'='*60}")
    
    # 1. Validação de Pré-requisitos
    if not INPUT_PARQUET_PATH.exists():
        logger.error(f"❌ Arquivo base não encontrado em: {INPUT_PARQUET_PATH}")
        return
    if not CSV_PATH.exists():
        logger.error(f"❌ Arquivo mestre de SPEs não encontrado em: {CSV_PATH}")
        return

    # Garante que a pasta de destino dos Parquets Gold existe
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Conecta ao banco de dados DuckDB
    logger.info(f"Conectando ao Data Warehouse: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    
    try:
        # Registra o Parquet limpo como uma tabela temporária no DuckDB
        logger.info("Carregando base limpa e auditada pelo Pandera...")
        con.execute(f"CREATE OR REPLACE TEMPORARY TABLE src_clean_data AS SELECT * FROM read_parquet('{INPUT_PARQUET_PATH}')")

        # =====================================================================
        # 2. CRIAÇÃO DA DIMENSÃO: dim_ativo_eolico (Garantindo todos os 47 ativos)
        # =====================================================================
        logger.info("Construindo tabela dimensional: dim_ativo_eolico baseada no CSV mestre...")
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_ativo_eolico AS
            SELECT DISTINCT
                s.spe,
                s.projeto as projeto_cv,
                s.ceg,
                d.nom_usina,
                d.nom_modalidadeoperacao,
                d.nom_conjuntousina,
                d.id_ons_spe,
                d.id_ons_conjunto,
                d.id_subsistema,
                d.nom_subsistema,
                d.id_estado
            FROM read_csv_auto('{str(CSV_PATH)}') s
            LEFT JOIN (
                SELECT DISTINCT 
                    nome_spe_cv, nom_usina, nom_modalidadeoperacao, nom_conjuntousina, 
                    id_ons_spe, id_ons_conjunto, id_subsistema, nom_subsistema, id_estado
                FROM src_clean_data
            ) d ON UPPER(s.spe) = UPPER(d.nome_spe_cv)
        """)
        con.execute("ALTER TABLE dim_ativo_eolico ADD PRIMARY KEY (spe);")
        
        # =====================================================================
        # 3. CRIAÇÃO DA DIMENSÃO: dim_tempo (Calendário Analítico)
        # =====================================================================
        logger.info("Construindo tabela dimensional: dim_tempo...")
        con.execute("""
            CREATE OR REPLACE TABLE dim_tempo AS
            SELECT DISTINCT
                din_instante,
                EXTRACT(year FROM din_instante) as ano,
                EXTRACT(month FROM din_instante) as mes,
                EXTRACT(day FROM din_instante) as dia,
                EXTRACT(hour FROM din_instante) as hora,
                EXTRACT(minute FROM din_instante) as minuto,
                CASE WHEN EXTRACT(dow FROM din_instante) IN (0, 6) THEN 1 ELSE 0 END as flg_fim_de_semana
            FROM src_clean_data
            WHERE din_instante IS NOT NULL
        """)
        con.execute("ALTER TABLE dim_tempo ADD PRIMARY KEY (din_instante);")

        # =====================================================================
        # 4. CRIAÇÃO DA TABELA FATO: fato_geracao_restricao
        # =====================================================================
        logger.info("Construindo tabela Fato central: fato_geracao_restricao...")
        con.execute("""
            CREATE OR REPLACE TABLE fato_geracao_restricao AS
            SELECT
                nome_spe_cv as spe, 
                din_instante,       
                val_ventoverificado,
                flg_dadoventoinvalido,
                val_geracaoestimada,
                val_geracaoverificada,
                val_geracao_conjunto,
                val_geracaolimitada_conjunto,
                val_disponibilidade_conjunto,
                val_geracaoreferencia_conjunto,
                val_geracaoreferenciafinal_conjunto,
                flg_alerta_limitada,
                cod_razaorestricao,
                cod_origemrestricao,
                dsc_restricao
            FROM src_clean_data
        """)
        con.execute("ALTER TABLE fato_geracao_restricao ADD PRIMARY KEY (spe, din_instante);")

        # =====================================================================
        # 5. EXPORTAÇÃO DOS ARQUIVOS PARQUET ANALÍTICOS (GOLD LAYER)
        # =====================================================================
        logger.info("Exportando tabelas do Star Schema para arquivos Parquet na camada Gold...")
        con.execute(f"COPY dim_ativo_eolico TO '{GOLD_DIM_ATIVO_PATH}' (FORMAT PARQUET)")
        con.execute(f"COPY dim_tempo TO '{GOLD_DIM_TEMPO_PATH}' (FORMAT PARQUET)")
        con.execute(f"COPY fato_geracao_restricao TO '{GOLD_FATO_PATH}' (FORMAT PARQUET)")

        # =====================================================================
        # 6. AUDITORIA E INSPEÇÃO DE DADOS NO TERMINAL
        # =====================================================================
        print("\n🏆 MODELAGEM DIMENSIONAL CONCLUÍDA COM SUCESSO!")
        print(f"📍 Tabelas físicas criadas no banco: {DB_PATH}")
        print(f"📍 Arquivos Parquet analíticos salvos em: {OUTPUT_DIR}/")
        
        print("\n📊 Balanço de Massa Analítico (Contagem de Linhas):")
        linhas_ativo = con.sql("SELECT COUNT(*) FROM dim_ativo_eolico").fetchone()[0]
        linhas_tempo = con.sql("SELECT COUNT(*) FROM dim_tempo").fetchone()[0]
        linhas_fato = con.sql("SELECT COUNT(*) FROM fato_geracao_restricao").fetchone()[0]
        
        print(f" -> 📐 dim_ativo_eolico:     {linhas_ativo} ativos cadastrados. (Mestre preservado!)")
        print(f" -> 📅 dim_tempo:            {linhas_tempo} intervalos temporais mapeados.")
        print(f" -> 🧮 fato_geracao_restricao: {linhas_fato} registros de medição vinculados.")
        
        print(f"\n{'-'*30} INSPEÇÃO: dim_ativo_eolico {'-'*30}")
        print("\n📋 Dicionário de Colunas e Tipos Armazenados:")
        con.sql("DESCRIBE dim_ativo_eolico").show()
        
        print("\n👀 Amostra das primeiras linhas na dim_ativo_eolico:")
        # Seleciona as colunas principais para não quebrar o layout do terminal largo
        con.sql("""
            SELECT spe, projeto_cv, ceg, nom_usina, nom_conjuntousina, id_estado 
            FROM dim_ativo_eolico 
            LIMIT 48
        """).show()
        print("="*80)

    except Exception as e:
        logger.error(f"Erro crítico durante a execução da modelagem: {e}")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()