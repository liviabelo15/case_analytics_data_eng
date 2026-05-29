# ==============================================================================
# NOME DO SCRIPT: check_data_spe.py
# AUTOR:          Lívia Belo
# DATA DA VERSÃO: Maio de 2026
# VERSÃO:         1.0.0
# DESCRIPTION:    Script de Validação e Auditoria da camada Intermediária (Silver).
#                 Inspeciona o sucesso do cruzamento de dados avaliando a 
#                 volumetria volumétrica detalhada por SPE (Usinas da CDV) e 
#                 extraindo amostras de colunas estratégicas das tabelas 
#                 'int_usinas_cv' e 'int_cv_conj' para validação visual.
# ==============================================================================

import duckdb
import pandas as pd

# Configura o Pandas para mostrar todas as colunas e linhas sem "cortar" a tela
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100) 
pd.set_option('display.width', 2000)

con = duckdb.connect('data/warehouse/cv_case.db')

try:
    # =====================================================================
    # 1. AUDITORIA DE VOLUMETRIA (CONTAGENS)
    # =====================================================================
    print("\n📊 CONTAGEM DE LINHAS POR SPE (Tabela: int_usinas_cv):")
    contagem_spe_df = con.sql("""
        SELECT 
            nome_spe_cv, 
            COUNT(*) as total_linhas
        FROM int_usinas_cv
        GROUP BY nome_spe_cv
        ORDER BY total_linhas DESC
    """).df()
    print(contagem_spe_df)

    print("\n📊 CONTAGEM DE LINHAS POR USINA/CONJUNTO (Tabela: int_cv_conj):")
    contagem_conj_df = con.sql("""
        SELECT 
            nom_usina, 
            COUNT(*) as total_linhas
        FROM int_cv_conj
        GROUP BY nom_usina
        ORDER BY total_linhas DESC
    """).df()
    print(contagem_conj_df)

    # =====================================================================
    # 2. AMOSTRAS DE DADOS
    # =====================================================================
    print("\n👀 AMOSTRA DE DADOS - 100 primeiras linhas da tabela [int_usinas_cv]:")
    amostra_df = con.sql("""
        SELECT 
            id_subsistema, din_instante, id_estado, nom_modalidadeoperacao, 
            nom_conjuntousina, nom_usina, id_ons, projeto_cv, nome_spe_cv 
        FROM int_usinas_cv 
        LIMIT 100
    """).df()
    print(amostra_df)

    print("\n👀 AMOSTRA DE DADOS - 100 primeiras linhas da tabela [int_cv_conj]:")
    amostra2_df = con.sql("""
        SELECT 
            id_subsistema, nom_subsistema, id_estado, nom_usina, 
            ceg, din_instante 
        FROM int_cv_conj 
        LIMIT 100
    """).df()
    print(amostra2_df)

except Exception as e:
    print(f"❌ Erro durante a consulta: {e}")
finally:
    con.close()