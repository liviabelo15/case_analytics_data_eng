# ==============================================================================
# NOME DO SCRIPT: check_data.py
# AUTOR:          Lívia Belo
# DATA DA VERSÃO: Maio de 2026
# VERSÃO:         1.0.0
# DESCRIPTION:    Script de Diagnóstico e Perfilamento de Dados (Data Profiling).
#                 Explora a integridade das tabelas armazenadas no DuckDB (camadas 
#                 Bronze e Interim), extraindo métricas de volumetria total, 
#                 estrutura e tipagem de colunas (Schema), amostras de registros 
#                 e auditoria de linhagem por arquivo de origem (filename).
# ==============================================================================

import duckdb
import pandas as pd

# Configura o Pandas para não esconder colunas na hora de printar no terminal
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# 1. Conecta ao banco de dados (Primeira etapa)
con = duckdb.connect('data/warehouse/cv_case.db')

# Lista exata das 3 tabelas que aterrissamos na fase Load
tabelas = ['seed_spes_cv', 'raw_usinas', 'raw_usinas_detail', 'int_usinas_cv', 'int_cv_conj']

for tabela in tabelas:
    print(f"\n{'='*60}")
    print(f"ANALISANDO A TABELA: {tabela.upper()}")
    print(f"{'='*60}")
    
    try:
        # 2. Análise de Volumetria (Total de registros)
        total_linhas = con.sql(f"SELECT COUNT(*) FROM {tabela}").fetchone()
        print(f"VOLUMETRIA: A tabela possui {total_linhas} linhas.\n")
        
        # 3. Análise de Estrutura e Tipagem (Schema)
        print("ESTRUTURA DAS COLUNAS (Tipos Inferidos):")
        estrutura_df = con.sql(f"DESCRIBE {tabela}").df()
        # Printamos apenas o nome da coluna e o tipo de dado para ficar limpo no terminal
        print(estrutura_df[['column_name', 'column_type']].to_string(index=False))
        print("\n")
        
        # 4. Amostra de Dados (Primeiras 5 linhas)
        print("AMOSTRA DE DADOS (5 primeiras linhas):")
        amostra_df = con.sql(f"SELECT * FROM {tabela} LIMIT 5").df()
        print(amostra_df)
        print("\n")

        print("📊 CONTAGEM DE LINHAS POR ARQUIVO DE ORIGEM:")
        auditoria_df = con.sql("""
            SELECT 
                filename, 
                COUNT(*) as total_linhas
            FROM raw_usinas_detail
            GROUP BY filename
            ORDER BY filename
""").df()

        print(auditoria_df)
        
    except duckdb.CatalogException:
        print(f"❌ ERRO: A tabela '{tabela}' não foi encontrada no banco de dados.")

con.close()