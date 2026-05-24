import duckdb
import pandas as pd

# Configura o Pandas para mostrar todas as colunas e linhas sem "cortar" a tela
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100) # Permite ver a lista de usinas inteira
pd.set_option('display.width', 2000)

con = duckdb.connect('data/warehouse/cv_case.db')

try:
    print("\n📊 CONTAGEM DE LINHAS POR SPE (Usinas):")
    # Agrupamos pelo nome da usina e ordenamos da que tem mais linhas para a que tem menos
    contagem_spe_df = con.sql("""
        SELECT 
            nome_spe_cv, 
            COUNT(*) as total_linhas
        FROM int_usinas_cv
        GROUP BY nome_spe_cv
        ORDER BY total_linhas DESC
    """).df()
    print(contagem_spe_df)

    print("\n👀 AMOSTRA DE DADOS (Primeiras 5 linhas da tabela):")
    # O LIMIT 5 garante que o banco só vai ler e trazer as 5 primeiras ocorrências
    amostra_df = con.sql("SELECT id_subsistema, din_instante, id_estado, nom_modalidadeoperacao, nom_conjuntousina, nom_usina, id_ons, projeto_cv, nome_spe_cv FROM int_usinas_cv LIMIT 100").df()
    print(amostra_df)

    print("\n👀 AMOSTRA DE DADOS (Primeiras 5 linhas da tabela):")

    amostra2_df = con.sql("SELECT id_subsistema, nom_subsistema, id_estado, nom_usina, ceg, din_instante FROM int_cv_conj LIMIT 100").df()
    print(amostra2_df)

except Exception as e:
    print(f"Erro durante a consulta: {e}")
finally:
    con.close()