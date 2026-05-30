import duckdb

con = duckdb.connect("data/warehouse/cv_case.db")

print("🕵️‍♂️ INICIANDO INVESTIGAÇÃO DO ATIVO RAF11...\n")

# 1. Verifica na Camada Bruta (Raw)
raw_count = con.execute("SELECT COUNT(*) FROM raw_usinas_detail WHERE ceg LIKE '%50017%'").fetchone()[0]
print(f"1. Registros brutos encontrados no ONS (raw_usinas_detail): {raw_count}")

if raw_count > 0:
    exemplo_ceg = con.execute("SELECT DISTINCT ceg FROM raw_usinas_detail WHERE ceg LIKE '%50017%'").fetchone()[0]
    print(f"   -> Formato do CEG no ONS: '{exemplo_ceg}'")

# 2. Verifica após o cruzamento de extração (Intermediate)
int_count = con.execute("SELECT COUNT(*) FROM int_usinas_cv WHERE nome_spe_cv = 'RAF11'").fetchone()[0]
print(f"2. Registros que passaram pelo JOIN de sementes (int_usinas_cv): {int_count}")

# 3. Verifica na Fato Final da Gold
gold_count = con.execute("SELECT COUNT(*) FROM fato_geracao_restricao WHERE spe = 'RAF11'").fetchone()[0]
print(f"3. Registros finais na Fato Gold (fato_geracao_restricao): {gold_count}")

con.close()