# ==============================================================================
# NOME DO SCRIPT: model.py
# AUTOR:          Lívia Belo
# DATA DA VERSÃO: Junho de 2026
# VERSÃO:         2.0.0
# DESCRIPTION:    Modelagem Dimensional (Camada Gold) — Star Schema com dois fatos
#                 separados por granularidade: SPE (aerogerador) e Conjunto (complexo).
#                 Elimina fan trap da v1 onde métricas de conjunto eram repetidas por SPE.
# ==============================================================================

import logging
from pathlib import Path
import duckdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = Path("data/warehouse/cv_case.db")
SILVER_PATH = Path("data/modeled/fato_geracao_cv_tratado.parquet")
CSV_PATH = Path("spes_casa_dos_ventos.csv")
OUTPUT_DIR = Path("data/modeled")

# Caminhos Gold layer
GOLD_DIM_SPE_PATH       = OUTPUT_DIR / "dim_spe.parquet"
GOLD_DIM_CONJUNTO_PATH  = OUTPUT_DIR / "dim_conjunto.parquet"
GOLD_DIM_RESTRICAO_PATH = OUTPUT_DIR / "dim_restricao.parquet"
GOLD_DIM_TEMPO_PATH     = OUTPUT_DIR / "dim_tempo.parquet"
GOLD_FATO_SPE_PATH      = OUTPUT_DIR / "fato_geracao_spe.parquet"
GOLD_FATO_CONJUNTO_PATH = OUTPUT_DIR / "fato_geracao_conjunto.parquet"

# Seed estática: 4 razões × 2 origens = 8 combinações definidas pelo dicionário ONS.
# Hardcoded para garantir que a dimensão exista mesmo que um código não apareça nos dados.
RESTRICAO_SEED = [
    ("REL", "LOC", "Indisponibilidade externa elétrica local"),
    ("REL", "SIS", "Indisponibilidade externa elétrica sistêmica"),
    ("CNF", "LOC", "Atendimento a requisitos de confiabilidade local"),
    ("CNF", "SIS", "Atendimento a requisitos de confiabilidade sistêmica"),
    ("ENE", "LOC", "Razão energética local"),
    ("ENE", "SIS", "Razão energética sistêmica"),
    ("PAR", "LOC", "Restrição indicada no parecer de acesso local"),
    ("PAR", "SIS", "Restrição indicada no parecer de acesso sistêmica"),
]


def main():
    print(f"\n{'='*65}")
    print(" MODELAGEM DIMENSIONAL — Star Schema com Dois Fatos (v2)")
    print(f"{'='*65}")

    if not SILVER_PATH.exists():
        logger.error(f"Silver layer não encontrado: {SILVER_PATH}")
        return
    if not CSV_PATH.exists():
        logger.error(f"CSV de SPEs não encontrado: {CSV_PATH}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    Path("data/warehouse").mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))

    try:
        logger.info("Carregando Silver layer em tabela temporária...")
        con.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE silver AS
            SELECT * FROM read_parquet('{SILVER_PATH}')
        """)

        # ==================================================================
        # dim_conjunto — granularidade: 1 complexo/conjunto eólico
        # Fonte: Silver, colunas de conjunto (Dataset 1 via join)
        # PK: id_ons_conjunto
        #
        # A construção do nom_conjunto usa a mesma expressão CASE do join em
        # load_transform.py, que é o único lado que conhece o nome do conjunto
        # (Dataset 1 não é persistido diretamente no Silver).
        # ==================================================================
        logger.info("Construindo dim_conjunto...")
        con.execute("""
            CREATE OR REPLACE TABLE dim_conjunto AS
            SELECT
                id_ons_conjunto,
                ANY_VALUE(
                    CASE
                        WHEN nom_modalidadeoperacao = 'Tipo II-C' THEN nom_conjuntousina
                        ELSE nom_usina
                    END
                ) AS nom_conjunto,
                ANY_VALUE(id_subsistema)  AS id_subsistema,
                ANY_VALUE(nom_subsistema) AS nom_subsistema,
                ANY_VALUE(id_estado)      AS id_estado
            FROM silver
            WHERE id_ons_conjunto IS NOT NULL
            GROUP BY id_ons_conjunto
        """)
        con.execute("ALTER TABLE dim_conjunto ADD PRIMARY KEY (id_ons_conjunto);")

        # ==================================================================
        # dim_spe — granularidade: 1 SPE (aerogerador individual)
        # Fonte: CSV mestre (todos os 47 ativos) LEFT JOIN Silver
        # PK: spe
        # FK: id_ons_conjunto → dim_conjunto
        # SCD Tipo 1 (sobrescreve) — período de 6 meses sem reclassificações ONS
        # ==================================================================
        logger.info("Construindo dim_spe...")
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_spe AS
            SELECT
                s.spe,
                s.projeto              AS projeto_cv,
                s.ceg,
                d.nom_usina,
                d.nom_modalidadeoperacao,
                d.id_ons_spe,
                d.id_ons_conjunto
            FROM read_csv_auto('{str(CSV_PATH)}') s
            LEFT JOIN (
                SELECT DISTINCT
                    nome_spe_cv,
                    nom_usina,
                    nom_modalidadeoperacao,
                    id_ons_spe,
                    id_ons_conjunto
                FROM silver
            ) d ON UPPER(s.spe) = UPPER(d.nome_spe_cv)
        """)
        con.execute("ALTER TABLE dim_spe ADD PRIMARY KEY (spe);")

        spes_sem_conj = con.sql(
            "SELECT COUNT(*) FROM dim_spe WHERE id_ons_conjunto IS NULL"
        ).fetchone()[0]
        if spes_sem_conj > 0:
            logger.warning(
                f"{spes_sem_conj} SPE(s) sem id_ons_conjunto — "
                "join por nom_usina sem correspondência no Silver."
            )

        # ==================================================================
        # dim_restricao — seed estática ONS (não derivada dos dados)
        # Chave natural composta: (cod_razaorestricao, cod_origemrestricao)
        # Garante cobertura dos 8 tipos mesmo que um deles não ocorra no período
        # ==================================================================
        logger.info("Construindo dim_restricao (seed estática ONS)...")
        seed_values = ", ".join(
            f"('{r}', '{o}', '{d}')" for r, o, d in RESTRICAO_SEED
        )
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_restricao AS
            SELECT
                cod_razaorestricao,
                cod_origemrestricao,
                dsc_restricao_completa
            FROM (VALUES {seed_values})
                t(cod_razaorestricao, cod_origemrestricao, dsc_restricao_completa)
        """)

        # ==================================================================
        # dim_tempo — granularidade: 1 instante de 30 min
        # Fonte: todos os din_instante distintos do Silver
        # PK: din_instante
        # ==================================================================
        logger.info("Construindo dim_tempo...")
        con.execute("""
            CREATE OR REPLACE TABLE dim_tempo AS
            SELECT DISTINCT
                din_instante,
                EXTRACT(year   FROM din_instante) AS ano,
                EXTRACT(month  FROM din_instante) AS mes,
                EXTRACT(day    FROM din_instante) AS dia,
                EXTRACT(hour   FROM din_instante) AS hora,
                EXTRACT(minute FROM din_instante) AS minuto,
                CASE WHEN EXTRACT(dow FROM din_instante) IN (0, 6) THEN 1 ELSE 0 END
                    AS flg_fim_semana
            FROM silver
            WHERE din_instante IS NOT NULL
        """)
        con.execute("ALTER TABLE dim_tempo ADD PRIMARY KEY (din_instante);")

        # ==================================================================
        # fato_geracao_spe — grão: 1 SPE × 1 intervalo de 30 min
        # Apenas métricas físicas do aerogerador (vento, geração estimada/verificada)
        # FK: spe → dim_spe | din_instante → dim_tempo
        #
        # DECISÃO DE DESIGN: métricas de conjunto (val_geracao_conjunto, etc.)
        # NÃO entram aqui. Se entrassem, SUM(val_geracao_conjunto) contaria
        # N vezes por complexo (fan trap). Elas ficam em fato_geracao_conjunto.
        # ==================================================================
        logger.info("Construindo fato_geracao_spe...")
        con.execute("""
            CREATE OR REPLACE TABLE fato_geracao_spe AS
            SELECT
                nome_spe_cv            AS spe,
                din_instante,
                val_ventoverificado,
                flg_dadoventoinvalido,
                val_geracaoestimada,
                val_geracaoverificada,
                flg_estimada_por_historico
            FROM silver
        """)
        con.execute(
            "ALTER TABLE fato_geracao_spe ADD PRIMARY KEY (spe, din_instante);"
        )

        # ==================================================================
        # fato_geracao_conjunto — grão: 1 conjunto × 1 intervalo de 30 min
        # Métricas elétricas do complexo: geração, limitação, disponibilidade
        # FK: id_ons_conjunto → dim_conjunto | din_instante → dim_tempo
        # FK natural: (cod_razaorestricao, cod_origemrestricao) → dim_restricao
        #
        # O Silver tem granularidade SPE×timestamp; deduplicamos para
        # conjunto×timestamp com GROUP BY + MAX nas métricas numéricas.
        # Todas as SPEs do mesmo conjunto carregam os mesmos valores de conjunto
        # no mesmo instante (fonte única: Dataset 1 via join).
        # ==================================================================
        logger.info("Construindo fato_geracao_conjunto...")
        con.execute("""
            CREATE OR REPLACE TABLE fato_geracao_conjunto AS
            SELECT
                id_ons_conjunto,
                din_instante,
                MAX(val_geracao_conjunto)                AS val_geracao,
                MAX(val_geracaolimitada_conjunto)        AS val_geracaolimitada,
                MAX(val_disponibilidade_conjunto)        AS val_disponibilidade,
                MAX(val_geracaoreferencia_conjunto)      AS val_geracaoreferencia,
                MAX(val_geracaoreferenciafinal_conjunto) AS val_geracaoreferenciafinal,
                ANY_VALUE(cod_razaorestricao)            AS cod_razaorestricao,
                ANY_VALUE(cod_origemrestricao)           AS cod_origemrestricao,
                ANY_VALUE(dsc_restricao)                 AS dsc_restricao,
                MAX(flg_alerta_limitada)                 AS flg_alerta_limitada
            FROM silver
            WHERE id_ons_conjunto IS NOT NULL
            GROUP BY id_ons_conjunto, din_instante
        """)
        con.execute(
            "ALTER TABLE fato_geracao_conjunto ADD PRIMARY KEY (id_ons_conjunto, din_instante);"
        )

        # ==================================================================
        # EXPORTAÇÃO — Gold Layer Parquet
        # ==================================================================
        logger.info("Exportando Gold layer para Parquet...")
        exports = {
            "dim_spe":               GOLD_DIM_SPE_PATH,
            "dim_conjunto":          GOLD_DIM_CONJUNTO_PATH,
            "dim_restricao":         GOLD_DIM_RESTRICAO_PATH,
            "dim_tempo":             GOLD_DIM_TEMPO_PATH,
            "fato_geracao_spe":      GOLD_FATO_SPE_PATH,
            "fato_geracao_conjunto": GOLD_FATO_CONJUNTO_PATH,
        }
        for table, path in exports.items():
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")

        # ==================================================================
        # AUDITORIA
        # ==================================================================
        print("\nMODELAGEM CONCLUIDA")
        print(f"  Banco:    {DB_PATH}")
        print(f"  Parquets: {OUTPUT_DIR}/\n")

        print(f"{'Tabela':<30} {'Linhas':>10}")
        print("-" * 42)
        for table in exports:
            n = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:<28} {n:>10,}")

        # Integridade: SPEs com dados vs total cadastrado
        spes_com_fato = con.sql(
            "SELECT COUNT(DISTINCT spe) FROM fato_geracao_spe"
        ).fetchone()[0]
        spes_total = con.sql(
            "SELECT COUNT(*) FROM dim_spe"
        ).fetchone()[0]
        if spes_com_fato < spes_total:
            logger.warning(
                f"{spes_total - spes_com_fato} SPE(s) em dim_spe sem dados "
                "na fato_geracao_spe (zero registros no Silver)."
            )

        print(f"\n  Integridade SPE: {spes_com_fato}/{spes_total} com dados na fato")

        print("\ndim_restricao (seed completa):")
        con.sql(
            "SELECT * FROM dim_restricao ORDER BY cod_razaorestricao, cod_origemrestricao"
        ).show()

        print("\nAmostra dim_spe:")
        con.sql("""
            SELECT spe, projeto_cv, ceg, id_ons_conjunto
            FROM dim_spe ORDER BY projeto_cv, spe LIMIT 8
        """).show()

        print("\nAmostra dim_conjunto:")
        con.sql("""
            SELECT id_ons_conjunto, nom_conjunto, nom_subsistema, id_estado
            FROM dim_conjunto LIMIT 8
        """).show()

        print("=" * 65)

    except Exception as e:
        logger.error(f"Erro durante a modelagem: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
