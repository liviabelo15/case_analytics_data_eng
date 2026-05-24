import os
import requests
import logging 
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema
import duckdb
import re
import numpy as np
from pathlib import Path 
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_USINAS_DIR = Path("data/raw/raw_usinas")
RAW_USINAS_DETAIL_DIR = Path("data/raw/raw_usinas_detail")

DB_PATH = Path("data/warehouse/cv_case.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# === NOVO: CAMINHO PARA SALVAR O ARQUIVO PARQUET TRATADO ===
GOLD_OUTPUT_DIR = Path("data/modeled")
GOLD_PARQUET_PATH = GOLD_OUTPUT_DIR / "fato_geracao_cv_tratado.parquet"
# ==========================================================


def load_duckdb(db_path: Path, raw_usinas_path: Path, raw_detail_path: Path):
    logger.info("Iniciando a fase LOAD no DuckDB lendo arquivos Parquet...")
    
    # Conecta ou cria o banco de dados analítico local
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
            delim=',',          -- Altere para ',' se for o delimitador real do seu arquivo
            encoding='utf-8',   -- Protege a grafia correta dos nossos projetos
            header=True,
            auto_detect=True    -- Ele vai varrer e tipar os textos automaticamente como VARCHAR
            )
        """)

        logger.info("Fase LOAD concluída com sucesso. Dados brutos aterrissados e tipados.")

    except Exception as e:
        logger.error(f"Erro durante o Load: {e}")
        raise
    finally:
        con.close()

def spes_cvd_data_extract(db_path: Path):

    logger.info("Iniciando a filtragem das SPEs da Casa dos Ventos (Fase Transform)...")
    
    con = duckdb.connect(str(db_path))
    
    try:
        # Criamos uma nova tabela (int_usinas_cv) na camada intermediária
        # Utilizamos o INNER JOIN: automaticamente todas as usinas que não forem da Casa dos Ventos serão descartadas
        query = r"""
            CREATE OR REPLACE TABLE int_usinas_cv AS
            SELECT 
                rud.*,
                s.projeto AS projeto_cv,
                s.spe AS nome_spe_cv
            FROM raw_usinas_detail AS rud
            INNER JOIN seed_spes_cv AS s
                -- Usando a opção Sênior com Regex para encontrar o padrão 000000-0:
                ON regexp_extract(rud.ceg, '\d{6}-\d') = s.ceg
        """
        con.sql(query)
        
        # Validando o resultado da transformação
        linhas_cv = con.sql("SELECT COUNT(*) FROM int_usinas_cv").fetchone()
        logger.info(f"Cruzamento concluído! A nova tabela contém {linhas_cv} registros filtrados da Casa dos Ventos.")
        
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
            SELECT *
            FROM raw_usinas
            
            -- Filtramos para manter apenas os nomes que existam na nossa "Lista VIP"
            WHERE UPPER(nom_usina) IN (
                
                -- Esta subquery cria uma lista única com os nomes corretos dos nossos ativos
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
    logger.info("Iniciando a junção final (Fato) entre SPEs e Conjuntos...")
    
    con = duckdb.connect(str(db_path))
    
    try:
        query = r"""
            CREATE OR REPLACE TABLE fato_geracao_cv AS
            SELECT 
                -- 1. Dados da SPE (Nossa granularidade principal e garantida)
                -- Trazemos tudo (incluindo as colunas de projeto e spe que adicionamos antes)
                spe.* EXCLUDE (id_ons),
                spe.id_ons AS id_ons_spe,
                
                -- 2. Dados do Conjunto (Trazemos apenas os IDs e as métricas para não duplicar colunas descritivas)
                conj.id_ons AS id_ons_conjunto,
                conj.nom_subsistema,
                conj.val_geracao AS val_geracao_conjunto,
                conj.cod_razaorestricao,
                conj.cod_origemrestricao,
                conj.dsc_restricao,
                conj.val_geracaolimitada AS val_geracaolimitada_conjunto,
                conj.val_disponibilidade AS val_disponibilidade_conjunto,
                conj.val_geracaoreferencia AS val_geracaoreferencia_conjunto,
                conj.val_geracaoreferenciafinal AS val_geracaoreferenciafinal_conjunto
                
            FROM int_usinas_cv AS spe
            
            -- O LEFT JOIN garante que nunca perderemos uma medição da SPE,
            -- mesmo se o ONS não publicar o dado do conjunto para aquele horário
            LEFT JOIN int_cv_conj AS conj
                ON UPPER(
                    CASE 
                        WHEN spe.nom_modalidadeoperacao = 'Tipo II-C' THEN spe.nom_conjuntousina
                        ELSE spe.nom_usina
                    END
                ) = UPPER(conj.nom_usina)
                
                -- Alinhamento temporal obrigatório
                AND spe.din_instante = conj.din_instante
        """
        con.sql(query)
        
        # O  no final extrai apenas o número limpo da tupla retornada pelo fetchone()
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
        # Regra A: Não permitem nulos (nullable=False)
        "id_subsistema": Column(nullable=False),
        "nom_subsistema": Column(nullable=False),
        "id_estado": Column(nullable=False),
        "nom_estado": Column(nullable=False),
        "id_ons_spe": Column(nullable=False),
        "id_ons_conjunto": Column(nullable=False),
        "ceg": Column(nullable=False),
        "din_instante": Column(pa.DateTime, nullable=False),
        
        # Regra A e B combinadas (Não nulo e >= 0)
        "val_geracao_conjunto": Column(float, checks=Check.ge(0), nullable=False),
        
        # Regra B: Métricas >= 0 (Permitem nulos)
        "val_geracaolimitada_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_disponibilidade_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoreferencia_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoreferenciafinal_conjunto": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoestimada": Column(float, checks=Check.ge(0), nullable=True),
        "val_geracaoverificada": Column(float, checks=Check.ge(0), nullable=True),
        
        # Regra C: Filtro de vento inválido
        "flg_dadoventoinvalido": Column(float, checks=Check.isin([0]), nullable=True),
        
        # Regra D: Faixa plausível de vento
        "val_ventoverificado": Column(float, checks=Check.in_range(0, 40), nullable=True),
    },
    # Regra E: Geração limitada não excede geração de referência (Check no nível do DataFrame)
    checks=Check(
        lambda df: (df["val_geracaolimitada_conjunto"] <= df["val_geracaoreferencia_conjunto"]) | 
                   df["val_geracaolimitada_conjunto"].isna() | 
                   df["val_geracaoreferencia_conjunto"].isna(),
        name="check_limitada_menor_referencia"
    )
)

def extrair_e_tratar_pandera(db_path: Path) -> pd.DataFrame:
    logger.info("Iniciando extração e tratamento via Pandera...")
    
    with duckdb.connect(str(db_path)) as con:
        df = con.sql("SELECT * FROM fato_geracao_cv").df()

    print(f"\n{'='*50}\n INICIANDO TRATAMENTO E RELATÓRIO DE QUALIDADE \n{'='*50}")
    
    linhas_iniciais = len(df)

    # === ADICIONE ISSO AQUI BEM NO INÍCIO DA FUNÇÃO ===
    arquivo_log = Path("justificativa_exclusoes.txt")
    
    # 1. Limpeza básica para o Pandera não quebrar com tipos errados
    df = df.replace(r'^\s*$', np.nan, regex=True).replace(['null', 'NULL', 'NaN', 'nan', 'N/A', 'n/a', '-'], np.nan)
    df['din_instante'] = pd.to_datetime(df['din_instante'], errors='coerce')
    
    cols_num = df.filter(regex='^val_|^flg_').columns
    df[cols_num] = df[cols_num].apply(pd.to_numeric, errors='coerce')

    # Duplicatas
    duplicatas_iniciais = df.duplicated(subset=['nome_spe_cv', 'din_instante']).sum()
    df = df.drop_duplicates(subset=['nome_spe_cv', 'din_instante'])

    # 2. APLICAÇÃO DO CONTRATO (PANDERA)
    try:
        df_clean = schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
            print("\n" + "!"*50)
            print(" ALERTA: REGISTROS DESCARTADOS PELO PANDERA ")
            print("!"*50)
            
            # DataFrame interno do Pandera que diz qual índice falhou e por quê
            failure_df = err.failure_cases
            
            # Filtramos apenas falhas associadas a índices válidos do dataframe original
            indices_falhos = failure_df['index'].dropna().unique().astype(int)
            df_falhas_originais = df.loc[indices_falhos]
            
            # # Vamos construir o arquivo TXT linha por linha de forma limpa
            # with open(arquivo_log, "w", encoding="utf-8") as f:
            #     f.write("="*80 + "\n")
            #     f.write("          RELATÓRIO DE JUSTIFICATIVA DE EXCLUSÃO DE REGISTROS (PANDERA)          \n")
            #     f.write("="*80 + "\n\n")
            #     f.write(f"Data do Processamento: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            #     f.write(f"Total de Linhas com Inconsistências: {len(indices_falhos)}\n\n")
            #     f.write("-"*80 + "\n")
                
            #     # Agrupamos os erros por índice para documentar todos os motivos caso uma linha tenha mais de um erro
            #     for idx, grupo_erro in failure_df.groupby('index'):
            #         idx = int(idx)
            #         row_data = df.loc[idx]
                    
            #         # Coleta metadados da linha para fácil identificação no negócio
            #         spe_nome = row_data.get('nome_spe_cv', 'N/A')
            #         timestamp = row_data.get('din_instante', 'N/A')
            #         if isinstance(timestamp, pd.Timestamp):
            #             timestamp = timestamp.strftime('%d/%m/%Y %H:%M')
                    
            #         f.write(f"👉 LINHA DA BASE ORIGINAL (Índice: {idx}) | SPE: {spe_nome} | Instante: {timestamp}\n")
            #         f.write("   Motivos do Descarte:\n")
                    
            #         for _, erro in grupo_erro.iterrows():
            #             coluna = erro['column']
            #             check_regra = erro['check']
            #             valor_errado = erro['failure_case']
                        
            #             # Formata uma mensagem amigável dependendo se o erro foi de Nulo ou de Limite
            #             if pd.isna(valor_errado) or str(valor_errado).strip().lower() in ['nan', 'nat']:
            #                 f.write(f"     ❌ Coluna [{coluna}]: O dado veio VAZIO (Nulo), mas a regra exige preenchimento obrigatório.\n")
            #             else:
            #                 f.write(f"     ❌ Coluna [{coluna}]: Valor capturado '{valor_errado}' violou a regra [{check_regra}].\n")
                            
            #         f.write("-"*80 + "\n")
                    
            # print(f"ℹ️ Sucesso! Arquivo '{arquivo_log}' gerado com as justificativas detalhadas.")
            
            # Aplica o drop final das linhas que falharam usando os índices rastreados
            df_clean = df.drop(index=indices_falhos)

    # =====================================================================
    # NOVA ANÁLISE 1: COMPLETUDE (EXPECTED VS RECEIVED)
    # =====================================================================
    completude_relatorio = []
    THRESHOLD_COMPLETUDE = 95.0 # Mínimo de 95% de dados esperados
    
    if 'din_instante' in df_clean.columns and 'nome_spe_cv' in df_clean.columns:
        for spe, grupo in df_clean.groupby('nome_spe_cv'):
            min_data = grupo['din_instante'].min()
            max_data = grupo['din_instante'].max()
            
            if pd.notna(min_data) and pd.notna(max_data):
                # Cria a sequência teórica perfeita de 30 em 30 minutos para o período do projeto
                range_esperado = pd.date_range(start=min_data, end=max_data, freq='30min')
                total_esperado = len(range_esperado)
                total_recebido = grupo['din_instante'].nunique()
                
                # Evita divisão por zero
                percentual_completude = (total_recebido / total_esperado * 100) if total_esperado > 0 else 0
                
                status = "🟢 OK" if percentual_completude >= THRESHOLD_COMPLETUDE else "🔴 ALERTA CRÍTICO"
                completude_relatorio.append(
                    f" - {spe}: {percentual_completude:.2f}% de completude ({total_recebido}/{total_esperado}) {status}"
                )

    # =====================================================================
    # NOVA ANÁLISE 2: FRESHNESS CHECK (DADOS RECENTES)
    # =====================================================================
    freshness_status = "🔴 FALHA: Sem dados do último mês completo na base."
    
    if 'din_instante' in df_clean.columns and not df_clean.empty:
        max_timestamp = df_clean['din_instante'].max()
        data_atual = pd.Timestamp.now()
        
        # Calcula dinamicamente o ano e o mês anterior (último mês fechado esperado)
        if data_atual.month == 1:
            mes_esperado = 12
            ano_esperado = data_atual.year - 1
        else:
            mes_esperado = data_atual.month - 1
            ano_esperado = data_atual.year
            
        # Validação estrita: O último registro deve estar exatamente no ano e mês passados
        if max_timestamp.year == ano_esperado and max_timestamp.month == mes_esperado:
            freshness_status = f"🟢 SUCESSO: Dados validados com o último mês fechado! Último registro em: {max_timestamp.strftime('%d/%m/%Y %H:%M')}"
        elif (max_timestamp.year > ano_esperado) or (max_timestamp.year == ano_esperado and max_timestamp.month > mes_esperado):
            # Se os dados forem do mês atual ou superiores, avisamos que a base passou do ponto de corte estrito
            freshness_status = f"⚠️ ALERTA: A base contém dados do mês atual ({max_timestamp.strftime('%m/%Y')}), mas o critério de fechamento esperava apenas o mês anterior completo ({mes_esperado:02d}/{ano_esperado})."
        else:
            freshness_status = f"🔴 FALHA: Dados desatualizados ou incompletos. Último registro em: {max_timestamp.strftime('%d/%m/%Y %H:%M')} (Esperado safra fechada: {mes_esperado:02d}/{ano_esperado})"

    # =====================================================================
    # 3. ANÁLISE DE CONTINUIDADE (GAPS DE TEMPO)
    # =====================================================================
    gaps_relatorio = []
    if 'din_instante' in df_clean.columns and 'nome_spe_cv' in df_clean.columns:
        df_sorted = df_clean.sort_values(by=['nome_spe_cv', 'din_instante'])
        df_sorted['tempo_diff'] = df_sorted.groupby('nome_spe_cv')['din_instante'].diff()
        gaps = df_sorted[df_sorted['tempo_diff'] > pd.Timedelta(minutes=45)]
        
        if not gaps.empty:
            for spe, count in gaps.groupby('nome_spe_cv').size().items():
                gaps_relatorio.append(f" - {spe}: {count} descontinuidades reais encontradas.")

    # =====================================================================
    # NOVO: SALVANDO A BASE TRATADA EM PARQUET (CAMINHO GOLD)
    # =====================================================================
    try:
        # Garante que a pasta 'data/gold' existe
        GOLD_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Gravando arquivo Parquet tratado em: {GOLD_PARQUET_PATH}...")
        # index=False evita criar uma coluna sem nome inútil para o Parquet
        df_clean.to_parquet(GOLD_PARQUET_PATH, index=False, compression="snappy")
        logger.info("Arquivo Parquet salvo com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo Parquet: {e}")
        raise

    # =====================================================================
    # RELATÓRIO FINAL EXPANDIDO
    # =====================================================================
    linhas_finais = len(df_clean)
    registros_descartados = linhas_iniciais - linhas_finais
    perc = (registros_descartados / linhas_iniciais) * 100 if linhas_iniciais > 0 else 0
    
    print("\n--- RELATÓRIO DE QUALIDADE DE DADOS ---")
    print(f"Total de registros originais: {linhas_iniciais}")
    print(f"Total de registros após limpeza: {linhas_finais}")
    print(f"Total de registros descartados: {registros_descartados} ({perc:.2f}%)")
    print(f"Total de linhas duplicadas removidas: {duplicatas_iniciais}")
    
    print("\n--- FRESHNESS CHECK (ATUALIDADE DA BASE) ---")
    print(freshness_status)
    
    print(f"\n--- COMPLETUDE DA SÉRIE TEMPORAL (THRESHOLD: {THRESHOLD_COMPLETUDE}%) ---")
    if completude_relatorio:
        for relato in completude_relatorio: print(relato)
    else:
        print(" - Não foi possível calcular a completude (colunas ausentes).")
    
    print("\n--- CONTINUIDADE TEMPORAL (GAPS REAIS) ---")
    if gaps_relatorio:
        for relato in gaps_relatorio: print(relato)
    else:
        print(" - Perfeito! Todos os timestamps estão contínuos (sem gaps reais de +45min).")
            
    print("\n==========================================")
    logger.info("Tratamento via Pandera concluído!")
    return df_clean

if __name__ == "__main__":
    load_duckdb(DB_PATH, RAW_USINAS_DIR, RAW_USINAS_DETAIL_DIR)
    spes_cvd_data_extract(DB_PATH)
    filtrar_raw_usinas_conj(DB_PATH)
    relacionar_spe_conjunto(DB_PATH)
    #extrair_e_tratar_sql(DB_PATH)
    extrair_e_tratar_pandera(DB_PATH)

    # === NOVA ETAPA DE QUALIDADE ===
    # Executa a extração, gera o relatório e salva no DataFrame com o nome solicitado
    #final_usinas_analysis = extrair_e_tratar_qualidade(DB_PATH)
    
    # Se quiser visualizar uma amostra do resultado final
    #print(final_usinas_analysis.info())

