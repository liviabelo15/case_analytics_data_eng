import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consolidar_arquivos(pasta: Path, padrao_nome: str, colunas_de_data: list = None) -> pd.DataFrame:
    # ... (Mantenha a sua função consolidar_arquivos exatamente como estava) ...
    arquivos = list(pasta.glob(padrao_nome))
    if not arquivos:
        logging.warning(f"Nenhum arquivo encontrado com o padrão: {padrao_nome}")
        return pd.DataFrame() 

    lista_tabelas = []
    for caminho in arquivos:
        logging.info(f"Lendo: {caminho.name}")
        df = pd.read_csv(caminho, sep=';', encoding='utf-8', decimal=',', parse_dates=colunas_de_data)
        lista_tabelas.append(df)
        
    return pd.concat(lista_tabelas, ignore_index=True)


def aplicar_qualidade_dados(df: pd.DataFrame, nome_dataset: str, is_detalhamento: bool = False) -> pd.DataFrame:
    """
    Função que limpa os dados e gera o relatório de qualidade no terminal.
    """
    logging.info(f"\n================ RELATÓRIO DE QUALIDADE: {nome_dataset} ================")
    linhas_antes = len(df)
    
    # 1. Consistência de Tipos: Forçar colunas de valores (que começam com 'val_') a serem numéricas
    # O errors='coerce' transforma qualquer texto perdido no meio dos números em NaN (Nulo)
    colunas_numericas = [col for col in df.columns if col.startswith('val_')]
    for col in colunas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 2. Relatório de Nulos (Percentual por coluna)
    # df.isnull().mean() calcula a média de nulos (que é a porcentagem se multiplicar por 100)
    pct_nulos = (df.isnull().mean() * 100).round(2)
    colunas_com_nulos = pct_nulos[pct_nulos > 0]
    
    if not colunas_com_nulos.empty:
        logging.warning(f"Percentual de Nulos por coluna:\n{colunas_com_nulos.astype(str) + '%'}")
    else:
        logging.info("Nenhum valor nulo encontrado nas colunas!")

    # TRATAMENTO DE NULOS: Removemos as linhas onde a data ou o identificador principal são nulos
    # (Não podemos fazer análises de tempo se não sabemos a data)
    colunas_chave = ['din_instante', 'id_ons'] 
    colunas_existentes = [c for c in colunas_chave if c in df.columns]
    df = df.dropna(subset=colunas_existentes)
    
    # 3. Identificar e Tratar Duplicatas
    duplicatas = df.duplicated().sum()
    if duplicatas > 0:
        logging.warning(f"Encontradas {duplicatas} linhas duplicadas. Removendo...")
        df = df.drop_duplicates()

    # 4. Filtrar dados de vento inválidos (Apenas para o dataset de detalhamento)
    if is_detalhamento:
        # ATENÇÃO: Confirme no seu CSV se o nome da coluna é exatamente este.
        # Geralmente 1 significa Válido (Verificado) e 0 significa Inválido.
        coluna_flag = 'val_velocidadeventoverificadoflag'
        if coluna_flag in df.columns:
            invalidos_antes = len(df)
            df = df[df[coluna_flag] == 1] # Mantém apenas onde a flag é 1 (válido)
            ventos_descartados = invalidos_antes - len(df)
            logging.info(f"Ventos inválidos descartados pela flag: {ventos_descartados}")
        else:
            logging.warning(f"Coluna de flag '{coluna_flag}' não encontrada!")

    linhas_depois = len(df)
    logging.info(f"RESUMO: Linhas originais: {linhas_antes} | Linhas descartadas: {linhas_antes - linhas_depois} | Linhas finais: {linhas_depois}")
    logging.info("====================================================================\n")
    
    return df


def executar_transformacao():
    pasta_raw = Path("data/raw")
    pasta_processed = Path("data/processed")
    pasta_processed.mkdir(parents=True, exist_ok=True)
    
    logging.info("--- Iniciando Consolidação ---")
    df_usinas = consolidar_arquivos(pasta_raw, "RESTRICAO_COFF_EOLICA_20*.csv", ['din_instante'])
    df_detalhes = consolidar_arquivos(pasta_raw, "RESTRICAO_COFF_EOLICA_DETAIL_20*.csv", ['din_instante'])
    
    # Aplicando a Qualidade e Limpeza ANTES de salvar
    df_usinas = aplicar_qualidade_dados(df_usinas, "DATASET USINAS (TM)", is_detalhamento=False)
    df_detalhes = aplicar_qualidade_dados(df_detalhes, "DATASET DETALHAMENTO", is_detalhamento=True)
    
    # Salvando no disco em formato Parquet
    caminho_usinas = pasta_processed / "usinas_consolidadas.parquet"
    caminho_detalhes = pasta_processed / "detalhes_consolidados.parquet"
    
    df_usinas.to_parquet(caminho_usinas, index=False)
    df_detalhes.to_parquet(caminho_detalhes, index=False)
    
    logging.info("--- Dados limpos e persistidos com sucesso! ---")

if __name__ == "__main__":
    executar_transformacao()