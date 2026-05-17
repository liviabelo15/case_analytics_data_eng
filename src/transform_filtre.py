import pandas as pd
import json
from pathlib import Path
import logging

# Configuração do nosso diário de bordo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validar_schema_com_json(df: pd.DataFrame, caminho_json: Path, nome_dataset: str):
    """
    Lê o dicionário JSON oficial do ONS e valida se o CSV contém todas as colunas exigidas.
    """
    try:
        with open(caminho_json, 'r', encoding='utf-8') as f:
            dicionario = json.load(f)
            
        # Extrai todos os "codigos" (nomes das colunas) da lista do dicionário
        colunas_esperadas = [item["codigo"] for item in dicionario["dicionario_simplificado"]]
        
        # Compara as colunas do Pandas com as colunas do Dicionário
        colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
        
        if colunas_faltantes:
            logging.error(f"[{nome_dataset}] ALERTA ESTRUTURAL: Faltam colunas no CSV: {colunas_faltantes}")
        else:
            logging.info(f"[{nome_dataset}] Validação Estrutural: Sucesso! Todas as {len(colunas_esperadas)} colunas oficiais estão presentes.")
            
    except FileNotFoundError:
        logging.warning(f"Arquivo JSON não encontrado em {caminho_json}. Pulando validação de schema.")


def consolidar_arquivos(pasta: Path, padrao_nome: str, colunas_de_data: list = None) -> pd.DataFrame:
    """
    Busca arquivos numa pasta por um padrão, lê tratando encoding/delimitadores e empilha num único DataFrame.
    """
    arquivos = list(pasta.glob(padrao_nome))
    if not arquivos:
        logging.warning(f"Nenhum arquivo encontrado com o padrão: {padrao_nome}")
        return pd.DataFrame() 

    lista_tabelas = []
    for caminho in arquivos:
        logging.info(f"Lendo: {caminho.name}")
        # Lendo o CSV com as configurações para o padrão brasileiro
        df = pd.read_csv(caminho, sep=';', encoding='utf-8', decimal=',', parse_dates=colunas_de_data)
        lista_tabelas.append(df)
        
    return pd.concat(lista_tabelas, ignore_index=True)


def aplicar_qualidade_dados(df: pd.DataFrame, nome_dataset: str, is_detalhamento: bool = False) -> pd.DataFrame:
    """
    Limpa os dados (nulos, duplicatas, tipagem) e aplica regras de negócio específicas.
    """
    logging.info(f"\n================ RELATÓRIO DE QUALIDADE: {nome_dataset} ================")
    linhas_antes = len(df)
    
    # 1. Consistência de Tipos: Forçar colunas numéricas
    colunas_numericas = [col for col in df.columns if col.startswith('val_')]
    for col in colunas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 2. Relatório de Nulos
    pct_nulos = (df.isnull().mean() * 100).round(2)
    colunas_com_nulos = pct_nulos[pct_nulos > 0]
    if not colunas_com_nulos.empty:
        logging.warning(f"Percentual de Nulos por coluna:\n{colunas_com_nulos.astype(str) + '%'}")

    # 3. Tratamento de Nulos em Colunas Chave
    colunas_chave = ['din_instante', 'id_ons'] 
    colunas_existentes = [c for c in colunas_chave if c in df.columns]
    df = df.dropna(subset=colunas_existentes)
    
    # 4. Tratamento de Duplicatas
    duplicatas = df.duplicated().sum()
    if duplicatas > 0:
        logging.warning(f"Encontradas {duplicatas} linhas duplicadas. Removendo...")
        df = df.drop_duplicates()

    # 5. Regra de Negócio: Filtro de Vento (Apenas Detalhamento)
    if is_detalhamento:
        coluna_flag = 'flg_dadoventoinvalido'
        if coluna_flag in df.columns:
            invalidos_antes = len(df)
            # Dicionário diz: flag = 1 é INVÁLIDO. Mantemos apenas os diferentes de 1.
            df = df[df[coluna_flag] != 1] 
            ventos_descartados = invalidos_antes - len(df)
            logging.info(f"Ventos inválidos descartados (flag=1): {ventos_descartados}")
        else:
            logging.warning(f"Coluna de flag '{coluna_flag}' não encontrada!")

    linhas_depois = len(df)
    logging.info(f"RESUMO: Linhas originais: {linhas_antes} | Descartadas: {linhas_antes - linhas_depois} | Finais: {linhas_depois}")
    logging.info("====================================================================\n")
    
    return df


def executar_transformacao():
    pasta_raw = Path("data/raw")
    pasta_processed = Path("data/processed")
    pasta_dicionarios = Path("data/dict")
    
    # Cria a pasta de saída se não existir
    pasta_processed.mkdir(parents=True, exist_ok=True)
    
    logging.info("--- Iniciando Consolidação ---")
    df_usinas = consolidar_arquivos(pasta_raw, "RESTRICAO_COFF_EOLICA_20*.csv", ['din_instante'])
    df_detalhes = consolidar_arquivos(pasta_raw, "RESTRICAO_COFF_EOLICA_DETAIL_20*.csv", ['din_instante'])
    
    # Validação de Schema com JSON
    # Certifique-se de que os nomes dos arquivos JSON batem com o que você salvou na pasta data/dict
    json_usinas = pasta_dicionarios / "DicionarioDados_RestricaoContrainedoff_UsiEolicas.json"
    json_detalhes = pasta_dicionarios / "DicionarioDados_RestricaoContrainedoff_UsiEolicas_DetalhamentoPorUsina.json"
    
    validar_schema_com_json(df_usinas, json_usinas, "DATASET USINAS")
    validar_schema_com_json(df_detalhes, json_detalhes, "DATASET DETALHAMENTO")
    
    # Qualidade e Limpeza
    df_usinas = aplicar_qualidade_dados(df_usinas, "DATASET USINAS", is_detalhamento=False)
    df_detalhes = aplicar_qualidade_dados(df_detalhes, "DATASET DETALHAMENTO", is_detalhamento=True)
    
    # Persistência no disco em formato Parquet
    caminho_usinas = pasta_processed / "usinas_consolidadas.parquet"
    caminho_detalhes = pasta_processed / "detalhes_consolidados.parquet"
    
    logging.info(f"Salvando base de Usinas em: {caminho_usinas}")
    df_usinas.to_parquet(caminho_usinas, index=False)
    
    logging.info(f"Salvando base de Detalhamento em: {caminho_detalhes}")
    df_detalhes.to_parquet(caminho_detalhes, index=False)
    
    logging.info("--- Transformação Concluída com Sucesso! ---")

if __name__ == "__main__":
    executar_transformacao()