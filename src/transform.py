import pandas as pd
from pathlib import Path
import logging

# Configuração do nosso diário de bordo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consolidar_arquivos(pasta: Path, padrao_nome: str, colunas_de_data: list = None) -> pd.DataFrame:
    """
    Busca arquivos numa pasta por um padrão, lê tratando encoding/delimitadores
    e empilha num único DataFrame.
    """
    # 1. O "cão farejador" acha todos os arquivos com o padrão desejado
    arquivos = list(pasta.glob(padrao_nome))
    
    if not arquivos:
        logging.warning(f"Nenhum arquivo encontrado com o padrão: {padrao_nome}")
        return pd.DataFrame() 

    lista_tabelas = []
    
    # 2. O Loop de Leitura e Tratamento (Encoding, Delimitador e Datas)
    for caminho in arquivos:
        logging.info(f"Lendo e processando: {caminho.name}")
        
        df = pd.read_csv(
            caminho, 
            sep=';',                   # Delimitador padrão do governo
            encoding='utf-8',          # Encoding para evitar que acentos quebrem (pode ser latin1)
            decimal=',',               # Avisa que a vírgula é o separador decimal no Brasil
            parse_dates=colunas_de_data # O Pandas converte essas colunas para Datetime automaticamente!
        )
        
        lista_tabelas.append(df)
        
    # 3. Concatenação (Empilhamento)
    logging.info(f"Empilhando {len(lista_tabelas)} arquivos...")
    tabela_consolidada = pd.concat(lista_tabelas, ignore_index=True)
    
    return tabela_consolidada

def executar_transformacao():
    pasta_raw = Path("data/raw")
    
    logging.info("--- Iniciando Consolidação de Dados ---")
    
    # ATENÇÃO: As colunas de data variam conforme o dataset. 
    # 'din_instante' costuma ser a coluna de data/hora nos dados do ONS.
    # Se o nome da coluna no CSV for diferente, basta alterar esta lista.
    datas_usinas = ['din_instante'] 
    
    logging.info("Consolidando dataset de Usinas...")
    df_usinas = consolidar_arquivos(
        pasta=pasta_raw, 
        padrao_nome="RESTRICAO_COFF_EOLICA_20*.csv", 
        colunas_de_data=datas_usinas
    )
    
    logging.info("Consolidando dataset de Detalhamento...")
    df_detalhes = consolidar_arquivos(
        pasta=pasta_raw, 
        padrao_nome="RESTRICAO_COFF_EOLICA_DETAIL_20*.csv", 
        colunas_de_data=datas_usinas # Supondo que a coluna de data tem o mesmo nome
    )
    
    # Exibindo o resultado final da consolidação
    logging.info(f"✅ Total de linhas em Usinas: {len(df_usinas)}")
    logging.info(f"✅ Total de linhas em Detalhamento: {len(df_detalhes)}")
    
    # Mostrando os tipos de dados para provar que o 'parsing de datas' funcionou
    logging.info("\nTipos de dados da tabela Usinas:\n" + str(df_usinas.dtypes.head()))
    
    logging.info("--- Consolidação Finalizada ---")
    
    # Retornamos os DataFrames para podermos usá-los no próximo passo
    return df_usinas, df_detalhes

if __name__ == "__main__":
    executar_transformacao()