# ==============================================================================
# NOME DO SCRIPT: extract.py 
# AUTOR:          Lívia Belo
# DATA DA VERSÃO: Maio de 2026
# VERSÃO:         1.0.0
# DESCRIPTION:    Pipeline de Ingestão e Extração (E) automatizado para coleta 
#                 de dados brutos de restrição eólica do open data do ONS (S3). 
#                 Possui geração dinâmica de safras retroativas, resiliência a 
#                 falhas de rede via retry (backoff exponencial) e política de 
#                 fallback de formatos (.parquet -> .csv -> .xlsx) para a camada Raw.
# ==============================================================================

import os
import requests
import logging 
import pandas as pd
from pathlib import Path 
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

#Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RAW_USINAS_DIR = Path("data/raw/raw_usinas")
RAW_USINAS_DIR.mkdir(parents=True, exist_ok=True)

RAW_USINAS_DETAIL_DIR = Path("data/raw/raw_usinas_detail")
RAW_USINAS_DETAIL_DIR.mkdir(parents=True, exist_ok=True)

#====================================================================
# Geração dos parâmetros
#====================================================================
def generate_dynamic_month_list(qtd_meses: int = 6, defasagem_meses: int = 1) -> list:
    """
    Gera uma lista com os últimos X meses no formato YYYY_MM.
    
    Args:
        qtd_meses: Quantidade de meses para buscar para trás.
        defasagem_meses: Quantos meses ignorar a partir de hoje (1 = ignora o mês atual incompleto).
    """
    # 1. Descobre a data de hoje e subtrai a defasagem (ex: se hoje é Maio, ele volta para Abril)
    data_referencia = pd.Timestamp.today() - pd.DateOffset(months=defasagem_meses)
    
    # 2. Gera a sequência de datas. 
    # O uso do parâmetro 'periods' em vez de 'start' diz ao Pandas: 
    # "A partir da data de referência, volte X períodos exatos para trás".
    intervalo_meses = pd.date_range(end=data_referencia, periods=qtd_meses, freq='MS')
    
    # 3. Converte para o formato de string exigido pela URL do ONS
    return intervalo_meses.strftime("%Y_%m").tolist()

# Agora a sua variável se adapta sozinha ao tempo!
COMPET = generate_dynamic_month_list()

print("Meses que serão baixados:", COMPET)

class S3DownloadError(Exception):
    pass
@retry(
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(requests.exceptions.ConnectionError)
)

def fetch_ons_data(base_url: str, filename_base: str, raw_data_path: str):
    #Baixar o arquivo Parquet. Fallback sequencial para CSV e XLSV caso receba HTTP 404.
    
    formatos = [".parquet",".csv",".xlsx"]

    for fmt in formatos:
        url = f"{base_url}/{filename_base}{fmt}"
        logging.info(f"Tentando extrair: {url}")
        
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                file_path = raw_data_path / f"{filename_base}{fmt}"
                with open(file_path, "wb") as f:
                    f.write(response.content)
                logging.info(f"Sucesso! Arquivo salvo em: {file_path}")
                return True
            elif response.status_code == 404:
                logging.warning(f"Formato {fmt} não encontrado (HTTP 404). Tentando fallback...")
                continue
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Falha de conexão ao acessar {url}: {e}")
            raise S3DownloadError(f"Erro na requisição: {e}")
            
    logging.error(f"Nenhum formato disponível para {filename_base}.")
    return False

def executar_extracao():
    url_complexos = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_eolica_tm"
    url_spes = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_eolica_detail_tm"
    
    # Agora o laço itera sobre a lista gerada dinamicamente pela sua função
    for comp in COMPET:
        # Extração Conjuntos/Complexos
        fetch_ons_data(url_complexos, f"RESTRICAO_COFF_EOLICA_{comp}", RAW_USINAS_DIR)
        # Extração SPEs/Detalhamento
        fetch_ons_data(url_spes, f"RESTRICAO_COFF_EOLICA_DETAIL_{comp}", RAW_USINAS_DETAIL_DIR)

if __name__ == "__main__":
    executar_extracao()