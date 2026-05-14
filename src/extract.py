import asyncio
import httpx   #
import logging #permite uma precisao maior na mostragem de erros (possui níveis de severidade, carimbos de tempo e salva o histórico)
import pandas as pd
from pathlib import Path 

#Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


async def download_file(client: httpx.AsyncClient, url: str, caminho_destino: Path):
    if caminho_destino.exists():
        logging.info(f"O arquivo {caminho_destino.name} já existe.") #se o arquivo já foi baixado, ele não vai ser baixado novamente (idempotência)
        return

    try:
        logging.info(f"Baixando {url}") #se houver um erro, o python interrompe a tarefa atual e pula pro except)

        answer = await client.get(url)               # answer aqui ele pega o arquivo no url Usamos o 'await' (aguarde). Ele diz: "Pode ir baixar, Python. Enquanto você espera 
                                                         # o servidor responder, vá fazer outras coisas e depois volte aqui".
        answer.raise_for_status()              # se houver erro de 404 ou 500, a execução vai para os excepts

        with open(caminho_destino, 'wb') as arquivo:
            arquivo.write(answer.content)

        logging.info(f"Feito! Arquivo salvo em: {caminho_destino}")

    except httpx.HTTPStatusError as erro_http: #erro devido a falta do arquivo, link errado
        logging.error(f"Erro no link {url}. Status: {erro_http.response.status_code}")
    except httpx.RequestError as erro_req:     #erro devido a falta de internet, conexão falhou
        logging.error(f"Erro de conexão ao tentar baixar {url}: {erro_req}")

def generate_month_list(inicio: str, fim: str) -> list:
    intervalo_meses = pd.date_range(start=f"{inicio}-01", end=f"{fim}-01", freq='MS') #pd.date_range gera uma sequência contínua de datas, freq MS = Month start
    return intervalo_meses.strftime("%Y_%m").tolist()                                 #strftime = string format time ; .tolist() tira do formato pandas  e transforma em lista simples


async def execute_extraction(data_inicio: str = "2025-10", data_fim: str = "2026-03"):
    # 1. Geração inteligente da lista de meses ['2025-10', '2025-11', ...]
    meses_alvo = generate_month_list(data_inicio, data_fim)
    
    # 2. As URLs base com a marcação {mes} esperando para ser preenchida
    url_base_usinas = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_eolica_tm/RESTRICAO_COFF_EOLICA_{mes}.csv"
    url_base_detalhes = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_eolica_detail_tm/RESTRICAO_COFF_EOLICA_DETAIL_{mes}.csv"
    
    # 3. Preparando o ambiente (criando a pasta se não existir)
    pasta_raw = Path("data/raw")
    pasta_raw.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"--- Iniciando Extração ONS ({data_inicio} até {data_fim}) ---")
    
    # 4. O Loop de Extração
    async with httpx.AsyncClient(timeout=60.0) as client:
        tarefas = [] # Lista para guardar as nossas 12 missões

        for mes in meses_alvo:
            # --- Para o arquivo de Usinas ---
            # A mágica acontece aqui: o '.format(mes=mes)' injeta '2025-10' na URL e no nome do arquivo
            nome_usi = f"RESTRICAO_COFF_EOLICA_{mes}.csv"
            url_usi = url_base_usinas.format(mes=mes)
            caminho_usi = pasta_raw / nome_usi
            
            tarefas.append(download_file(client, url_usi, caminho_usi))
            
            # --- Para o arquivo de Detalhamento ---
            nome_det = f"RESTRICAO_COFF_EOLICA_DETAIL_{mes}.csv"
            url_det = url_base_detalhes.format(mes=mes)
            caminho_det = pasta_raw / nome_det
            
            tarefas.append(download_file(client, url_det, caminho_det))

        await asyncio.gather(*tarefas)

    logging.info("--- Extração Finalizada ---")

# Ponto de entrada do script
if __name__ == "__main__":
    #execute_extraction()
    asyncio.run(execute_extraction()) # Como a função principal agora é async, usamos o asyncio.run para dar a ignição inicial