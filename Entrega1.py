import os
import requests
import pendulum
from datetime import datetime, timedelta

from twilio.rest import Client
from airflow import DAG
from airflow.operators import PythonOperator

PASTA_DESTINO = "/tmp/gravacoes_twilio"

def baixar_gravacoes(**kwargs):
    """
    Baixa gravações da Twilio baseadas na data de execução da DAG.
    
    kwargs: Dicionário de contexto do Airflow injetado automaticamente.
    """
    
    # 1. Recuperar Data de Execução do Airflow
    # 'ds' retorna a data de execução no formato YYYY-MM-DD
    data_execucao_str = kwargs.get('ds') 
    
    # Converter para objeto datetime para manipulação
    data_referencia = datetime.strptime(data_execucao_str, '%Y-%m-%d')
    
    # Definindo o intervalo
    # Aqui estamos pegando o dia exato da execução da DAG.
    data_inicio = data_referencia
    data_fim = data_referencia + timedelta(days=1)

    print(f"Iniciando processo para data: {data_execucao_str}")
    
    # 2. Segurança: Carregar credenciais APENAS dentro da função
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    
    if not account_sid or not auth_token:
        raise ValueError("ERRO: Variáveis de ambiente TWILIO não configuradas.")

    # Criar pasta se não existir
    os.makedirs(PASTA_DESTINO, exist_ok=True)

    # Inicializar cliente
    client = Client(account_sid, auth_token)
    
    # 3. Iterar sobre subcontas ou apenas a conta principal
    # Nota: client.api.accounts.list() pode ser lento se houver muitas subcontas.
    try:
        subcontas = client.api.accounts.list()
    except Exception as e:
        print(f"Erro ao listar contas: {e}")
        return

    total_baixados = 0

    for subconta in subcontas:
        # Inicializa cliente para a subconta específica
        sub_client = Client(subconta.sid, auth_token)
        
        print(f"Verificando conta: {subconta.sid}...")

        # Buscar gravações filtradas pela data do Airflow
        gravacoes = sub_client.recordings.list(
            date_created_after=data_inicio,
            date_created_before=data_fim
        )

        for gravacao in gravacoes:
            try:
                # Usamos os dados que já vêm no objeto 'gravacao'
                call_sid = gravacao.call_sid
                # data_criacao vem como datetime, formatamos para string segura p/ arquivo
                data_arquivo = gravacao.date_created.strftime("%Y-%m-%d_%H-%M")
                
                nome_arquivo = f"{subconta.sid}_{data_arquivo}_{call_sid}.wav"
                caminho_completo = os.path.join(PASTA_DESTINO, nome_arquivo)
                
                # Modificar a URL para baixar em .wav (Twilio padrão é json ou mp3)
                # Remove a extensão .json da URI se existir e força .wav
                uri_base = gravacao.uri.replace(".json", "")
                #url_download = f"https://api.twilio.com{uri_base}.wav"
                url_download = uri_base

                print(f"Baixando: {nome_arquivo}...")
                
                response = requests.get(url_download, auth=(account_sid, auth_token))
                
                if response.status_code == 200:
                    with open(caminho_completo, "wb") as f:
                        f.write(response.content)
                    print(f"Sucesso: {caminho_completo}")
                    total_baixados += 1
                else:
                    print(f"Falha {response.status_code} ao baixar {gravacao.sid}")
                    
            except Exception as e_loop:
                print(f"Erro ao processar gravação {gravacao.sid}: {e_loop}")

    print(f"Processo finalizado. Total de arquivos baixados: {total_baixados}")


# --- Definição da DAG ---

with DAG(
    dag_id="dag_storage_call_records",
    # Define o início (pode ser data passada para testar backfill)
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False, # Se True, roda os dias passados desde start_date até hoje
    tags=["twilio", "ingestao"],
) as dag:
    
    tarefa_baixar_audios = PythonOperator(
        task_id="baixar_audios_twilio",
        python_callable=baixar_gravacoes,
        provide_context=True, # Importante para passar **kwargs e datas
    )