from __future__ import annotations

import pendulum
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.speech_to_text import CloudSpeechToTextHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

# --- Configurações do Projeto ---
# IMPORTANTE: "google_cloud_default" é o nome padrão da conexão no Airflow.
# Não coloque o ID numérico do projeto aqui.
GCP_CONN_ID = "google_cloud_default" 
ELASTIC_CONN_ID = "elasticsearch_default"

GCS_BUCKET_NAME = "audios_teste" 
ELASTIC_INDEX_NAME = "transcriptions_index"

SPEECH_TO_TEXT_CONFIG = {
    "encoding": "LINEAR16", 
    "sample_rate_hertz": 16000, 
    "language_code": "pt-BR",
    "enable_automatic_punctuation": True,
}

log = logging.getLogger(__name__)

# --- Funções Python (Tasks) ---

def identifica_audios(ti):
    """
    Lista arquivos de áudio no Bucket do GCS.
    """
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    
    # Lista arquivos com prefixo 'audio/'
    files = gcs_hook.list(GCS_BUCKET_NAME, prefix="audio/")
    
    # Filtra extensões de áudio
    audio_files = [f for f in files if f.endswith(('.wav', '.mp3', '.flac'))]
    
    if not audio_files:
        log.info("Nenhum áudio encontrado.")
        return []

    # Monta a URI completa (gs://bucket/arquivo)
    gcs_uris = [f"gs://{GCS_BUCKET_NAME}/{f}" for f in audio_files]
    
    # Envia para o XCom para ser usado na próxima tarefa
    ti.xcom_push(key='gcs_audio_uris', value=gcs_uris)
    log.info(f"Identificados {len(gcs_uris)} áudios: {gcs_uris}")
    return gcs_uris

def transcrever_lista_de_audios(ti):
    """
    Esta função permitir o loop sobre múltiplos arquivos e formatar a saída corretamente.
    """
    # 1. Recupera a lista de URIs da tarefa anterior
    uris = ti.xcom_pull(task_ids='identify_pending_audios', key='gcs_audio_uris')
    
    if not uris:
        log.info("Lista de áudios vazia. Nada a transcrever.")
        return []

    # Inicializa o Hook (ferramenta que conversa com a API do Google)
    stt_hook = CloudSpeechToTextHook(gcp_conn_id=GCP_CONN_ID)
    
    resultados_formatados = []

    for uri in uris:
        log.info(f"Transcrevendo: {uri}")
        
        # Configura o objeto de áudio para a API
        audio_payload = {"uri": uri}
        
        try:
            # Chama a API do Google Speech-to-Text
            response = stt_hook.recognize(
                config=SPEECH_TO_TEXT_CONFIG, 
                audio=audio_payload
            )
            
            # Processa a resposta (JSON complexo) para extrair apenas o texto
            texto_completo = ""
            if response.results:
                for result in response.results:
                    # Pega a melhor alternativa de transcrição
                    texto_completo += result.alternatives[0].transcript + " "
            
            # Salva no formato que a tarefa de Elastic espera
            resultados_formatados.append({
                "gcs_uri": uri,
                "transcribed_text": texto_completo.strip()
            })
            
        except Exception as e:
            log.error(f"Erro ao transcrever {uri}: {e}")
            # Continua para o próximo arquivo mesmo com erro

    return resultados_formatados

def store_transcription_to_elasticsearch(ti):
    """
    Recebe os resultados formatados e insere no Elasticsearch.
    """
    elastic_hook = ElasticsearchHook(elasticsearch_conn_id=ELASTIC_CONN_ID)
    
    # Pega o retorno da função 'transcrever_lista_de_audios'
    transcription_results = ti.xcom_pull(task_ids='run_speech_to_text')

    if not transcription_results:
        log.warning("Nenhum resultado de transcrição para processar.")
        return

    es_client = elastic_hook.get_conn()

    for item in transcription_results:
        gcs_uri = item.get('gcs_uri')
        transcribed_text = item.get('transcribed_text', 'N/A')
        
        # Extrai ID (ex: gs://bucket/audio/CALL-123.wav -> CALL-123)
        call_id = gcs_uri.split('/')[-1].split('.')[0]
        
        document = {
            "call_id": call_id,
            "timestamp_processamento": pendulum.now().isoformat(),
            "status": "TRANSCRIBED",
            "transcription_text": transcribed_text,
        }
        
        try:
            # Indexa o documento
            es_client.index(
                index=ELASTIC_INDEX_NAME, 
                id=call_id,
                body=document
            )
            log.info(f"Documento {call_id} indexado no Elasticsearch.")
        except Exception as e:
            log.error(f"Erro ao indexar {call_id}: {e}")

# --- Definição da DAG ---

with DAG(
    dag_id="dag_transcript",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="0 4 * * *",
    catchup=False,
    tags=["gcp", "speech-to-text", "elasticsearch"],
) as dag:
    
    # 1. Identificar arquivos
    identify_task = PythonOperator(
        task_id="identify_pending_audios",
        python_callable=identifica_audios,
    )
    
    # 2. Transcrever (Usando PythonOperator para controlar o loop)
    transcribe_task = PythonOperator(
        task_id="run_speech_to_text",
        python_callable=transcrever_lista_de_audios,
    )

    # 3. Salvar no Elastic
    store_to_elastic_task = PythonOperator(
        task_id="store_to_elasticsearch",
        python_callable=store_transcription_to_elasticsearch,
    )
    
    # 4. Finalização
    finalize_metadata_task = PythonOperator(
        task_id="update_metadata_status",
        python_callable=lambda: log.info("Processo finalizado."),
    )

    # Fluxo
    identify_task >> transcribe_task >> store_to_elastic_task >> finalize_metadata_task