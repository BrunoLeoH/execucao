from __future__ import annotations
import pendulum
import logging
from scipy import spatial
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

# Configuração
ELASTIC_CONN_ID = "elasticsearch_default"
INDEX_VETORES = "vectors_index"
INDEX_SCORES = "coherence_scores_index"

log = logging.getLogger(__name__)

def calculate_cosine_similarity(vec1, vec2):
    """
    Calcula a similaridade de cosseno entre dois vetores.
    Retorna um valor entre 0 e 1.
    """
    # 1 - distância de cosseno = similaridade
    return 1 - spatial.distance.cosine(vec1, vec2)

def fetch_uncompared_vectors(ti):
    """
    Busca vetores que estão marcados como 'similarity_status': 'PENDING'.
    """
    es = ElasticsearchHook(elasticsearch_conn_id=ELASTIC_CONN_ID)
    
    # Query simulada
    query = {
        "query": {
            "match": {"similarity_status": "PENDING"}
        }
    }
    
    # Simulação de dados recuperados (com vetor transcrito e vetor digitado)
    # Num cenário real, você teria que fazer um 'join' ou ter ambos no mesmo doc.
    # Aqui assumimos que o documento no INDEX_VETORES já tem os dois vetores para comparação
    # (ex: vetor da resposta transcrita vs vetor da resposta padrão/esperada).
    
    results = [
        {
            "_id": "call_123_0",
            "_source": {
                "origin_call_id": "call_123",
                "pergunta_txt": "Tem diabetes?",
                # Simulando vetores aleatórios para o exemplo
                "vector_resposta_transcrita": [0.1, 0.2, 0.3], 
                "vector_resposta_digitada": [0.1, 0.25, 0.28] # O que o entrevistador digitou
            }
        }
    ]
    
    ti.xcom_push(key='vectors_to_compare', value=results)

def calculate_and_store_scores(ti):
    """
    Calcula a similaridade e armazena o Score de Coerência final.
    """
    docs = ti.xcom_pull(key='vectors_to_compare', task_ids='fetch_uncompared_vectors')
    
    if not docs:
        log.info("Nada para comparar.")
        return
        
    es = ElasticsearchHook(elasticsearch_conn_id=ELASTIC_CONN_ID)
    es_client = es.get_conn()
    
    for doc in docs:
        doc_id = doc["_id"]
        source = doc["_source"]
        
        vec_a = source["vector_resposta_transcrita"]
        vec_b = source["vector_resposta_digitada"]
        
        # 1. Cálculo da Similaridade (O "Coração" da Entrega 3)
        score = calculate_cosine_similarity(vec_a, vec_b)
        
        log.info(f"Score calculado para {doc_id}: {score}")
        
        # 2. Monta o documento final de Score
        score_doc = {
            "call_id": source["origin_call_id"],
            "fragment_id": doc_id,
            "pergunta": source["pergunta_txt"],
            "coherence_score": score,
            "calculation_date": pendulum.now().isoformat(),
            "status": "PROCESSED"
        }
        
        # 3. Salva na Busca Inteligente (Índice de Scores)
        es_client.index(index=INDEX_SCORES, body=score_doc)
        
        # 4. Atualiza o status do vetor original para 'COMPARED'
        es_client.update(
            index=INDEX_VETORES,
            id=doc_id,
            body={"doc": {"similarity_status": "COMPARED"}}
        )

with DAG(
    dag_id="dag_similarity",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="0 6 * * *", # Roda após a vetorização
    catchup=False,
    tags=["entrega3", "similarity", "analysis"],
) as dag:
    
    t1 = PythonOperator(
        task_id="fetch_uncompared_vectors",
        python_callable=fetch_uncompared_vectors
    )
    
    t2 = PythonOperator(
        task_id="calculate_and_store_scores",
        python_callable=calculate_and_store_scores
    )
    
    t1 >> t2