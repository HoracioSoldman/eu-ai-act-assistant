import os
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="rag_evaluation",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rag", "evaluation", "qdrant", "postgres"],
)
def rag_retrieval_evaluation():

    @task()
    def load_evaluation_dataset() -> list[dict]:
        """Loads the original dataset for EU AI Act compliance testing."""
        return [
            {
                "query": "What is the definition of a provider under the EU AI Act?",
                "expected_id": "art-49-par-2",
            },
            {
                "query": "What specific compliance obligations apply to high-risk AI systems?",
                "expected_id": "art-40-par-1",
            },
            {
                "query": "Why does this document matter for an AI system consumer?",
                "expected_id": "art-11-par-2",
            },
        ]

    @task()
    def evaluate_qdrant_retrieval(eval_dataset: list[dict]) -> dict:
        """
        Executes semantic search against Qdrant and calculates Hit Rate and MRR.
        """
        from qdrant_client import QdrantClient
        from sentence_transformers import SentenceTransformer
        
        # Connect to the local Docker Qdrant server
        collection_name = "eu_ai_act_definitions"

        client = QdrantClient(url="http://qdrant:6333")
        
        # CPU-only execution to minimize container footprint
        model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")

        k = 5 
        hits = 0
        reciprocal_rank_sum = 0.0
        total_queries = len(eval_dataset)

        for item in eval_dataset:
            query = item["query"]
            expected_id = item["expected_id"]

            query_vector = model.encode(query).tolist()

            # Query Qdrant via the query_points API
            search_results = client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=k
            ).points

            found = False
            rank = 0
            for idx, hit in enumerate(search_results):
                payload = hit.payload or {}
                term_id = payload.get("term_id", "")
                
                if expected_id in term_id or expected_id in str(payload.get("source", "")):
                    found = True
                    rank = idx + 1
                    break

            if found:
                hits += 1
                reciprocal_rank_sum += 1.0 / rank

        return {
            "total_queries": total_queries,
            "hit_rate_at_k": hits / total_queries if total_queries > 0 else 0.0,
            "mrr": reciprocal_rank_sum / total_queries if total_queries > 0 else 0.0,
            "top_k": k,
        }

    @task()
    def log_metrics_to_postgres(metrics: dict):
        """Saves the calculated evaluation metrics into the PostgreSQL feedback_db."""
        import psycopg2
        
        pg_host = 'postgres'
        pg_db = "feedback_db"
        pg_user = "airflow"
        pg_password = "airflow" # default password for my testing purpose only

        try:
            conn = psycopg2.connect(
                host=pg_host,
                database=pg_db,
                user=pg_user,
                password=pg_password
            )
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO retrieval_evaluation (total_queries, hit_rate, mrr, top_k)
                VALUES (%s, %s, %s, %s);
            ''', (metrics["total_queries"], metrics["hit_rate_at_k"], metrics["mrr"], metrics["top_k"]))
            
            conn.commit()
            logging.info(f"Successfully logged metrics to PostgreSQL: {metrics}")
            
        except Exception as e:
            logging.error(f"Failed to log metrics to database: {e}")
            raise
        finally:
            if 'conn' in locals() and conn is not None:
                cursor.close()
                conn.close()

    # Functional invocation builds the pipeline dependencies seamlessly
    dataset = load_evaluation_dataset()
    eval_metrics = evaluate_qdrant_retrieval(dataset)
    log_metrics_to_postgres(eval_metrics)

dag = rag_retrieval_evaluation()