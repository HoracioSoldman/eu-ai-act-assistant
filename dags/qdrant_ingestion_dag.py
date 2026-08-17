from datetime import datetime
from airflow.decorators import dag, task
import os
import io

@dag(
    dag_id="ingest_terms_to_qdrant_db",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["qdrant", "vector-db", "ingestion", "sentence-transformers", "eu-ai-act"],
)
def qdrant_ingestion():

    @task
    def embed_and_upsert():
        from sentence_transformers import SentenceTransformer
        from qdrant_client import QdrantClient
        from qdrant_client.models import PointStruct, VectorParams, Distance
        import pandas as pd 
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import uuid


        # Connect to Qdrant service via Docker internal network
        client = QdrantClient(url="http://qdrant:6333")
        
        collection_name = "eu_ai_act_definitions"

        s3_hook = S3Hook()
        
        bucket_name = "compliance-docs"
        folder_prefix = "silver/act_doc/"

        s3_client = s3_hook.get_conn()
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        
        if "Contents" not in response:
            raise ValueError(f"No files found in bucket '{bucket_name}' under prefix '{folder_prefix}'")
        
        # Filter out folder placeholders (keys ending with '/') and find the latest by LastModified
        files = [obj for obj in response["Contents"] if not obj["Key"].endswith("/")]
        
        if not files:
            raise ValueError(f"No valid files found in folder '{folder_prefix}'")
            
        latest_file = max(files, key=lambda x: x["LastModified"])
        latest_key = latest_file["Key"]
        
        print(f"Latest file identified: {latest_key} (Modified at: {latest_file['LastModified']})")

        file_obj = s3_hook.get_key(latest_key, bucket_name)
        file_content = file_obj.get()["Body"].read()

        # processing with Pandas
        df = pd.read_csv(io.BytesIO(file_content))
        print(f"Successfully loaded {len(df)} rows from {latest_key}.")
        
        terms = []
        metadata_list = []

        for index, row in df.iterrows():
            content = row['text']
            if pd.notna(content):
                full_content = f"{row['structure_path']} {content}"
                terms.append(full_content)
                metadata = {
                    "term": full_content,
                    "id": row['id'],
                    "defined_terms": row['defined_terms'],
                    "source": row['structure_path'],
                    "chunk_type": row['chunk_type'],
                    "citation_label": row['citation_label'],
                    "ingested_at": datetime.now().isoformat()
                }

                metadata_list.append(metadata)

        print("Loading SentenceTransformer model...")
        # downloads the open-source model weights (approx 120MB)
        model = SentenceTransformer("all-MiniLM-L6-v2")
        print("Encoding text into vectors...")
        # encode returns a numpy array, which we convert to standard Python lists
        embeddings = model.encode(terms).tolist()
        
        # ensure the collection exists with the correct vector dimensions (384 for this model)
        if not client.collection_exists(collection_name):
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=len(embeddings[0]), distance=Distance.COSINE),
            )

        points = []
        # zip the metadata items with their generated vectors
        for item, vector in zip(metadata_list, embeddings):    
            points.append(
                PointStruct(
                    id=item['id'],
                    vector=vector,
                    payload=item 
                )
            )

        print(f"Upserting {len(points)} items with metadata...")
        client.upsert(
            collection_name=collection_name,
            points=points,
        )
        print("Successfully ingested documents with full metadata!")

    embed_and_upsert()

dag_instance = qdrant_ingestion()