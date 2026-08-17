
from datetime import datetime
from datasets import load_dataset
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import re
import os
import uuid
import pandas as pd



@dag(
    dag_id="ingest_terms_to_qdrant",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "minio", "eu-ai-act", "qdrant", "vector-db", "sentence-transformers"]
)
def ingest_to_qdrant():

    @task
    def download_external_file() -> str:
        """
        Downloads an external file and saves it temporarily on the Airflow worker.
        Returns the local file path to pass to the next task.
        """
        dataset = load_dataset("jeroenherczeg/eu-ai-act")

        # Convert the 'train' split to a pandas DataFrame
        df = dataset["train"].to_pandas()

        local_path = "/tmp/eu_ai_act_2024_1744.csv"

        df.to_csv(local_path, index=False, encoding="utf-8")
        
        return local_path

    @task
    def upload_to_bronze(local_path: str):
        """
        Uploads the local file to the MinIO Bronze layer using S3Hook.
        """
        # S3Hook automatically uses the AIRFLOW_CONN_AWS_DEFAULT environment variable
        hook = S3Hook()
        
        bucket_name = "compliance-docs"
        
        s3_key = f"bronze/act_doc/raw_data_{datetime.now().strftime('%Y%m%d')}.csv"
        
        # Load the file into MinIO
        hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        # Clean up the local file from the worker
        if os.path.exists(local_path):
            os.remove(local_path)


    @task
    def filtering():
        # S3Hook automatically reads the AIRFLOW_CONN_AWS_DEFAULT variable 
        # and points to your local MinIO container (http://minio:9000)
        s3_hook = S3Hook()
        
        bucket_name = "compliance-docs"
        folder_prefix = "bronze/act_doc/"

        client = s3_hook.get_conn()
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        
        if "Contents" not in response:
            raise ValueError(f"No files found in bucket '{bucket_name}' under prefix '{folder_prefix}'")
        
        # Filter out folder placeholders (keys ending with '/') and find the latest by LastModified
        files = [obj for obj in response["Contents"] if not obj["Key"].endswith("/")]
        
        if not files:
            raise ValueError(f"No valid files found in folder '{folder_prefix}'")
            
        latest_file = max(files, key=lambda x: x["LastModified"])
        latest_key = latest_file["Key"]
        filename = os.path.basename(latest_key)
        
        print(f"Latest file identified: {latest_key} (Modified at: {latest_file['LastModified']})")

        file_obj = s3_hook.get_key(latest_key, bucket_name)
        file_content = file_obj.get()["Body"].read()

        # processing with Pandas
        df = pd.read_csv(io.BytesIO(file_content))
        print(f"Successfully loaded {len(df)} rows from {latest_key}.")


        # only keep the english language rows
        df = df[df['language'] == 'en']
        print(f"Filtered to {len(df)} rows with English language.")

        df = df[['id', 'text', 'parent_structure_path', 'structure_path', 'chunk_type', 'citation_label', 'defined_terms']]

        local_path = f"/tmp/{filename}"

        df.to_csv(local_path, index=False, encoding="utf-8")
        
        return local_path
        
    @task
    def preprocessing(local_path: str):

        df = pd.read_csv(local_path)
        # fix the defined_terms column to be a list of strings instead of a string representation of a list
        df['defined_terms'] = df['defined_terms'].apply(lambda x: re.findall(r"'([^']+)'", x))

        # fix the structure_path column to be a list of strings instead of a string representation of a list
        df['structure_path'] = df['structure_path'].apply(lambda x: x.replace("anx", "annex").replace("rec", "recital").replace("sec", "section").replace("art", "article").replace("par", "paragraph").replace("/", " > ") if pd.notnull(x) else x)

        df.to_csv(local_path, index=False, encoding="utf-8")

        s3_hook = S3Hook()
        bucket_name = "compliance-docs"
        s3_key = f"silver/act_doc/processed_{datetime.now().strftime('%Y%m%d')}.csv"
        
        s3_hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        if os.path.exists(local_path):
            os.remove(local_path)
        
        print(f"Uploaded processed file to {s3_key} in bucket {bucket_name}.")

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
                    "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, full_content)),
                    "term": full_content,
                    "term_id": row['id'],
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

    file_path = download_external_file()
    t_upload = upload_to_bronze(file_path)

    local_filepath = filtering()
    t_preproc = preprocessing(local_filepath)

    t_load = embed_and_upsert()

    t_upload >> local_filepath >> t_preproc >> t_load

# Instantiate the DAG
dag_instance = ingest_to_qdrant()


