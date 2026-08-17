from datetime import datetime
import io
import os
import re
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    dag_id="data_processor",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["preprocess", "silver", "eu-ai-act"],
)
def preprocess_data():

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

    local_filepath = filtering()
    preprocessing(local_filepath)

preprocess_data()