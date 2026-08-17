
from datetime import datetime
from datasets import load_dataset
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import pandas as pd

@dag(
    dag_id="file_download_and_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "bronze", "eu-ai-act"]
)
def ingest_to_minio_bronze():

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


    file_path = download_external_file()
    upload_to_bronze(file_path)

# Instantiate the DAG
ingest_to_minio_bronze()