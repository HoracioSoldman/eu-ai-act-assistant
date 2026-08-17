FROM apache/airflow:3.3.1

USER airflow

# CPU-only PyTorch first
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt