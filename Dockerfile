FROM apache/airflow:3.3.1

# Switch to root temporarily if you need system-level dependencies (like gcc)
# USER root
# RUN apt-get update && apt-get install -y my-system-package

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt